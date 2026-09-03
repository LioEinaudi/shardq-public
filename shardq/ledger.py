from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .spec import slug


FAILURE_REASONS = {
    "preempted",
    "lease_expired",
    "launcher_terminal",
    "backend_error",
    "validation_error",
    "session_error",
}
PREEMPTION_REASONS = {"preempted", "lease_expired", "launcher_terminal"}
PARTITION_STATUSES = {"cutting", "cut", "merged", "partial"}


class ShardState(str, Enum):
    PENDING = "PENDING"
    PENDING_BACKOFF = "PENDING_BACKOFF"
    LEASED = "LEASED"
    DONE = "DONE"
    QUARANTINED = "QUARANTINED"
    SUPERSEDED = "SUPERSEDED"


@dataclass
class Claim:
    job_id: str
    partition: str
    shard_id: str
    start: int
    count: int
    attempt_id: str
    shard_dir: Path
    attempt_dir: Path
    session_key: str
    est_seconds: float


@dataclass
class ShardView:
    shard_id: str
    partition: str
    start: int
    count: int
    state: ShardState
    errors: int
    preemptions: int
    retry_after_unix: float
    depth: int
    lease_owner: Optional[dict]
    heartbeat_unix: Optional[float]
    complete_attempts: List[str]


@dataclass
class PartitionView:
    name: str
    size: int
    next_cut: int
    unit_seconds_est: float
    status: str
    shards: List[ShardView]
    done: int
    leased: int
    pending: int
    backoff: int
    quarantined: int
    superseded: int
    done_units: int
    quarantined_units: int


@dataclass
class JobView:
    job_id: str
    spec: dict
    pool: str
    column: str
    stopped: bool
    partitions: Dict[str, PartitionView]


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    temporary.write_text(content, encoding="utf-8")
    os.replace(str(temporary), str(path))


def atomic_write_json(path: Path, payload: Any) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _attempt_failures(shard_dir: Path) -> List[dict]:
    rows = []
    for path in sorted((shard_dir / "attempts").glob("*/failed.json")):
        payload = read_json(path, {})
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def shard_state(shard_dir: Path, now: float) -> ShardState:
    if (shard_dir / "superseded.json").is_file():
        return ShardState.SUPERSEDED
    if any((shard_dir / "attempts").glob("*/complete.json")):
        return ShardState.DONE
    if (shard_dir / "quarantine.json").is_file():
        return ShardState.QUARANTINED
    if (shard_dir / "lease").is_dir():
        return ShardState.LEASED
    failures = _attempt_failures(shard_dir)
    if failures:
        latest = max(failures, key=lambda row: float(row.get("failed_unix", 0)))
        if float(latest.get("retry_after_unix", 0)) > now:
            return ShardState.PENDING_BACKOFF
    return ShardState.PENDING


def _read_heartbeat(path: Path) -> Optional[float]:
    try:
        return float(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, TypeError, ValueError):
        return None


class Ledger:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def job_dir(self, job_id: str) -> Path:
        return self.root / "jobs" / job_id

    def list_jobs(self) -> List[str]:
        jobs = self.root / "jobs"
        if not jobs.is_dir():
            return []
        return sorted(path.name for path in jobs.iterdir() if path.is_dir() and (path / "job.json").is_file())

    def _scan_shard(self, shard_dir: Path, now: float) -> ShardView:
        metadata = read_json(shard_dir / "shard.json", {})
        failures = _attempt_failures(shard_dir)
        errors = sum(str(row.get("reason")) not in PREEMPTION_REASONS for row in failures)
        preemptions = sum(str(row.get("reason")) in PREEMPTION_REASONS for row in failures)
        latest_failure = max(failures, key=lambda row: float(row.get("failed_unix", 0))) if failures else {}
        retry_after = float(latest_failure.get("retry_after_unix", 0))
        owner = read_json(shard_dir / "lease" / "owner.json")
        completed = sorted(path.parent.name for path in (shard_dir / "attempts").glob("*/complete.json"))
        return ShardView(
            shard_id=str(metadata.get("shard_id", shard_dir.name)),
            partition=str(metadata.get("partition", "")),
            start=int(metadata.get("start", 0)),
            count=int(metadata.get("count", 0)),
            state=shard_state(shard_dir, now),
            errors=errors,
            preemptions=preemptions,
            retry_after_unix=retry_after,
            depth=int(metadata.get("depth", 0)),
            lease_owner=owner if isinstance(owner, dict) else None,
            heartbeat_unix=_read_heartbeat(shard_dir / "lease" / "heartbeat"),
            complete_attempts=completed,
        )

    def scan_job(self, job_id: str, now: float) -> JobView:
        directory = self.job_dir(job_id)
        spec = read_json(directory / "job.json")
        if not isinstance(spec, dict):
            raise ValueError(f"missing or invalid job.json for {job_id}")
        partitions: Dict[str, PartitionView] = {}
        base = directory / "partitions"
        for partition_dir in sorted(base.iterdir() if base.is_dir() else []):
            if not partition_dir.is_dir():
                continue
            metadata = read_json(partition_dir / "partition.json")
            if not isinstance(metadata, dict):
                continue
            shards = [
                self._scan_shard(path, now)
                for path in sorted((partition_dir / "shards").iterdir() if (partition_dir / "shards").is_dir() else [])
                if path.is_dir() and (path / "shard.json").is_file()
            ]
            counts = {state: sum(shard.state == state for shard in shards) for state in ShardState}
            name = str(metadata.get("name", partition_dir.name))
            partitions[name] = PartitionView(
                name=name,
                size=int(metadata["size"]),
                next_cut=int(metadata.get("next_cut", 0)),
                unit_seconds_est=float(metadata.get("unit_seconds_est", 0)),
                status=str(metadata.get("status", "cutting")),
                shards=shards,
                done=counts[ShardState.DONE],
                leased=counts[ShardState.LEASED],
                pending=counts[ShardState.PENDING],
                backoff=counts[ShardState.PENDING_BACKOFF],
                quarantined=counts[ShardState.QUARANTINED],
                superseded=counts[ShardState.SUPERSEDED],
                done_units=sum(shard.count for shard in shards if shard.state == ShardState.DONE),
                quarantined_units=sum(shard.count for shard in shards if shard.state == ShardState.QUARANTINED),
            )
        return JobView(
            job_id=job_id,
            spec=spec,
            pool=str(spec.get("pool", "")),
            column=str(spec.get("column", "")),
            stopped=(directory / "STOP").exists(),
            partitions=partitions,
        )

    def claim(
        self,
        pool: str,
        job_ids: List[str],
        worker: dict,
        prefer_partition: Optional[str],
        prefer_session_key: Optional[str],
        now: float,
    ) -> Optional[Claim]:
        candidates = []
        for job_id in job_ids or self.list_jobs():
            job = self.scan_job(job_id, now)
            if job.pool != pool or job.stopped:
                continue
            priority = int(job.spec.get("priority", 0))
            session_key = str(job.spec.get("session_key", ""))
            traits_by_partition = job.spec.get("traits") or {}
            retry = job.spec.get("retry") or {}
            for partition in job.partitions.values():
                max_parallel = int((traits_by_partition.get(partition.name) or {}).get("max_parallel", 2 ** 31 - 1))
                if partition.leased >= max_parallel:
                    continue
                remaining = partition.pending + partition.backoff
                for shard in partition.shards:
                    if shard.state != ShardState.PENDING:
                        continue
                    if (
                        shard.errors >= int(retry.get("max_errors", 3))
                        or shard.preemptions >= int(retry.get("max_preemptions", 20))
                    ):
                        # Leave threshold-exceeded shards pending so the scheduler
                        # has a durable window in which to split or quarantine them.
                        continue
                    candidates.append(
                        (
                            partition.name != prefer_partition,
                            session_key != prefer_session_key,
                            -priority,
                            remaining,
                            not bool(shard.errors or shard.preemptions),
                            shard.start,
                            job,
                            partition,
                            shard,
                            session_key,
                        )
                    )
        candidates.sort(key=lambda row: row[:6])
        for row in candidates:
            job, partition, shard, session_key = row[6], row[7], row[8], row[9]
            shard_dir = self.job_dir(job.job_id) / "partitions" / partition.name / "shards" / shard.shard_id
            lease = shard_dir / "lease"
            try:
                os.mkdir(str(lease))
            except FileExistsError:
                continue
            acquired = float(now)
            attempt_epoch = int(acquired)
            attempt_id = f"{worker['worker_id']}-{attempt_epoch}"
            attempt_dir = shard_dir / "attempts" / attempt_id
            while attempt_dir.exists():
                attempt_epoch += 1
                acquired = float(attempt_epoch)
                attempt_id = f"{worker['worker_id']}-{attempt_epoch}"
                attempt_dir = shard_dir / "attempts" / attempt_id
            attempt_dir.mkdir(parents=True)
            owner = {
                "worker_id": str(worker["worker_id"]),
                "launcher": str(worker.get("launcher", "")),
                "handle": str(worker.get("handle", "")),
                "attempt_id": attempt_id,
                "acquired_unix": acquired,
                "session_key": session_key,
            }
            atomic_write_json(lease / "owner.json", owner)
            atomic_write_json(attempt_dir / "owner.json", owner)
            _atomic_write_text(lease / "heartbeat", f"{now}\n")
            return Claim(
                job_id=job.job_id,
                partition=partition.name,
                shard_id=shard.shard_id,
                start=shard.start,
                count=shard.count,
                attempt_id=attempt_id,
                shard_dir=shard_dir,
                attempt_dir=attempt_dir,
                session_key=session_key,
                est_seconds=float(shard.count) * partition.unit_seconds_est,
            )
        return None

    def heartbeat(self, claim: Claim, now: float) -> None:
        owner = read_json(claim.shard_dir / "lease" / "owner.json", {})
        if owner.get("attempt_id") != claim.attempt_id:
            raise RuntimeError(f"lease for {claim.shard_id} is no longer owned by {claim.attempt_id}")
        _atomic_write_text(claim.shard_dir / "lease" / "heartbeat", f"{now}\n")

    def complete(
        self,
        claim: Claim,
        records_file: Path,
        count: int,
        started_unix: float,
        elapsed_seconds: float,
        backend_extra: dict,
        now: float,
    ) -> None:
        target = claim.attempt_dir / "records.jsonl"
        records_file = Path(records_file)
        if records_file.resolve() != target.resolve():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(records_file), str(target))
        if not target.is_file():
            raise FileNotFoundError(target)
        owner = read_json(claim.attempt_dir / "owner.json", {})
        owner_before = read_json(claim.shard_dir / "lease" / "owner.json", {})
        owned_before = owner_before.get("attempt_id") == claim.attempt_id
        atomic_write_json(
            claim.attempt_dir / "complete.json",
            {
                "attempt_id": claim.attempt_id,
                "worker_id": owner.get("worker_id", claim.attempt_id.rsplit("-", 1)[0]),
                "handle": owner.get("handle", ""),
                "started_unix": started_unix,
                "completed_unix": now,
                "elapsed_seconds": elapsed_seconds,
                "count": count,
                "records_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                "backend_extra": backend_extra,
            },
        )
        current = read_json(claim.shard_dir / "lease" / "owner.json", {})
        if owned_before and current.get("attempt_id") == claim.attempt_id:
            shutil.rmtree(str(claim.shard_dir / "lease"), ignore_errors=True)

    def fail(
        self,
        claim: Claim,
        reason: str,
        retryable: bool,
        error: str,
        backoff_seconds: int,
        now: float,
    ) -> None:
        if reason not in FAILURE_REASONS:
            raise ValueError(f"invalid failure reason: {reason}")
        owner = read_json(claim.attempt_dir / "owner.json", {})
        owner_before = read_json(claim.shard_dir / "lease" / "owner.json", {})
        owned_before = owner_before.get("attempt_id") == claim.attempt_id
        atomic_write_json(
            claim.attempt_dir / "failed.json",
            {
                "attempt_id": claim.attempt_id,
                "worker_id": owner.get("worker_id", claim.attempt_id.rsplit("-", 1)[0]),
                "handle": owner.get("handle", ""),
                "failed_unix": now,
                "reason": reason,
                "retryable": bool(retryable),
                "error": error,
                "retry_after_unix": now + int(backoff_seconds),
            },
        )
        current = read_json(claim.shard_dir / "lease" / "owner.json", {})
        if owned_before and current.get("attempt_id") == claim.attempt_id:
            shutil.rmtree(str(claim.shard_dir / "lease"), ignore_errors=True)

    def cut(self, job_id: str, partition: str, count: int, est_seconds: float, now: float) -> str:
        if count <= 0:
            raise ValueError("cut count must be positive")
        partition_dir = self.job_dir(job_id) / "partitions" / partition
        metadata = read_json(partition_dir / "partition.json")
        if not isinstance(metadata, dict):
            raise ValueError(f"unknown partition: {job_id}:{partition}")
        start = int(metadata.get("next_cut", 0))
        size = int(metadata["size"])
        if count > size - start:
            raise ValueError(f"cut exceeds partition: count={count}, remaining={size - start}")
        shard_id = f"{slug(partition)}-{start:06d}-{start + count - 1:06d}"
        shard_dir = partition_dir / "shards" / shard_id
        shard_dir.mkdir(parents=True, exist_ok=True)
        shard_payload = {
            "shard_id": shard_id,
            "partition": partition,
            "start": start,
            "count": count,
            "created_unix": now,
            "est_seconds": est_seconds,
            "parent_shard_id": None,
            "depth": 0,
        }
        existing = read_json(shard_dir / "shard.json")
        if existing is not None and any(existing.get(key) != shard_payload[key] for key in ("shard_id", "partition", "start", "count")):
            raise ValueError(f"conflicting existing shard: {shard_dir}")
        atomic_write_json(shard_dir / "shard.json", existing or shard_payload)
        metadata["next_cut"] = start + count
        if metadata["next_cut"] == size:
            metadata["status"] = "cut"
        atomic_write_json(partition_dir / "partition.json", metadata)
        return shard_id

    def expire_leases(
        self,
        job_id: str,
        ttl_seconds: int,
        is_alive: Callable[[dict], bool],
        now: float,
    ) -> List[str]:
        expired = []
        job = self.scan_job(job_id, now)
        for partition in job.partitions.values():
            for shard in partition.shards:
                if shard.state != ShardState.LEASED:
                    continue
                owner = shard.lease_owner or {}
                heartbeat = shard.heartbeat_unix
                if heartbeat is None:
                    heartbeat = float(owner.get("acquired_unix", 0))
                age = now - heartbeat
                if age <= ttl_seconds or (age <= 2 * ttl_seconds and is_alive(owner)):
                    continue
                shard_dir = self.job_dir(job_id) / "partitions" / partition.name / "shards" / shard.shard_id
                attempt_id = str(owner.get("attempt_id", "unknown"))
                attempt_dir = shard_dir / "attempts" / attempt_id
                attempt_dir.mkdir(parents=True, exist_ok=True)
                if not (attempt_dir / "complete.json").is_file():
                    atomic_write_json(
                        attempt_dir / "failed.json",
                        {
                            "attempt_id": attempt_id,
                            "worker_id": owner.get("worker_id", ""),
                            "handle": owner.get("handle", ""),
                            "failed_unix": now,
                            "reason": "lease_expired",
                            "retryable": True,
                            "error": f"heartbeat age {age:.3f}s exceeded ttl {ttl_seconds}s",
                            "retry_after_unix": now,
                        },
                    )
                current = read_json(shard_dir / "lease" / "owner.json", {})
                if current.get("attempt_id") == attempt_id:
                    shutil.rmtree(str(shard_dir / "lease"), ignore_errors=True)
                    expired.append(shard.shard_id)
        return expired

    def _find_shard(self, job_id: str, shard_id: str) -> Tuple[Path, dict]:
        for path in (self.job_dir(job_id) / "partitions").glob("*/shards/*"):
            if path.is_dir() and path.name == shard_id:
                payload = read_json(path / "shard.json")
                if isinstance(payload, dict):
                    return path, payload
        raise ValueError(f"unknown shard: {job_id}:{shard_id}")

    def split(self, job_id: str, shard_id: str, now: float) -> Tuple[str, str]:
        parent_dir, parent = self._find_shard(job_id, shard_id)
        count = int(parent["count"])
        if count <= 1:
            raise ValueError("cannot split a one-unit shard")
        left_count = count // 2
        right_count = count - left_count
        children = []
        for start, child_count in ((int(parent["start"]), left_count), (int(parent["start"]) + left_count, right_count)):
            child_id = f"{slug(str(parent['partition']))}-{start:06d}-{start + child_count - 1:06d}"
            child_dir = parent_dir.parent / child_id
            child_dir.mkdir(parents=True, exist_ok=True)
            child_payload = {
                "shard_id": child_id,
                "partition": parent["partition"],
                "start": start,
                "count": child_count,
                "created_unix": now,
                "est_seconds": float(parent.get("est_seconds", 0)) * child_count / count,
                "parent_shard_id": shard_id,
                "depth": int(parent.get("depth", 0)) + 1,
            }
            existing = read_json(child_dir / "shard.json")
            if existing is not None and any(existing.get(key) != child_payload[key] for key in ("shard_id", "partition", "start", "count", "parent_shard_id")):
                raise ValueError(f"conflicting split child: {child_dir}")
            atomic_write_json(child_dir / "shard.json", existing or child_payload)
            children.append(child_id)
        atomic_write_json(parent_dir / "superseded.json", {"children": children, "superseded_unix": now})
        return children[0], children[1]

    def quarantine(self, job_id: str, shard_id: str, reason: str, now: float) -> None:
        shard_dir, _ = self._find_shard(job_id, shard_id)
        failures = _attempt_failures(shard_dir)
        atomic_write_json(
            shard_dir / "quarantine.json",
            {
                "reason": reason,
                "errors": sum(str(row.get("reason")) not in PREEMPTION_REASONS for row in failures),
                "preemptions": sum(str(row.get("reason")) in PREEMPTION_REASONS for row in failures),
                "quarantined_unix": now,
            },
        )

    def set_partition_status(self, job_id: str, partition: str, status: str) -> None:
        if status not in PARTITION_STATUSES:
            raise ValueError(f"invalid partition status: {status}")
        path = self.job_dir(job_id) / "partitions" / partition / "partition.json"
        payload = read_json(path)
        if not isinstance(payload, dict):
            raise ValueError(f"unknown partition: {job_id}:{partition}")
        payload["status"] = status
        atomic_write_json(path, payload)
