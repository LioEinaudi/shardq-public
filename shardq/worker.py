from __future__ import annotations

import os
import signal
import socket
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional

from .backends import get_backend
from .backends.base import RangeValidationError, WorkerRestartRequired
from .ledger import Claim, Ledger, ShardState, read_json
from .spec import PoolSpec, load_pools, pool_from_dict, resolve_pools_path


class PreemptedError(BaseException):
    pass


def _pool_from_ledger(
    root: Path, name: str, pools_path: Optional[Path] = None
) -> PoolSpec:
    configured = (
        pools_path is not None
        or bool(os.environ.get("SHARDQ_POOLS"))
        or (Path(root).resolve() / "pools.json").is_file()
    )
    pools = {}
    if configured:
        path = resolve_pools_path(
            str(pools_path) if pools_path is not None else None, root
        )
        pools = load_pools(path)
    if name in pools:
        return pools[name]
    ledger = Ledger(root)
    for job_id in ledger.list_jobs():
        payload = read_json(ledger.job_dir(job_id) / "job.json", {})
        snapshot = payload.get("pool_snapshot")
        if payload.get("pool") == name and isinstance(snapshot, dict):
            return pool_from_dict(name, snapshot)
    raise ValueError(f"unknown pool: {name}")


def _next_estimate(ledger: Ledger, pool: str, job_ids: List[str], now: float) -> Optional[float]:
    estimates = []
    for job_id in job_ids or ledger.list_jobs():
        job = ledger.scan_job(job_id, now)
        if job.pool != pool or job.claims_blocked:
            continue
        for partition in job.partitions.values():
            for shard in partition.shards:
                if shard.state == ShardState.PENDING:
                    estimates.append(shard.count * partition.unit_seconds_est)
    return min(estimates) if estimates else None


def _identity(root: Path, pool: PoolSpec, worker_id: str) -> dict:
    record = read_json(root / "workers" / f"{worker_id}.json", {})
    return {
        "worker_id": worker_id,
        "launcher": str(record.get("launcher") or os.environ.get("SHARDQ_LAUNCHER") or pool.launcher),
        "handle": str(record.get("handle") or os.environ.get("SHARDQ_HANDLE") or ""),
    }


def _heartbeat_loop(ledger: Ledger, claim: Claim, stop: threading.Event, seconds: float) -> None:
    while not stop.wait(seconds):
        try:
            ledger.heartbeat(claim, time.time())
        except Exception:
            return


@contextmanager
def _maintain_lease(ledger: Ledger, claim: Claim, interval: float):
    stop = threading.Event()
    thread = threading.Thread(target=_heartbeat_loop, args=(ledger, claim, stop, interval), daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()


def run_worker(
    root: Path,
    pool: str,
    worker_id: str,
    max_wall_seconds: int,
    idle_exit_seconds: int,
    heartbeat_seconds: int = 60,
    job_ids: Optional[List[str]] = None,
    pools_path: Optional[Path] = None,
) -> int:
    root = Path(root).resolve()
    pool_spec = _pool_from_ledger(root, pool, pools_path)
    ledger = Ledger(root)
    allowed_jobs = list(job_ids or [])
    deadline = time.time() + max_wall_seconds
    idle = 0.0
    session = None
    session_identity = None
    prefer_session_key = None
    prefer_partition = None
    active_claim = None

    # The launcher learns the external handle only after submission returns.
    # Wait briefly so the first claimed lease records that durable handle.
    worker_record = root / "workers" / f"{worker_id}.json"
    record_deadline = time.time() + 5.0
    while not worker_record.is_file() and time.time() < record_deadline:
        time.sleep(0.05)

    def preempted(signum, frame):
        del signum, frame
        raise PreemptedError()

    previous_term = signal.signal(signal.SIGTERM, preempted)
    previous_int = signal.signal(signal.SIGINT, preempted)
    try:
        while True:
            now = time.time()
            if now >= deadline:
                break
            estimate = _next_estimate(ledger, pool, allowed_jobs, now)
            if estimate is not None and deadline - now < 1.5 * estimate:
                break
            claim = ledger.claim(
                pool,
                allowed_jobs,
                _identity(root, pool_spec, worker_id),
                prefer_partition,
                prefer_session_key,
            )
            if claim is None:
                if idle >= idle_exit_seconds:
                    break
                sleep_seconds = min(30.0, max(0.1, idle_exit_seconds - idle))
                time.sleep(sleep_seconds)
                idle += sleep_seconds
                continue
            idle = 0.0
            active_claim = claim
            job_spec = read_json(ledger.job_dir(claim.job_id) / "job.json", {})
            current_retry = dict(job_spec.get("retry") or {})
            interval = min(heartbeat_seconds, float(current_retry.get("lease_ttl_seconds", 900)) / 2)
            with _maintain_lease(ledger, claim, interval):
                try:
                    backend = get_backend(str(job_spec["backend"]))(
                        dict(job_spec.get("backend_args") or {}),
                        asdict(pool_spec),
                        dict(job_spec.get("pins") or {}),
                    )
                    # The saved key is an affinity hint and may predate the current backend.
                    prefer_session_key = claim.session_key
                    identity = (job_spec["backend"], backend.session_key())
                    if session is None or session_identity != identity:
                        if session is not None:
                            previous_session, session = session, None
                            previous_session.close()
                        session = backend.open_session()
                        session_identity = identity
                except Exception as exc:
                    if isinstance(exc, WorkerRestartRequired) and session_identity is not None:
                        ledger.fail(claim, "worker_restart", True, str(exc), 0, time.time())
                        active_claim = None
                        return 0
                    ledger.fail(
                        claim, "session_error", True, traceback.format_exc()[-4000:],
                        int(current_retry.get("backoff_seconds", 300)), time.time(),
                    )
                    active_claim = None
                    return 2
                started = time.time()
                try:
                    result = session.run_range(
                        claim.partition, claim.start, claim.count, claim.attempt_dir
                    )
                    backend.validate_range(claim.partition, claim.start, claim.count, result)
                    ledger.complete(
                        claim,
                        result.records_file,
                        result.count,
                        started,
                        result.elapsed_seconds,
                        result.extra,
                        time.time(),
                    )
                    prefer_partition = claim.partition
                except RangeValidationError as exc:
                    ledger.fail(
                        claim,
                        "validation_error",
                        True,
                        str(exc),
                        int(current_retry.get("backoff_seconds", 300)),
                        time.time(),
                    )
                except Exception:
                    ledger.fail(
                        claim,
                        "backend_error",
                        True,
                        traceback.format_exc()[-4000:],
                        int(current_retry.get("backoff_seconds", 300)),
                        time.time(),
                    )
            active_claim = None
    except PreemptedError:
        if active_claim is not None:
            ledger.fail(active_claim, "preempted", True, "worker received termination signal", 0, time.time())
        return 143
    finally:
        if session is not None:
            session.close()
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
    return 0


def default_worker_id(pool: str) -> str:
    return f"{pool}-{socket.gethostname()}-{int(time.time())}"
