from __future__ import annotations

import fcntl
import json
import logging
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from typing import Dict

import shardq

from .cutter import plan_cut, update_history
from .launchers import get_launcher
from .ledger import Ledger, ShardState, atomic_write_json, read_json
from .merge import merge_partition
from .spec import PoolSpec, load_pools, resolve_pools_path, slug


LOGGER = logging.getLogger("shardq.controller")
_LAUNCHERS = {}
_LOG_ROOT = None
STATUS_THREADS = 8
MERGE_THREADS = 2
MERGE_RETRY_SECONDS = 600


def _package_root() -> Path:
    return Path(shardq.__file__).resolve().parents[1]


def _launcher(root: Path, name: str, pool: PoolSpec):
    key = (str(root), name, pool.name)
    if key not in _LAUNCHERS:
        _LAUNCHERS[key] = get_launcher(name)(root, pool, _package_root())
    return _LAUNCHERS[key]


def _worker_files(root: Path) -> list:
    workers = root / "workers"
    if not workers.is_dir():
        return []
    return sorted(workers.glob("*.json"))


def _last_failure_reason(shard_dir: Path) -> str:
    failures = []
    for path in (shard_dir / "attempts").glob("*/failed.json"):
        payload = read_json(path, {})
        if isinstance(payload, dict):
            failures.append(payload)
    if not failures:
        return "backend_error"
    return str(max(failures, key=lambda row: float(row.get("failed_unix", 0))).get("reason", "backend_error"))


def _refresh_workers(root: Path, launcher_name: str, pools: Dict[str, PoolSpec], now: float) -> Dict[str, dict]:
    workers = {}
    pending = []
    for path in _worker_files(root):
        row = read_json(path, {})
        if not isinstance(row, dict) or row.get("launcher") != launcher_name:
            continue
        pool_name = str(row.get("pool", ""))
        if pool_name not in pools or pools[pool_name].launcher != launcher_name:
            continue
        handle = str(row.get("handle", ""))
        if row.get("dead") and str(row.get("last_status", "")).startswith("terminal:"):
            # Terminal launcher states are final: keep the recorded row and skip the
            # launcher call so tick time does not grow with worker history.
            workers[handle] = row
            continue
        launcher = _launcher(root, launcher_name, pools[pool_name])
        pending.append((path, row, handle, launcher))

    def refresh(item):
        path, row, handle, launcher = item
        try:
            status = launcher.status(handle)
            row.pop("status_error", None)
        except Exception as exc:
            status = str(row.get("last_status", "unknown"))
            row["status_error"] = f"{type(exc).__name__}: {exc}"
        row["last_status"] = status
        row["dead"] = status.startswith("terminal:")
        row["last_checked_unix"] = now
        atomic_write_json(path, row)
        return handle, row

    if pending:
        with ThreadPoolExecutor(max_workers=min(STATUS_THREADS, len(pending))) as executor:
            for handle, row in executor.map(refresh, pending):
                workers[handle] = row
    return workers


def _merge_executor(opts: dict) -> ThreadPoolExecutor:
    executor = opts.get("_merge_executor")
    if executor is None:
        executor = ThreadPoolExecutor(max_workers=MERGE_THREADS, thread_name_prefix="shardq-merge")
        opts["_merge_executor"] = executor
    return executor


def _run_merge(root: Path, job, partition: str, pool: PoolSpec, allow_partial: bool) -> dict:
    try:
        return merge_partition(root, job, partition, pool, allow_partial, time.time())
    except Exception:
        LOGGER.exception("merge failed for %s:%s", job.job_id, partition)
        raise


def _update_history(root: Path, jobs: list, now: float, actions: list) -> None:
    history_path = root / "history.json"
    history = read_json(history_path, {})
    if not isinstance(history, dict):
        history = {}
    events = {}
    partition_paths = {}
    for job in jobs:
        backend = str(job.spec["backend"])
        session_key = str(job.spec.get("session_key", ""))
        for partition in job.partitions.values():
            key = f"{backend}:{session_key}:{partition.name}"
            partition_dir = root / "jobs" / job.job_id / "partitions" / partition.name
            partition_paths.setdefault(key, []).append(partition_dir / "partition.json")
            for shard in partition.shards:
                shard_dir = partition_dir / "shards" / shard.shard_id
                for complete_path in sorted((shard_dir / "attempts").glob("*/complete.json")):
                    complete = read_json(complete_path, {})
                    count = int(complete.get("count", 0))
                    if count <= 0:
                        continue
                    events.setdefault(key, []).append(
                        (
                            float(complete.get("completed_unix", 0)),
                            str(complete_path),
                            float(complete.get("elapsed_seconds", 0)),
                            count,
                        )
                    )

    changed = False
    for key, rows in events.items():
        rebuilt = {}
        for completed_unix, identity, elapsed, count in sorted(rows):
            update_history(rebuilt, key, elapsed, count, completed_unix or now)
        if history.get(key) != rebuilt[key]:
            history[key] = rebuilt[key]
            changed = True
            actions.append({"action": "history", "key": key, "attempts": len(rows)})
        estimate = float(rebuilt[key]["unit_seconds"])
        for metadata_path in partition_paths.get(key, []):
            metadata = read_json(metadata_path, {})
            if metadata.get("unit_seconds_est") != estimate:
                metadata["unit_seconds_est"] = estimate
                atomic_write_json(metadata_path, metadata)
                changed = True
    if changed or not history_path.exists():
        atomic_write_json(history_path, history)


def _job_summary(job) -> dict:
    return {
        "job_id": job.job_id,
        "pool": job.pool,
        "column": job.column,
        "stopped": job.stopped,
        "partitions": {
            name: {
                "size": row.size,
                "next_cut": row.next_cut,
                "done_units": row.done_units,
                "quarantined_units": row.quarantined_units,
                "done": row.done,
                "leased": row.leased,
                "pending": row.pending,
                "backoff": row.backoff,
                "quarantined": row.quarantined,
                "superseded": row.superseded,
                "status": row.status,
            }
            for name, row in job.partitions.items()
        },
    }


def _write_status(root: Path, state: dict) -> None:
    lines = ["# ShardQ scheduler status", "", f"Updated unix: {state['updated_unix']}", ""]
    for job in state["jobs"]:
        lines.extend([f"## {job['job_id']}", "", "| partition | size | cut | done units | pending | leased | quarantined | status |", "|---|---:|---:|---:|---:|---:|---:|---|"])
        for name, row in job["partitions"].items():
            lines.append(
                f"| {name} | {row['size']} | {row['next_cut']} | {row['done_units']} | "
                f"{row['pending']} | {row['leased']} | {row['quarantined']} | {row['status']} |"
            )
        lines.append("")
    lines.extend(["## Workers", "", "| worker | pool | handle | status |", "|---|---|---|---|"])
    for worker in state["workers"]:
        lines.append(
            f"| {worker.get('worker_id', '')} | {worker.get('pool', '')} | "
            f"{worker.get('handle', '')} | {worker.get('last_status', '')} |"
        )
    path = root / "scheduler" / "status.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(str(temporary), str(path))


def tick(root: Path, launcher_name: str, pools: Dict[str, PoolSpec], opts: dict, now: float) -> dict:
    root = Path(root).resolve()
    ledger = Ledger(root)
    actions = []
    warnings = []
    tick_started = time.time()
    workers = _refresh_workers(root, launcher_name, pools, now)
    workers_polled = sum(1 for row in workers.values() if row.get("last_checked_unix") == now)
    relevant_pool_names = {name for name, pool in pools.items() if pool.launcher == launcher_name}
    job_ids = [
        job_id
        for job_id in ledger.list_jobs()
        if str(read_json(ledger.job_dir(job_id) / "job.json", {}).get("pool")) in relevant_pool_names
    ]

    def alive(owner: dict) -> bool:
        status = str((workers.get(str(owner.get("handle"))) or {}).get("last_status", "unknown"))
        return status in {"running", "queued"}

    for job_id in job_ids:
        job = ledger.scan_job(job_id, now)
        ttl = int((job.spec.get("retry") or {}).get("lease_ttl_seconds", 900))
        for shard_id in ledger.expire_leases(job_id, ttl, alive, now):
            actions.append({"action": "expire", "job_id": job_id, "shard_id": shard_id})

    for job_id in job_ids:
        job = ledger.scan_job(job_id, now)
        retry = job.spec.get("retry") or {}
        max_depth = int(math.ceil(math.log(max(1, int((job.spec.get("shard") or {}).get("max_units", 1))), 2)))
        for partition in job.partitions.values():
            for shard in partition.shards:
                if shard.state not in {ShardState.PENDING, ShardState.PENDING_BACKOFF}:
                    continue
                exceeded = shard.errors >= int(retry.get("max_errors", 3)) or shard.preemptions >= int(retry.get("max_preemptions", 20))
                if not exceeded:
                    continue
                if bool(retry.get("bisect", True)) and shard.count > 1 and shard.depth < max_depth:
                    children = ledger.split(job_id, shard.shard_id, now)
                    actions.append({"action": "split", "job_id": job_id, "shard_id": shard.shard_id, "children": list(children)})
                else:
                    shard_dir = ledger.job_dir(job_id) / "partitions" / partition.name / "shards" / shard.shard_id
                    reason = _last_failure_reason(shard_dir)
                    ledger.quarantine(job_id, shard.shard_id, reason, now)
                    actions.append({"action": "quarantine", "job_id": job_id, "shard_id": shard.shard_id, "reason": reason})

    jobs = [ledger.scan_job(job_id, now) for job_id in job_ids]
    _update_history(root, jobs, now, actions)

    for job_id in job_ids:
        job = ledger.scan_job(job_id, now)
        if job.stopped:
            continue
        shard_cfg = job.spec.get("shard") or {}
        for partition in job.partitions.values():
            if partition.next_cut >= partition.size or partition.pending + partition.backoff >= int(shard_cfg.get("lookahead", 4)):
                continue
            count = plan_cut(partition.size, partition.next_cut, partition.unit_seconds_est, shard_cfg)
            if count:
                shard_id = ledger.cut(job_id, partition.name, count, count * partition.unit_seconds_est, now)
                actions.append({"action": "cut", "job_id": job_id, "partition": partition.name, "shard_id": shard_id, "count": count})

    # Merges run in background threads so a slow aggregation never stalls the tick.
    merges = opts.setdefault("_merges", {})
    merge_retry_after = opts.setdefault("_merge_retry_after", {})
    for key, entry in list(merges.items()):
        future = entry["future"]
        if not future.done():
            continue
        merges.pop(key)
        try:
            report = future.result()
            actions.append(
                {
                    "action": "merge",
                    "job_id": key[0],
                    "partition": key[1],
                    "missing_units": report["missing_units"],
                    "seconds": round(now - entry["started_unix"], 1),
                }
            )
        except Exception as exc:
            merge_retry_after[key] = now + MERGE_RETRY_SECONDS
            warnings.append(f"{key[0]}:{key[1]}: merge failed: {type(exc).__name__}: {exc}")

    for job_id in job_ids:
        job = ledger.scan_job(job_id, now)
        pool = pools[job.pool]
        for name, partition in job.partitions.items():
            terminal_states = {ShardState.DONE, ShardState.SUPERSEDED, ShardState.QUARANTINED}
            ready = partition.next_cut == partition.size and bool(partition.shards) and all(shard.state in terminal_states for shard in partition.shards)
            if not ready or partition.status in {"merged", "partial"}:
                continue
            key = (job_id, name)
            if key in merges or float(merge_retry_after.get(key, 0)) > now:
                continue
            has_missing = partition.quarantined_units > 0
            if opts.get("require_complete") and has_missing:
                warnings.append(f"{job_id}:{name}: {partition.quarantined_units} quarantined units prevent complete merge")
                continue
            future = _merge_executor(opts).submit(
                _run_merge, root, job, name, pool, not bool(opts.get("require_complete"))
            )
            merges[key] = {"future": future, "started_unix": now}
            actions.append({"action": "merge_start", "job_id": job_id, "partition": name})

    backoff = opts.setdefault("_pool_backoff", {})
    current_jobs = [ledger.scan_job(job_id, now) for job_id in job_ids]
    max_submits = int(opts.get("max_submits_per_tick", 4))
    for pool_name in sorted(relevant_pool_names):
        pool = pools[pool_name]
        if float(backoff.get(pool_name, 0)) > now:
            continue
        pool_jobs = [job for job in current_jobs if job.pool == pool_name and not job.stopped]
        claimable = 0
        uncut_shards = 0
        for job in pool_jobs:
            shard_cfg = job.spec.get("shard") or {}
            for partition in job.partitions.values():
                claimable += partition.pending
                remaining = max(0, partition.size - partition.next_cut)
                estimated_units = max(1, plan_cut(partition.size, partition.next_cut, partition.unit_seconds_est, shard_cfg)) if remaining else 1
                uncut_shards += int(math.ceil(float(remaining) / estimated_units)) if remaining else 0
        demand = claimable + uncut_shards
        pool_workers = [row for row in workers.values() if row.get("pool") == pool_name]
        non_dead = [row for row in pool_workers if not str(row.get("last_status", "")).startswith("terminal:")]
        queued = sum(row.get("last_status") == "queued" for row in non_dead)
        running = len(non_dead)
        to_submit = max(0, min(demand - running, pool.max_workers - running, max_submits))
        launcher = _launcher(root, launcher_name, pool)
        slots = launcher.capacity_slots()
        gpu_slots = int(pool.volc.get("gpu_slots", 1))
        if slots is not None:
            to_submit = min(to_submit, int(slots) // max(1, gpu_slots))
        elif launcher_name == "volc":
            max_queued = int(pool.volc.get("max_queued", 0))
            to_submit = (
                0 if queued >= max_queued else min(to_submit, max_queued - queued)
            )
        for index in range(to_submit):
            worker_id = f"shardq-{slug(pool_name)}-{int(now * 1000)}-{index}"
            pools_path = str(opts.get("_pools_path") or "")
            env = dict(pool.env)
            env.update(
                {
                    "SHARDQ_ROOT": str(root),
                    "SHARDQ_POOL": pool_name,
                    "SHARDQ_POOLS": pools_path,
                    "SHARDQ_WORKER_ID": worker_id,
                    "SHARDQ_MAX_WALL": str(pool.worker_max_wall_seconds),
                    "SHARDQ_IDLE_EXIT": str(pool.worker_idle_exit_seconds),
                    "SHARDQ_PKG": str(_package_root()),
                }
            )
            worker_argv = [
                "--root", str(root), "--pool", pool_name, "--worker-id", worker_id,
                "--max-wall-seconds", str(pool.worker_max_wall_seconds),
                "--idle-exit-seconds", str(pool.worker_idle_exit_seconds),
            ]
            if pools_path:
                worker_argv.extend(["--pools", pools_path])
            try:
                handle = launcher.submit(worker_id, worker_argv, env)
                row = {
                    "worker_id": worker_id,
                    "pool": pool_name,
                    "launcher": launcher_name,
                    "handle": handle,
                    "submitted_unix": now,
                    "last_status": "queued" if launcher_name == "volc" else "running",
                    "last_checked_unix": now,
                    "dead": False,
                    "gpu_slots": gpu_slots,
                }
                atomic_write_json(root / "workers" / f"{worker_id}.json", row)
                workers[handle] = row
                actions.append({"action": "submit", "pool": pool_name, "worker_id": worker_id, "handle": handle})
            except Exception as exc:
                backoff[pool_name] = now + 300
                warnings.append(f"{pool_name}: submit failed: {type(exc).__name__}: {exc}")
                LOGGER.exception("worker submission failed for pool %s", pool_name)
                break

    final_jobs = [ledger.scan_job(job_id, now) for job_id in job_ids]
    state = {
        "schema_version": 1,
        "updated_unix": now,
        "launcher": launcher_name,
        "jobs": [_job_summary(job) for job in final_jobs],
        "workers": sorted(workers.values(), key=lambda row: str(row.get("worker_id", ""))),
        "actions": actions,
        "warnings": warnings,
        "pool_backoff": dict(backoff),
        "workers_polled": workers_polled,
        "tick_seconds": round(time.time() - tick_started, 3),
        "merges_in_flight": [
            {"job_id": key[0], "partition": key[1], "started_unix": entry["started_unix"]}
            for key, entry in sorted(merges.items())
        ],
    }
    atomic_write_json(root / "scheduler" / "state.json", state)
    _write_status(root, state)
    return state


def _configure_logging(root: Path) -> None:
    global _LOG_ROOT
    if _LOG_ROOT == str(root) and LOGGER.handlers:
        return
    for handler in list(LOGGER.handlers):
        handler.close()
        LOGGER.removeHandler(handler)
    _LOG_ROOT = str(root)
    LOGGER.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(formatter)
    LOGGER.addHandler(stream)
    log_path = root / "scheduler" / "log" / "scheduler.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def run_scheduler(
    root: Path,
    launcher_name: str,
    poll_seconds: int,
    once: bool,
    exit_when_idle: bool,
    opts: dict,
) -> int:
    root = Path(root).resolve()
    root.joinpath("scheduler").mkdir(parents=True, exist_ok=True)
    _configure_logging(root)
    pools = opts.pop("pools", None)
    pools_path = opts.pop("pools_path", None)
    if pools is None:
        pools_path = resolve_pools_path(
            str(pools_path) if pools_path is not None else None, root
        )
        pools = load_pools(pools_path)
    opts["_pools_path"] = (
        str(Path(pools_path).resolve()) if pools_path is not None else ""
    )
    lock_path = root / "scheduler" / "lock"
    with lock_path.open("w", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            LOGGER.error("another shardq scheduler holds %s", lock_path)
            return 3
        previous = read_json(root / "scheduler" / "state.json", {})
        if isinstance(previous, dict):
            opts.setdefault("_pool_backoff", dict(previous.get("pool_backoff") or {}))
        while True:
            state = tick(root, launcher_name, pools, opts, time.time())
            LOGGER.info(
                "tick actions=%d warnings=%d workers_polled=%d merges=%d seconds=%.1f",
                len(state["actions"]),
                len(state["warnings"]),
                int(state.get("workers_polled", 0)),
                len(state.get("merges_in_flight", [])),
                float(state.get("tick_seconds", 0.0)),
            )
            if once:
                return 0
            if exit_when_idle:
                complete = all(
                    partition["status"] in {"merged", "partial"}
                    for job in state["jobs"]
                    for partition in job["partitions"].values()
                )
                alive = any(
                    not str(worker.get("last_status", "")).startswith("terminal:")
                    for worker in state["workers"]
                )
                if complete and not alive:
                    return 0
            time.sleep(poll_seconds)
