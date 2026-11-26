from __future__ import annotations

import fcntl
import logging
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict

import shardq

from .cutter import plan_cut, update_history
from .fs import atomic_write_text
from .launchers import get_launcher
from . import submission
from .launchers.base import Launcher
from .ledger import Ledger, ShardState, atomic_write_json, read_json
from .merge import merge_partition
from .spec import PoolSpec, load_pools, resolve_pools_path


LOGGER = logging.getLogger("shardq.controller")
_LAUNCHERS = {}
_LOG_ROOT = None
STATUS_THREADS = 8
SUBMIT_THREADS = 4
MERGE_THREADS = 2
MERGE_RETRY_SECONDS = 600


def _package_root() -> Path:
    return Path(shardq.__file__).resolve().parents[1]


def _launcher(root: Path, name: str, pool: PoolSpec):
    key = (str(root), name, pool.name)
    cached = _LAUNCHERS.get(key)
    if cached is None or (isinstance(cached, Launcher) and cached.pool != pool):
        _LAUNCHERS[key] = get_launcher(name)(root, pool, _package_root())
    return _LAUNCHERS[key]


def _launcher_block(pool: PoolSpec, launcher_name: str) -> dict:
    if launcher_name == "volc":
        return pool.volc
    if launcher_name == "aihc":
        return pool.aihc
    return {}


def _last_failure_reason(shard_dir: Path) -> str:
    failures = []
    for path in (shard_dir / "attempts").glob("*/failed.json"):
        payload = read_json(path, {})
        if isinstance(payload, dict) and payload.get("reason") != "session_error":
            failures.append(payload)
    if not failures:
        return "backend_error"
    return str(max(failures, key=lambda row: float(row.get("failed_unix", 0))).get("reason", "backend_error"))


def _refresh_workers(root: Path, launcher_name: str, pools: Dict[str, PoolSpec], now: float,
                     pools_path: str = "") -> Dict[str, dict]:
    managed_pools = {name for name, pool in pools.items() if pool.launcher == launcher_name}
    # Switching a pool's launcher cannot discard a reservation in that pool.
    records = [row for _, row in submission.worker_records(root) if row["launcher"] == launcher_name
               or (row.get("pool") in managed_pools and submission.occupies(row))]

    def refresh(row):
        pool = pools.get(row.get("pool"))
        if pool is None or pool.launcher != row["launcher"]:
            if submission.occupies(row) and row.get("submission_state") != "conflict":
                message = "original pool or launcher is no longer configured"
                if row.get("submission_state"):
                    submission.conflict(row, message)
                else:
                    row["status_error"] = message
                submission.save(root, row)
            return row
        try:
            launcher = _launcher(root, launcher_name, pool)
        except Exception as exc:
            row["status_error"] = f"{type(exc).__name__}: {exc}"
            if row.get("submission_state") in {"submitting", "uncertain"} and now >= row.get("reconcile_after_unix", 0):
                submission.uncertain(row, row["status_error"], now)
            submission.save(root, row)
            return row
        return submission.refresh(root, pool, launcher, pools_path, _package_root(), row, now)

    if not records:
        return {}
    with ThreadPoolExecutor(max_workers=min(STATUS_THREADS, len(records))) as executor:
        return {row["worker_id"]: row for row in executor.map(refresh, records)}


def _owner_alive(workers: dict, pool: str, owner: dict) -> bool:
    worker = workers.get(owner.get("worker_id"))
    if worker is None and owner.get("handle"):
        # Legacy leases may only carry a launcher handle.
        worker = next((row for row in workers.values()
                       if row.get("pool") == pool and row.get("handle") == owner["handle"]), None)
    return bool(worker and worker.get("pool") == pool and (worker.get("last_status") in {"queued", "running"}
                or worker.get("submission_state") in submission.UNRESOLVED))


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
            partition_paths.setdefault(key, []).append((partition_dir / "partition.json", partition.unit_seconds_est))
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
        for metadata_path, previous_estimate in partition_paths.get(key, []):
            if previous_estimate != estimate:
                Ledger.update_partition(metadata_path, unit_seconds_est=estimate)
    if changed or not history_path.exists():
        atomic_write_json(history_path, history)


def _job_summary(job) -> dict:
    return {
        "job_id": job.job_id,
        "pool": job.pool,
        "column": job.column,
        "stopped": job.stopped,
        "setup_status": job.setup_status,
        "session_errors": job.session_errors,
        "session_retry_after_unix": job.session_retry_after_unix,
        "partitions": {
            name: {
                "size": row.size,
                "next_cut": row.next_cut,
                "done_units": row.done_units,
                "quarantined_units": row.quarantined_units,
                "done": row.done,
                "leased": row.leased,
                "active_leases": sum(shard.lease_present for shard in row.shards),
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
        lines.extend([f"## {job['job_id']}", ""])
        if job["setup_status"] != "ready":
            lines.extend([f"Session setup {job['setup_status']} ({job['session_errors']} failures).", ""])
        lines.extend(["| partition | size | cut | done units | pending | leased | quarantined | status |", "|---|---:|---:|---:|---:|---:|---:|---|"])
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
    atomic_write_text(path, "\n".join(lines) + "\n")


def _submit_workers(root, pool, pools_path, demand, workers, backoff, max_submits,
                    actions, warnings, now):
    rows = [row for row in workers.values() if row["pool"] == pool.name]
    intents = [row for row in rows if row.get("submission_state") == "intent"]
    # A waiting intent already owns its place. Admit those identities first,
    # deducting the in-flight workers separately so a reservation can be used.
    live = [row for row in rows if row.get("submission_state") != "intent" and submission.occupies(row)]
    occupied = sum(submission.occupies(row) for row in live)
    wanted = max(0, min(demand, pool.max_workers) - occupied)
    for row in intents[wanted:]:
        row.update(submission_state="terminal", last_status="terminal:withdrawn-before-submit", dead=True)
        submission.save(root, row)
    if not wanted or backoff.get(pool.name, 0) > now or max_submits <= 0:
        return

    count = min(wanted, max_submits)
    block = _launcher_block(pool, pool.launcher)
    gpu = int(block.get("gpu_slots", 1))
    try:
        launcher = _launcher(root, pool.launcher, pool)
        slots = launcher.capacity_slots()
        inputs = launcher.submission_inputs()
    except Exception as exc:
        backoff[pool.name] = now + submission.RECONCILE_SECONDS
        warnings.append(f"{pool.name}: admission failed: {type(exc).__name__}: {exc}")
        return
    if slots is not None and gpu > 0:
        # Volc's audit budget is per queue/creator, which can span several
        # ShardQ pools. Use the original request's domain even after a config edit.
        reserved = 0
        for row in workers.values():
            state = row.get("submission_state")
            if state not in submission.UNRESOLVED and state != "intent":
                continue
            if row["pool"] == pool.name:
                if state == "intent":  # this batch can consume its own reservations
                    continue
            elif not (pool.launcher == row["launcher"] == "volc"
                      and row.get("resource_queue") == block.get("queue")
                      and row.get("resource_creator") in {None, block.get("creator")}):
                continue
            reserved += submission.occupies(row) * row.get("gpu_slots", gpu)
        count = min(count, max(0, slots - reserved) // gpu)
    if pool.launcher in {"volc", "aihc"} and (slots is None or "max_queued" in block):
        queued = sum(submission.occupies(row) for row in live if row.get("last_status") == "queued"
                     or row.get("submission_state") in submission.UNRESOLVED
                     or row.get("last_status") == "unknown")
        count = min(count, max(0, int(block.get("max_queued", 0)) - queued))
    if not count:
        return

    batch = intents[:count]
    for row in rows:
        if len(batch) == count:
            break
        if row.get("submission_state") == "rejected":
            command = submission.worker_command(root, pool, pools_path, _package_root(), row["worker_id"])
            if row.get("request_fingerprint") == submission.fingerprint(pool, inputs, command):
                row.update(submission_state="intent", last_status="intent", dead=False)
                submission.save(root, row)
                batch.append(row)
    while len(batch) < count:
        row = submission.intent(root, pool, inputs, pools_path, _package_root(), gpu, now)
        batch.append(row)
        workers[row["worker_id"]] = row

    # Every intent in the batch is visible before the first external call.
    def dispatch(row):
        return submission.dispatch(root, pool, launcher, inputs, pools_path, _package_root(), row, now)

    with ThreadPoolExecutor(max_workers=min(SUBMIT_THREADS, count), thread_name_prefix="shardq-submit") as executor:
        for row in executor.map(dispatch, batch):
            state = row["submission_state"]
            if state == "confirmed":
                actions.append(dict(action="submit", pool=pool.name, worker_id=row["worker_id"], handle=row["handle"]))
            elif state == "rejected":
                LOGGER.warning("worker submission throttled for pool %s: %s", pool.name, row["submission_error"])
                warnings.append(f"{pool.name}: submit throttled: {row['submission_error']}")
            else:
                LOGGER.error("worker submission failed for pool %s: %s", pool.name, row["submission_error"])
                backoff[pool.name] = now + submission.RECONCILE_SECONDS
                warnings.append(f"{pool.name}: submit failed: {row['submission_error']}")


def tick(root: Path, launcher_name: str, pools: Dict[str, PoolSpec], opts: dict, now: float) -> dict:
    root = Path(root).resolve()
    ledger = Ledger(root)
    actions = []
    warnings = []
    tick_started = time.time()
    pools_path = str(opts.get("_pools_path") or "")
    workers = _refresh_workers(root, launcher_name, pools, now, pools_path)
    workers_polled = sum(1 for row in workers.values() if row.get("last_checked_unix") == now)
    relevant_pool_names = {name for name, pool in pools.items() if pool.launcher == launcher_name}
    job_ids = [
        job_id
        for job_id in ledger.list_jobs()
        if str(read_json(ledger.job_dir(job_id) / "job.json", {}).get("pool")) in relevant_pool_names
    ]

    for job_id in job_ids:
        job = ledger.scan_job(job_id, now)
        ttl = int((job.spec.get("retry") or {}).get("lease_ttl_seconds", 900))
        for shard_id in ledger.expire_leases(job_id, ttl, lambda owner: _owner_alive(workers, job.pool, owner), now):
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

    max_cuts = int(opts.get("max_cuts_per_tick", 64))
    for job_id in job_ids:
        job = ledger.scan_job(job_id, now)
        if job.claims_blocked:
            continue
        shard_cfg = job.spec.get("shard") or {}
        lookahead = int(shard_cfg.get("lookahead", 4))
        for partition in job.partitions.values():
            next_cut = partition.next_cut
            ahead = partition.pending + partition.backoff
            cuts = 0
            while next_cut < partition.size and ahead < lookahead and cuts < max_cuts:
                count = plan_cut(partition.size, next_cut, partition.unit_seconds_est, shard_cfg)
                if not count:
                    break
                shard_id = ledger.cut(job_id, partition.name, count, count * partition.unit_seconds_est, now)
                actions.append({"action": "cut", "job_id": job_id, "partition": partition.name, "shard_id": shard_id, "count": count})
                next_cut += count
                ahead += 1
                cuts += 1

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
        demand = sum(part.pending + part.backoff + part.leased
                     for job in current_jobs if job.pool == pool_name and not job.claims_blocked
                     for part in job.partitions.values())
        _submit_workers(root, pool, pools_path, demand, workers, backoff, max_submits,
                        actions, warnings, now)

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
        "shards_cut": sum(action["action"] == "cut" for action in actions),
        "workers_submitted": sum(action["action"] == "submit" for action in actions),
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
            try:
                state = tick(root, launcher_name, pools, opts, time.time())
            except Exception:
                if once:
                    raise
                LOGGER.exception("tick failed; retrying in %ds", poll_seconds)
                time.sleep(poll_seconds)
                continue
            LOGGER.info(
                "tick actions=%d warnings=%d workers_polled=%d cut=%d submitted=%d merges=%d seconds=%.1f",
                len(state["actions"]),
                len(state["warnings"]),
                int(state.get("workers_polled", 0)),
                int(state.get("shards_cut", 0)),
                int(state.get("workers_submitted", 0)),
                len(state.get("merges_in_flight", [])),
                float(state.get("tick_seconds", 0.0)),
            )
            if once:
                return 0
            if exit_when_idle:
                # Stopped jobs cannot make progress until explicitly resumed.
                # Their unfinished partitions must not keep a batch scheduler alive.
                complete = all(
                    partition["status"] in {"merged", "partial"}
                    for job in state["jobs"]
                    if not job.get("stopped", False) and job.get("setup_status") != "paused"
                    for partition in job["partitions"].values()
                )
                alive = any(submission.occupies(worker) for worker in state["workers"])
                leased = any(
                    partition.get("active_leases", partition.get("leased", 0)) > 0
                    for job in state["jobs"]
                    for partition in job["partitions"].values()
                )
                if complete and not alive and not leased and not state.get("merges_in_flight"):
                    LOGGER.info("idle: active jobs finished, workers and merges drained; exiting")
                    return 0
            time.sleep(poll_seconds)
