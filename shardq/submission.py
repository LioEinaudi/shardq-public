"""Scheduler-owned, single-writer records for external worker submissions."""

from __future__ import annotations

import hashlib
import json
from uuid import uuid4

from .fs import atomic_write_json
from .launchers import get_launcher
from .launchers.reservations import reservation_fields
from .launchers.base import SubmissionChangedError, TransientSubmitError
from .ledger import read_json


RECONCILE_SECONDS = 300
UNRESOLVED = {"submitting", "uncertain", "conflict"}


def worker_records(root):
    for path in sorted((root / "workers").glob("*.json")):
        row = read_json(path)
        if isinstance(row, dict) and row.get("worker_id") == path.stem and row.get("launcher"):
            yield path, row


def save(root, row):
    atomic_write_json(root / "workers" / (row["worker_id"] + ".json"), row)


def occupies(row):
    if row.get("submission_state") in {"rejected", "terminal"}:
        return 0
    if row.get("submission_state") == "conflict":
        return max(1, len(row.get("match_handles", [])))
    if row.get("submission_state") in {"intent", "submitting", "uncertain"}:
        return 1
    return int(not str(row.get("last_status", "")).startswith("terminal:"))


def worker_command(root, pool, pools_path, package_root, worker_id):
    env = dict(pool.env)
    env.update(SHARDQ_ROOT=str(root), SHARDQ_POOL=pool.name, SHARDQ_POOLS=pools_path,
               SHARDQ_WORKER_ID=worker_id, SHARDQ_REQUEST_ID=worker_id,
               SHARDQ_MAX_WALL=str(pool.worker_max_wall_seconds),
               SHARDQ_IDLE_EXIT=str(pool.worker_idle_exit_seconds), SHARDQ_PKG=str(package_root))
    argv = ["--root", str(root), "--pool", pool.name, "--worker-id", worker_id,
            "--max-wall-seconds", str(pool.worker_max_wall_seconds),
            "--idle-exit-seconds", str(pool.worker_idle_exit_seconds)]
    if pools_path:
        argv.extend(["--pools", pools_path])
    return argv, env


def fingerprint(pool, inputs, command):
    # Quotas control admission. A changed queue, image, command, or execution
    # environment changes the request. Persist the digest, never the environment.
    admission_only = get_launcher(pool.launcher).admission_only_args
    block = {key: value for key, value in pool.launcher_args.items() if key not in admission_only}
    description = [pool.name, pool.launcher, pool.backend, pool.entry,
                   pool.extra, pool.local_python, block, inputs, command]
    return hashlib.sha256(json.dumps(description, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def intent(root, pool, inputs, pools_path, package_root, admission, now):
    worker_id = "shardq-" + uuid4().hex
    command = worker_command(root, pool, pools_path, package_root, worker_id)
    row = dict(worker_id=worker_id, request_id=worker_id, pool=pool.name, launcher=pool.launcher,
               request_fingerprint=fingerprint(pool, inputs, command), launcher_inputs=inputs,
               submitted_unix=now, submission_state="intent", last_status="intent", dead=False)
    row.update(reservation_fields(pool, admission))
    save(root, row)
    return row


def conflict(row, message):
    row.update(submission_state="conflict", last_status="conflict", submission_error=message, dead=False)
    row.pop("reconcile_after_unix", None)


def uncertain(row, message, now):
    row.update(submission_state="uncertain", last_status="uncertain", submission_error=message,
               reconcile_after_unix=now + RECONCILE_SECONDS, dead=False)


def confirm(row, handle, now):
    if not isinstance(handle, str) or not handle.strip():
        raise ValueError("platform returned an empty or invalid handle")
    row.update(submission_state="confirmed", handle=handle, dead=False, last_checked_unix=now,
               last_status=get_launcher(row["launcher"]).initial_status)
    row.pop("submission_error", None)
    row.pop("reconcile_after_unix", None)


def dispatch(root, pool, launcher, inputs, pools_path, package_root, row, now):
    command = worker_command(root, pool, pools_path, package_root, row["worker_id"])
    if row["worker_id"] != row.get("request_id") or fingerprint(pool, inputs, command) != row["request_fingerprint"]:
        conflict(row, "submission configuration changed since intent was recorded")
        save(root, row)
        return row
    row.update(submission_state="submitting", last_status="submitting", dead=False,
               submit_started_unix=now, submit_attempts=row.get("submit_attempts", 0) + 1)
    row.pop("submission_error", None)
    save(root, row)  # After this write a process exit requires lookup, even before the call.
    try:
        handle = launcher.submit(row["worker_id"], *command)
        confirm(row, handle, now)
    except TransientSubmitError as exc:
        row.update(submission_state="rejected", last_status="terminal:rejected",
                   dead=True, submission_error=str(exc))
    except SubmissionChangedError as exc:
        conflict(row, str(exc))
    except Exception as exc:
        uncertain(row, f"{type(exc).__name__}: {exc}", now)
    save(root, row)  # Record each thread's own result as soon as it returns.
    return row


def refresh(root, pool, launcher, pools_path, package_root, row, now):
    state = row.get("submission_state")
    if state == "conflict" or state in {"terminal", "rejected"} or (
        not state and row.get("dead") and not occupies(row)
    ):
        return row
    if state == "uncertain" and now < row.get("reconcile_after_unix", 0):
        return row
    if state:
        command = worker_command(root, pool, pools_path, package_root, row["worker_id"])
        try:
            current = fingerprint(pool, launcher.submission_inputs(), command)
        except Exception as exc:
            row["status_error"] = f"cannot verify submission configuration: {type(exc).__name__}: {exc}"
            if state in {"uncertain", "submitting"}:
                uncertain(row, row["status_error"], now)
            save(root, row)
            return row
        if row.get("request_id") != row["worker_id"] or current != row.get("request_fingerprint"):
            conflict(row, "submission identity or configuration changed; inspect the original request")
            save(root, row)
            return row
    if state == "intent":
        if row.pop("status_error", None):
            save(root, row)
        return row
    if state in {"submitting", "uncertain"}:
        row.pop("status_error", None)
        try:
            matches = launcher.lookup_submission(row["request_id"])
            if not isinstance(matches, list) or any(not isinstance(h, str) or not h.strip() for h in matches):
                raise ValueError("lookup did not return a complete handle list")
            matches = sorted(set(matches))
            if len(matches) == 1:
                confirm(row, matches[0], now)
            elif matches:
                row["match_handles"] = matches
                conflict(row, "multiple tasks match this request")
            else:
                uncertain(row, "no matching task currently visible", now)
        except Exception as exc:
            uncertain(row, f"{type(exc).__name__}: {exc}", now)
    if row.get("submission_state") not in UNRESOLVED:
        handle = row.get("handle")
        if handle:
            try:
                status = launcher.status(handle)
                row.update(last_status=status, dead=status.startswith("terminal:"))
                if state and row["dead"]:
                    row["submission_state"] = "terminal"
                row.pop("status_error", None)
            except Exception as exc:
                row["status_error"] = f"{type(exc).__name__}: {exc}"
        else:
            conflict(row, "worker record has no queryable handle or recoverable submission")
    row["last_checked_unix"] = now
    save(root, row)
    return row
