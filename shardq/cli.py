from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .backends.reports import report_for_job
from .controller import run_scheduler
from .fs import atomic_output, atomic_write_text
from .launchers import get_launcher
from .ledger import Ledger, atomic_write_json, read_json
from .merge import merge_partition
from .register import register
from .spec import (
    JOB_ID_RE,
    load_pools,
    pool_from_dict,
    resolve_pools_path,
    resolve_root,
)
from .status import render_status
from .submission_admin import resolve_submission
from .worker import _pool_from_ledger, default_worker_id, run_worker


def _root(value: Optional[str]) -> Path:
    return resolve_root(value)


def _atomic_copy(source: Path, target: Path) -> None:
    with atomic_output(target) as temporary:
        shutil.copy2(source, temporary)


def _parse_sizes(values: Optional[List[str]]) -> Optional[Dict[str, int]]:
    if not values:
        return None
    result = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"invalid size {value!r}; expected name=N")
        name, count = value.split("=", 1)
        if not JOB_ID_RE.fullmatch(name) or int(count) <= 0:
            raise ValueError(f"invalid size {value!r}")
        result[name] = int(count)
    return result


def _claim_once(arguments) -> int:
    path, ready = arguments
    ready.wait()
    try:
        os.mkdir(path)
        return 1
    except FileExistsError:
        return 0


def selftest_fs(root: Path, procs: int, rounds: int) -> dict:
    if procs <= 1 or rounds <= 0:
        raise ValueError("selftest-fs requires procs > 1 and rounds > 0")
    run_dir = (
        Path(root).resolve() / f"run-{os.getpid()}-{int(time.time())}"
    )
    run_dir.mkdir(parents=True, exist_ok=False)
    single = 0
    conflicts = 0
    manager = multiprocessing.Manager()
    try:
        with multiprocessing.Pool(procs) as pool:
            for index in range(rounds):
                ready = manager.Event()
                target = str(run_dir / f"round-{index:06d}")
                results = [
                    pool.apply_async(_claim_once, ((target, ready),))
                    for _ in range(procs)
                ]
                ready.set()
                winners = sum(result.get(timeout=30) for result in results)
                if winners == 1:
                    single += 1
                else:
                    conflicts += 1
    finally:
        manager.shutdown()
    return {
        "rounds": rounds,
        "single_winner_rounds": single,
        "conflicts": conflicts,
    }


def lmms_merge_check(samples_path: Path, results_path: Path, lmms_root: Path, local_python: Path) -> dict:
    """Compatibility entry point for the optional lmms fixture checker."""
    from .backends.lmms_reports import lmms_merge_check as check

    return check(samples_path, results_path, lmms_root, local_python)


def _publish(root: Path, job_id: str, out_dir: Path) -> dict:
    ledger = Ledger(root)
    job = ledger.scan_job(job_id, time.time())
    if not job.partitions or any(
        row.status not in {"merged", "partial"}
        for row in job.partitions.values()
    ):
        raise ValueError(
            "all job partitions must be merged or partial before publish"
        )
    run_dir = Path(out_dir).expanduser().resolve()
    if run_dir.exists():
        raise ValueError(f"output directory already exists: {run_dir}")
    target_markers = run_dir / "state" / "full" / job.column
    target_markers.mkdir(parents=True)
    marker_source = ledger.job_dir(job_id) / "markers" / job.column
    for marker in sorted(marker_source.glob("*.json")):
        _atomic_copy(marker, target_markers / marker.name)

    artifacts = []
    missing_ids = {}
    for name in sorted(job.partitions):
        merged = ledger.job_dir(job_id) / "merged" / name
        missing_ids[name] = read_json(merged / "merge.json")["missing_units"]
        row = {"partition": name}
        for kind in ("results", "records"):
            path = (
                merged / "results.json"
                if kind == "results"
                else merged / "records.jsonl"
            )
            row[kind] = {
                "path": str(path.resolve()),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        artifacts.append(row)

    pins = job.spec.get("pins") or {}
    source = pins.get("execution_source")
    if source is None:
        snapshot = job.spec.get("pool_snapshot")
        source = {}
        if snapshot:
            pool = pool_from_dict(job.pool, snapshot)
            source = get_launcher(pool.launcher).execution_source(pool)
    status = (
        "partial"
        if any(
            row.status == "partial" for row in job.partitions.values()
        )
        else "complete"
    )
    run_id = run_dir.name
    report = report_for_job(job.spec)
    run_payload = {
        **report.get("run", {}),
        "schema_version": 2 if "backend_report" in job.spec else 1,
        "kind": "shardq",
        "run_id": run_id,
        "status": status,
        "created_at": datetime.now().astimezone().isoformat(
            timespec="seconds"
        ),
        "columns": [job.column],
        "protocol_by_column": {job.column: job.spec["protocol_id"]},
        "queue": source.get("queue"),
        "execution_source": source,
        "source_revisions": {
            **report.get("source_revisions", {}),
            "control_commit": pins.get("control_commit"),
            "shardq_version": pins.get("shardq_version"),
        },
        "shardq_job": job_id,
        "shardq_root": str(Path(root).resolve()),
        "artifacts": "artifacts.json",
        "acceptance": "acceptance.json",
        "summary": "summary.md",
    }
    atomic_write_json(run_dir / "run.json", run_payload)
    atomic_write_json(
        run_dir / "artifacts.json",
        {"schema_version": 1, "artifacts": artifacts},
    )
    atomic_write_json(
        run_dir / "acceptance.json",
        {
            "schema_version": 1,
            "status": (
                "accepted" if status == "complete" else "data_gap"
            ),
            "missing_units": {name: len(ids) for name, ids in missing_ids.items()},
            "missing_unit_ids": missing_ids,
        },
    )
    atomic_write_text(
        run_dir / "summary.md",
        f"# {run_id}\n\nShardQ job `{job_id}` published "
        f"with status `{status}`.\n",
    )
    return run_payload


def _add_config_args(
    parser: argparse.ArgumentParser, root_required: bool = False
) -> None:
    parser.add_argument("--root", required=root_required)
    parser.add_argument("--pools")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="shardq")
    commands = parser.add_subparsers(dest="command", required=True)

    register_parser = commands.add_parser("register", help="register a batch job and its partitions")
    register_parser.add_argument("spec", type=Path)
    _add_config_args(register_parser)
    registration = register_parser.add_argument_group("source pinning and partition overrides")
    registration.add_argument(
        "--pin-mode",
        choices=["worktree", "verify"],
        default="worktree",
    )
    registration.add_argument("--sizes", nargs="*")
    register_parser.add_argument("--dry-run", action="store_true")

    scheduler = commands.add_parser("scheduler", help="execute registered jobs using their pools")
    _add_config_args(scheduler)
    scheduler.add_argument(
        "--launcher", metavar="NAME",
        help="only manage pools with this launcher (alias or module:Class); default: all pools"
    )
    scheduler.add_argument("--poll-seconds", type=int, default=60)
    scheduler.add_argument("--once", action="store_true")
    scheduler.add_argument("--exit-when-idle", action="store_true")
    tuning = scheduler.add_argument_group("advanced scheduling limits")
    tuning.add_argument("--max-submits-per-tick", type=int, default=4)
    tuning.add_argument("--max-cuts-per-tick", type=int, default=64)
    scheduler.add_argument("--require-complete", action="store_true")

    worker = commands.add_parser("worker")
    worker.add_argument("--pool", required=True)
    _add_config_args(worker)
    worker.add_argument("--max-wall-seconds", type=int)
    worker.add_argument("--idle-exit-seconds", type=int)
    worker.add_argument("--worker-id")
    worker.add_argument("--job", action="append")

    status = commands.add_parser("status")
    _add_config_args(status)
    status.add_argument("--job")
    status.add_argument("--json", action="store_true")

    resolve = commands.add_parser("resolve-submission", help="record a verified provider outcome with the scheduler stopped")
    _add_config_args(resolve)
    resolve.add_argument("--worker-id", required=True)
    outcome = resolve.add_mutually_exclusive_group(required=True)
    outcome.add_argument("--handle", help="bind a verified handle after verifying all other matches are inactive")
    outcome.add_argument("--not-created", action="store_true", help="record provider evidence that no task was created")
    outcome.add_argument("--terminal", action="store_true", help="record that all possible matching tasks are terminal")
    resolve.add_argument("--terminal-match", action="append", default=[], metavar="HANDLE",
                         help="with --handle, list each other match verified terminal (repeatable)")
    resolve.add_argument("--evidence", required=True, help="provider evidence supporting this decision")

    merge = commands.add_parser("merge")
    merge.add_argument("--job", required=True)
    _add_config_args(merge)
    merge.add_argument("--partition")
    merge.add_argument("--allow-partial", action="store_true")

    split = commands.add_parser("split")
    split.add_argument("--job", required=True)
    split.add_argument("--shard", required=True)
    _add_config_args(split)

    for name in ("stop", "resume"):
        command = commands.add_parser(name)
        command.add_argument("--job", required=True)
        _add_config_args(command)

    publish = commands.add_parser("publish")
    publish.add_argument("--job", required=True)
    publish.add_argument("--out-dir", type=Path, required=True)
    _add_config_args(publish)

    fs_test = commands.add_parser("selftest-fs")
    _add_config_args(fs_test, root_required=True)
    fs_test.add_argument("--procs", type=int, default=16)
    fs_test.add_argument("--rounds", type=int, default=200)

    check = commands.add_parser("lmms-merge-check")
    _add_config_args(check)
    check.add_argument("--samples", type=Path, required=True)
    check.add_argument("--results", type=Path, required=True)
    check.add_argument("--lmms-root", type=Path)
    check.add_argument("--local-python", type=Path)
    check.add_argument("--pool")
    return parser


def _optional_pools_path(args, root: Path) -> Optional[Path]:
    configured = (
        args.pools is not None
        or bool(os.environ.get("SHARDQ_POOLS"))
        or (Path(root).resolve() / "pools.json").is_file()
    )
    return resolve_pools_path(args.pools, root) if configured else None


def main(argv: List[str] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "register":
            root = _root(args.root)
            pools_path = resolve_pools_path(args.pools, root)
            pools = load_pools(pools_path)
            payload = register(
                root,
                args.spec,
                args.pin_mode,
                _parse_sizes(args.sizes),
                args.dry_run,
                pools=pools,
                pools_path=pools_path,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        if args.command == "scheduler":
            if args.launcher is not None:
                get_launcher(args.launcher)
            if (
                args.poll_seconds <= 0
                or args.max_submits_per_tick <= 0
                or args.max_cuts_per_tick <= 0
            ):
                raise ValueError(
                    "poll-seconds, max-submits-per-tick and max-cuts-per-tick must be positive"
                )
            root = _root(args.root)
            pools_path = resolve_pools_path(args.pools, root)
            return run_scheduler(
                root,
                args.launcher,
                args.poll_seconds,
                args.once,
                args.exit_when_idle,
                {
                    "max_submits_per_tick": args.max_submits_per_tick,
                    "max_cuts_per_tick": args.max_cuts_per_tick,
                    "require_complete": args.require_complete,
                    "pools": load_pools(pools_path),
                    "pools_path": pools_path,
                },
            )

        if args.command == "worker":
            root = _root(args.root)
            pools_path = _optional_pools_path(args, root)
            pool = _pool_from_ledger(root, args.pool, pools_path)
            worker_id = args.worker_id or os.environ.get("SHARDQ_REQUEST_ID") or default_worker_id(args.pool)
            return run_worker(
                root,
                args.pool,
                worker_id,
                args.max_wall_seconds or pool.worker_max_wall_seconds,
                args.idle_exit_seconds or pool.worker_idle_exit_seconds,
                job_ids=args.job,
                pools_path=pools_path,
            )

        if args.command == "status":
            print(render_status(_root(args.root), args.job, args.json), end="")
            return 0

        if args.command == "resolve-submission":
            action = "bind" if args.handle is not None else ("not-created" if args.not_created else "terminal")
            row = resolve_submission(_root(args.root), args.worker_id, action, args.evidence,
                                     args.handle, args.terminal_match)
            print(json.dumps(row, ensure_ascii=False))
            return 0

        if args.command == "merge":
            root = _root(args.root)
            pools_path = resolve_pools_path(args.pools, root)
            ledger = Ledger(root)
            job = ledger.scan_job(args.job, time.time())
            pools = load_pools(pools_path)
            if job.pool not in pools:
                raise ValueError(f"unknown pool: {job.pool}")
            pool = pools[job.pool]
            partitions = (
                [args.partition]
                if args.partition
                else sorted(job.partitions)
            )
            reports = [
                merge_partition(
                    root,
                    job,
                    name,
                    pool,
                    args.allow_partial,
                    time.time(),
                )
                for name in partitions
            ]
            print(json.dumps(reports, ensure_ascii=False, indent=2))
            return 0

        if args.command == "split":
            children = Ledger(_root(args.root)).split(
                args.job, args.shard, time.time()
            )
            print(json.dumps({"parent": args.shard, "children": children}))
            return 0

        if args.command in {"stop", "resume"}:
            ledger = Ledger(_root(args.root))
            if args.command == "stop":
                ledger.stop(args.job)
            else:
                ledger.resume(args.job)
            print(
                json.dumps(
                    {
                        "job_id": args.job,
                        "stopped": args.command == "stop",
                    }
                )
            )
            return 0

        if args.command == "publish":
            report = _publish(_root(args.root), args.job, args.out_dir)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0

        if args.command == "selftest-fs":
            print(
                json.dumps(
                    selftest_fs(
                        Path(args.root), args.procs, args.rounds
                    ),
                    sort_keys=True,
                )
            )
            return 0

        if args.command == "lmms-merge-check":
            from .backends.lmms_reports import check_from_cli

            report = check_from_cli(args)
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
            return 0 if report["match"] else 1

        raise ValueError(f"unknown command: {args.command}")
    except (KeyError, ValueError, RuntimeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
