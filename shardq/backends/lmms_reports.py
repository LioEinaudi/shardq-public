"""lmms report compatibility and the optional reference-fixture command."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path

from ..spec import PoolSpec, load_pools, resolve_pools_path, resolve_root


def _parse_generation_kwargs(value: object) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    result = {}
    for item in value.split(","):
        if "=" not in item:
            continue
        key, raw = item.split("=", 1)
        raw = raw.strip()
        if raw.lower() in {"true", "false"}:
            parsed = raw.lower() == "true"
        else:
            try:
                parsed = int(raw)
            except ValueError:
                try:
                    parsed = float(raw)
                except ValueError:
                    parsed = raw
        result[key.strip()] = parsed
    return result


class LmmsReports:
    @classmethod
    def report_metadata(cls, job_spec: dict) -> dict:
        args = job_spec.get("backend_args") or {}
        pins = job_spec.get("pins") or {}
        return {
            "marker": {
                "batch_size": args.get("batch_size"),
                "generation_kwargs": _parse_generation_kwargs(args.get("gen_kwargs")),
                "num_processes": 1,
                "canonical_source_proof": pins.get("canonical_source_proof"),
            },
            "run": {"canonical_lmms_root": pins.get("lmms_root")},
            "source_revisions": {"canonical_lmms_tree": pins.get("lmms_commit")},
        }


def _public_scalars(metrics: dict) -> dict:
    return {
        str(key): value
        for key, value in metrics.items()
        if key != "alias"
        and "stderr" not in str(key).lower()
        and not str(key).lower().startswith("submission")
        and isinstance(value, (int, float, str, bool))
    }


def lmms_merge_check(
    samples_path: Path,
    results_path: Path,
    lmms_root: Path,
    local_python: Path,
) -> dict:
    from ..backend_exec import run_backend_op

    root = Path(lmms_root).expanduser().resolve()
    python = Path(local_python).expanduser().absolute()
    if not root.is_dir():
        raise ValueError(f"lmms root is not a directory: {root}")
    if not python.is_file() or not os.access(python, os.X_OK):
        raise ValueError(f"local python is not executable: {python}")
    proc = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode:
        raise RuntimeError(f"cannot resolve lmms commit: {proc.stderr.strip()}")
    expected_payload = json.loads(
        Path(results_path).read_text(encoding="utf-8")
    )
    task_names = list((expected_payload.get("results") or {}).keys())
    if len(task_names) != 1:
        raise ValueError(
            f"expected exactly one task in results file, got {task_names}"
        )
    task = task_names[0]
    rows = []
    for line in Path(samples_path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        row["unit_id"] = int(row["doc_id"])
        rows.append(row)
    counts = (
        expected_payload.get("n-samples")
        or expected_payload.get("n_samples")
        or {}
    )
    original = int((counts.get(task) or {}).get("original", len(rows)))
    pool = PoolSpec(
        name="lmms-merge-check",
        backend="lmms_eval",
        entry="",
        launcher="local",
        volc={},
        env={},
        local_python=str(python),
        max_workers=1,
        worker_max_wall_seconds=300,
        worker_idle_exit_seconds=30,
        extra={"lmms_root": str(root), "column": "check"},
    )
    with tempfile.TemporaryDirectory(
        prefix="shardq-lmms-merge-"
    ) as directory:
        work = Path(directory)
        records_file = work / "records.jsonl"
        records_file.write_text(
            "".join(
                json.dumps(row, ensure_ascii=False) + "\n" for row in rows
            ),
            encoding="utf-8",
        )
        run_backend_op(
            pool,
            "merge",
            {
                "backend": "lmms_eval",
                "backend_args": {
                    "tasks": [task],
                    "model": "",
                    "bootstrap_iters": 0,
                },
                "pool": pool.snapshot(),
                "pins": {
                    "lmms_root": str(root),
                    "lmms_commit": proc.stdout.strip(),
                },
                "partition": {
                    "name": task,
                    "size": original,
                    "traits": {},
                },
                "records_file": str(records_file),
                "out_dir": str(work / "merged"),
                "meta": {"shards_used": []},
            },
        )
        actual_payload = json.loads(
            (work / "merged" / "results.json").read_text(encoding="utf-8")
        )
    expected = _public_scalars(expected_payload["results"][task])
    actual = _public_scalars(actual_payload["results"][task])
    diffs = {}
    for key in sorted(set(expected) | set(actual)):
        left = expected.get(key)
        right = actual.get(key)
        if (
            isinstance(left, (int, float))
            and not isinstance(left, bool)
            and isinstance(right, (int, float))
        ):
            if abs(float(left) - float(right)) < 1e-9:
                continue
            delta = float(right) - float(left)
        else:
            if left == right:
                continue
            delta = None
        diffs[key] = {
            "expected": left,
            "actual": right,
            "delta": delta,
        }
    return {"task": task, "match": not diffs, "diffs": diffs}


def check_from_cli(args) -> dict:
    config_root = (
        resolve_root(args.root)
        if args.root or os.environ.get("SHARDQ_ROOT")
        else Path.cwd()
    )
    lmms_root = args.lmms_root
    local_python = args.local_python
    if args.pool:
        pools_path = resolve_pools_path(args.pools, config_root)
        pools = load_pools(pools_path)
        if args.pool not in pools:
            raise ValueError(f"unknown pool: {args.pool}")
        pool = pools[args.pool]
        lmms_root = lmms_root or pool.extra.get("lmms_root")
        local_python = local_python or pool.local_python
    if lmms_root is None:
        raise ValueError(
            "lmms root is not set; pass --lmms-root or --pool"
        )
    if local_python is None:
        raise ValueError(
            "local python is not set; pass --local-python or --pool"
        )
    report = lmms_merge_check(
        args.samples,
        args.results,
        Path(lmms_root),
        Path(local_python),
    )
    return report
