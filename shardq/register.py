from __future__ import annotations

import hashlib
import subprocess
import time
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Optional

from . import __version__
from .backend_exec import run_backend_op
from .backends import get_backend
from .cutter import estimate_unit_seconds
from .ledger import atomic_write_json, read_json
from .spec import (
    JOB_ID_RE,
    PoolSpec,
    load_job_spec,
    load_pools,
    resolve_pools_path,
)


def _launcher_block(pool: PoolSpec) -> dict:
    if pool.launcher == "volc":
        return pool.volc
    if pool.launcher == "aihc":
        return pool.aihc
    return {}


def _git(root: Path, *arguments: str, optional: bool = False) -> Optional[str]:
    proc = subprocess.run(
        ["git", "-C", str(root), *arguments],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode:
        if optional:
            return None
        raise RuntimeError(
            f"git {' '.join(arguments)} failed ({proc.returncode}): {proc.stderr[-4000:]}"
        )
    return proc.stdout.strip()


def register(
    root: Path,
    spec_path: Path,
    pin_mode: str = "worktree",
    sizes: Optional[Dict[str, int]] = None,
    dry_run: bool = False,
    pools: Optional[Dict[str, PoolSpec]] = None,
    pools_path: Optional[Path] = None,
) -> dict:
    root = Path(root).resolve()
    spec_path = Path(spec_path)
    spec = load_job_spec(spec_path)
    if pools is None:
        resolved_pools = (
            Path(pools_path).resolve()
            if pools_path is not None
            else resolve_pools_path(None, root)
        )
        pools = load_pools(resolved_pools)
    if spec.pool not in pools:
        raise ValueError(f"unknown pool: {spec.pool}")
    pool = pools[spec.pool]
    if spec.backend != pool.backend:
        raise ValueError(
            f"job backend {spec.backend!r} does not match pool backend {pool.backend!r}"
        )
    job_dir = root / "jobs" / spec.job_id
    if job_dir.exists():
        raise ValueError(f"job already exists: {spec.job_id}")

    backend_class = get_backend(spec.backend)
    pin_hook = backend_class.preview_pins if dry_run else backend_class.pins
    launcher_block = _launcher_block(pool)
    pins = {
        "image": launcher_block.get("image"),
        "flavor": launcher_block.get(
            "flavor", launcher_block.get("gpu_resource")
        ),
    }
    pins.update(pin_hook(pool, root, spec.job_id, pin_mode))
    control_root = Path.cwd().resolve()
    pins["control_root"] = str(control_root)
    pins["control_commit"] = _git(
        control_root, "rev-parse", "HEAD", optional=True
    )
    pins["shardq_version"] = __version__

    operation_pins = dict(pins)
    if (
        dry_run
        and pin_mode == "worktree"
        and operation_pins.get("lmms_source_root")
    ):
        operation_pins["lmms_root"] = operation_pins["lmms_source_root"]
    backend_payload = {
        "backend": spec.backend,
        "backend_args": spec.backend_args,
        "pool": asdict(pool),
        "pins": pins,
    }
    if sizes is None:
        described = run_backend_op(pool, "describe", {**backend_payload, "pins": operation_pins})
        partition_rows = described["partitions"]
    else:
        partition_rows = [
            {
                "name": str(name),
                "size": int(size),
                "unit_seconds_hint": None,
                "traits": {},
            }
            for name, size in sizes.items()
        ]

    if sizes is None and operation_pins == pins:
        session_key = str(described["session_key"])
    else:
        session_key = str(run_backend_op(pool, "session_key", backend_payload)["session_key"])

    names = [str(row["name"]) for row in partition_rows]
    if not names or len(names) != len(set(names)):
        raise ValueError("backend must describe at least one uniquely named partition")
    for row in partition_rows:
        if not JOB_ID_RE.fullmatch(str(row["name"])):
            raise ValueError(
                f"partition name is not filesystem safe: {row['name']!r}"
            )
        size = int(row["size"])
        if size <= 0:
            raise ValueError(f"partition size must be positive: {row}")

    column = str(pool.extra.get("column", pool.name))
    job_payload = asdict(spec)
    job_payload.update(
        {
            "schema_version": 1,
            "column": column,
            "pins": pins,
            "pool_snapshot": asdict(pool),
            "session_key": session_key,
            "registered_unix": time.time(),
            "spec_file": str(spec_path.resolve()),
            "spec_sha256": hashlib.sha256(spec_path.read_bytes()).hexdigest(),
        }
    )
    if dry_run:
        return job_payload

    job_dir.mkdir(parents=True, exist_ok=True)
    history = read_json(root / "history.json", {})
    limit = spec.shard.get("limit_units")
    for row in partition_rows:
        name = str(row["name"])
        described_size = int(row["size"])
        size = (
            min(described_size, int(limit))
            if limit is not None
            else described_size
        )
        key = f"{spec.backend}:{session_key}:{name}"
        estimate = estimate_unit_seconds(
            history, key, row.get("unit_seconds_hint")
        )
        partition_traits = dict(row.get("traits") or {})
        partition_traits.update(spec.traits.get(name, {}))
        if partition_traits:
            job_payload["traits"][name] = partition_traits
        directory = job_dir / "partitions" / name
        directory.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            directory / "partition.json",
            {
                "name": name,
                "size": size,
                "source_size": described_size,
                "next_cut": 0,
                "unit_seconds_est": estimate,
                "status": "cutting",
            },
        )
    atomic_write_json(job_dir / "job.json", job_payload)
    return job_payload
