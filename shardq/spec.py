from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


JOB_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
DEFAULT_SHARD = {
    "target_seconds": 1800,
    "min_units": 4,
    "max_units": 512,
    "limit_units": None,
    "lookahead": 4,
}
DEFAULT_RETRY = {
    "max_errors": 3,
    "max_preemptions": 20,
    "backoff_seconds": 300,
    "bisect": True,
    "lease_ttl_seconds": 900,
}


@dataclass
class PoolSpec:
    name: str
    backend: str
    entry: str
    launcher: str
    volc: dict
    env: dict
    local_python: Optional[str]
    max_workers: int
    worker_max_wall_seconds: int
    worker_idle_exit_seconds: int
    extra: dict
    aihc: dict = field(default_factory=dict)


@dataclass
class JobSpec:
    job_id: str
    backend: str
    pool: str
    protocol_id: str
    priority: int
    backend_args: dict
    shard: dict
    retry: dict
    traits: dict


def _read_json(path: Path) -> dict:
    with Path(path).open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return payload


def resolve_root(cli_root: Optional[str]) -> Path:
    value = cli_root or os.environ.get("SHARDQ_ROOT")
    if not value:
        raise ValueError("shardq root is not set; pass --root or set SHARDQ_ROOT")
    return Path(value).expanduser().resolve()


def resolve_pools_path(cli_pools: Optional[str], root: Path) -> Path:
    value = cli_pools or os.environ.get("SHARDQ_POOLS")
    if value:
        path = Path(value).expanduser().resolve()
        if not path.is_file():
            raise ValueError(f"pools file does not exist: {path}")
        return path
    fallback = Path(root).resolve() / "pools.json"
    if fallback.is_file():
        return fallback
    raise ValueError(
        "shardq pools are not set; pass --pools, set SHARDQ_POOLS, "
        f"or create {fallback}"
    )


def pool_from_dict(name: str, row: dict) -> PoolSpec:
    if not isinstance(row, dict):
        raise ValueError(f"pool {name!r} must be an object")
    launcher = str(row.get("launcher", ""))
    if launcher not in {"local", "volc", "aihc"}:
        raise ValueError(f"pool {name!r}: invalid launcher {launcher!r}")
    extra = dict(row.get("extra") or {})
    # Old ledger snapshots stored these two backend-specific fields at top level.
    if "column" in row and "column" not in extra:
        extra["column"] = row["column"]
    if row.get("lmms_root") and "lmms_root" not in extra:
        extra["lmms_root"] = row["lmms_root"]
    spec = PoolSpec(
        name=str(name),
        backend=str(row.get("backend", "")),
        entry=str(row.get("entry", "")),
        launcher=launcher,
        volc=dict(row.get("volc") or {}),
        env={str(key): str(value) for key, value in (row.get("env") or {}).items()},
        local_python=str(row["local_python"]) if row.get("local_python") else None,
        max_workers=int(row.get("max_workers", 1)),
        worker_max_wall_seconds=int(row.get("worker_max_wall_seconds", 14400)),
        worker_idle_exit_seconds=int(row.get("worker_idle_exit_seconds", 300)),
        extra=extra,
        aihc=dict(row.get("aihc") or {}),
    )
    if not spec.backend:
        raise ValueError(f"pool {name!r}: backend is required")
    if min(
        spec.max_workers,
        spec.worker_max_wall_seconds,
        spec.worker_idle_exit_seconds,
    ) <= 0:
        raise ValueError(f"pool {name!r}: worker limits must be positive")
    return spec


def load_pools(path: Path) -> Dict[str, PoolSpec]:
    path = Path(path)
    payload = _read_json(path)
    rows = payload.get("pools")
    if not isinstance(rows, dict):
        raise ValueError(f"pools must be an object: {path}")
    return {str(name): pool_from_dict(str(name), row) for name, row in rows.items()}


def _positive_int(mapping: dict, key: str) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return value


def load_job_spec(path: Path) -> JobSpec:
    payload = _read_json(path)
    job_id = str(payload.get("job_id", ""))
    if not JOB_ID_RE.fullmatch(job_id):
        raise ValueError("job_id may contain only A-Z, a-z, 0-9, '.', '_' and '-'")
    for key in ("backend", "pool", "protocol_id"):
        if not isinstance(payload.get(key), str) or not payload[key].strip():
            raise ValueError(f"{key} must be a non-empty string")
    backend_args = payload.get("backend_args", {})
    if not isinstance(backend_args, dict):
        raise ValueError("backend_args must be an object")
    shard = dict(DEFAULT_SHARD)
    shard.update(payload.get("shard") or {})
    retry = dict(DEFAULT_RETRY)
    retry.update(payload.get("retry") or {})
    traits = payload.get("traits") or {}
    if not isinstance(traits, dict):
        raise ValueError("traits must be an object")
    for key in ("target_seconds", "min_units", "max_units", "lookahead"):
        _positive_int(shard, key)
    if shard["min_units"] > shard["max_units"]:
        raise ValueError("shard.min_units may not exceed shard.max_units")
    limit = shard.get("limit_units")
    if limit is not None and (
        isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
    ):
        raise ValueError("shard.limit_units must be null or a positive integer")
    for key in ("max_errors", "max_preemptions", "lease_ttl_seconds"):
        _positive_int(retry, key)
    if (
        isinstance(retry.get("backoff_seconds"), bool)
        or not isinstance(retry.get("backoff_seconds"), int)
        or retry["backoff_seconds"] < 0
    ):
        raise ValueError("retry.backoff_seconds must be a non-negative integer")
    if not isinstance(retry.get("bisect"), bool):
        raise ValueError("retry.bisect must be boolean")
    for partition, partition_traits in traits.items():
        if not isinstance(partition_traits, dict):
            raise ValueError(f"traits[{partition!r}] must be an object")
        if "max_parallel" in partition_traits:
            _positive_int(partition_traits, "max_parallel")
    priority = payload.get("priority", 0)
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise ValueError("priority must be an integer")
    return JobSpec(
        job_id=job_id,
        backend=str(payload["backend"]),
        pool=str(payload["pool"]),
        protocol_id=str(payload["protocol_id"]),
        priority=priority,
        backend_args=dict(backend_args),
        shard=shard,
        retry=retry,
        traits={str(key): dict(value) for key, value in traits.items()},
    )


def slug(value: str) -> str:
    return "".join(
        character if character.isalnum() else "-" for character in value
    ).strip("-")
