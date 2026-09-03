from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

from .backends import get_backend
from .backends.base import Partition
from .spec import PoolSpec


def _load_records(path: Path) -> list:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def execute_backend_op(op: str, payload: dict) -> dict:
    backend_class = get_backend(str(payload["backend"]))
    backend = backend_class(
        dict(payload.get("backend_args") or {}),
        dict(payload.get("pool") or {}),
        dict(payload.get("pins") or {}),
    )
    if op == "describe":
        partitions = backend.describe()
        return {
            "partitions": [
                {
                    "name": item.name,
                    "size": item.size,
                    "unit_seconds_hint": item.unit_seconds_hint,
                    "traits": item.traits,
                }
                for item in partitions
            ],
            "session_key": backend.session_key(),
        }
    if op == "merge":
        row = payload["partition"]
        partition = Partition(
            name=str(row["name"]),
            size=int(row["size"]),
            unit_seconds_hint=row.get("unit_seconds_hint"),
            traits=dict(row.get("traits") or {}),
        )
        records = _load_records(Path(payload["records_file"]))
        return backend.merge(
            partition,
            records,
            Path(payload["out_dir"]),
            dict(payload.get("meta") or {}),
        )
    raise ValueError(f"unsupported backend operation: {op}")


def run_backend_op(pool: PoolSpec, op: str, payload: dict) -> dict:
    envelope: Dict[str, Any] = dict(payload)
    envelope.setdefault("backend", pool.backend)
    envelope.setdefault("backend_args", {})
    envelope.setdefault("pool", asdict(pool))
    envelope.setdefault("pins", {})
    python = pool.local_python
    if not python or Path(python).resolve() == Path(sys.executable).resolve():
        return execute_backend_op(op, envelope)
    temporary = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".json", prefix="shardq-backend-", delete=False
    )
    try:
        json.dump(envelope, temporary, ensure_ascii=False)
        temporary.write("\n")
        temporary.close()
        env = dict(os.environ)
        package_root = Path(__file__).resolve().parent.parent
        paths = [str(package_root)]
        lmms_root = (envelope.get("pins") or {}).get("lmms_root")
        if lmms_root:
            paths.append(str(lmms_root))
        if env.get("PYTHONPATH"):
            paths.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = os.pathsep.join(paths)
        proc = subprocess.run(
            [python, "-m", "shardq.backend_exec", op, temporary.name],
            cwd=str(package_root),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if proc.returncode:
            raise RuntimeError(
                f"backend {op} subprocess failed ({proc.returncode}): "
                f"{(proc.stderr + proc.stdout)[-6000:]}"
            )
        for line in reversed(proc.stdout.splitlines()):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                return value
        raise RuntimeError(f"backend {op} subprocess produced no JSON object: {proc.stdout[-2000:]}")
    finally:
        try:
            os.unlink(temporary.name)
        except FileNotFoundError:
            pass


def main(argv: list = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 2 or arguments[0] not in {"describe", "merge"}:
        print("usage: python -m shardq.backend_exec <describe|merge> <payload.json>", file=sys.stderr)
        return 2
    payload = json.loads(Path(arguments[1]).read_text(encoding="utf-8"))
    result = execute_backend_op(arguments[0], payload)
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
