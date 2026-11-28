from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict

from .backends import get_backend
from .backends.base import Partition
from .spec import PoolSpec


def execute_backend_op(op: str, payload: dict) -> dict:
    backend_class = get_backend(str(payload["backend"]))
    backend = backend_class(
        dict(payload.get("backend_args") or {}),
        dict(payload.get("pool") or {}),
        dict(payload.get("pins") or {}),
    )
    if op == "session_key":
        return {"session_key": backend.session_key()}
    if op == "describe":
        partitions = backend.preview_describe() if payload.get("dry_run") else backend.describe()
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
        return backend.merge_file(
            partition,
            Path(payload["records_file"]),
            Path(payload["out_dir"]),
            dict(payload.get("meta") or {}),
        )
    raise ValueError(f"unsupported backend operation: {op}")


def run_backend_op(pool: PoolSpec, op: str, payload: dict) -> dict:
    envelope: Dict[str, Any] = dict(payload)
    envelope.setdefault("backend", pool.backend)
    envelope.setdefault("backend_args", {})
    envelope.setdefault("pool", pool.snapshot())
    envelope.setdefault("pins", {})
    python = pool.local_python or sys.executable
    backend = get_backend(envelope["backend"])
    import_roots = backend.control_pythonpath(pool, envelope["pins"])
    if import_roots is not None and not isinstance(import_roots, (tuple, list)):
        raise TypeError(f"{envelope['backend']} control_pythonpath must return import roots or None")
    # A virtual environment can symlink its executable to the control interpreter.
    if import_roots is None and os.path.abspath(python) == os.path.abspath(sys.executable):
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
        paths = [str(package_root), *(str(path) for path in import_roots or ())]
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
    if len(arguments) != 2 or arguments[0] not in {"describe", "session_key", "merge"}:
        print("usage: python -m shardq.backend_exec <describe|session_key|merge> <payload.json>", file=sys.stderr)
        return 2
    payload = json.loads(Path(arguments[1]).read_text(encoding="utf-8"))
    result = execute_backend_op(arguments[0], payload)
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
