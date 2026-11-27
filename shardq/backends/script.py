from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import List

from ..ledger import atomic_write_json
from .base import Backend, Partition, RangeResult, session_fingerprint, validate_unit_ids


def _run(command: str, env: dict, label: str) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        command,
        shell=True,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode:
        raise RuntimeError(
            f"{label} failed ({proc.returncode}): {(proc.stderr or proc.stdout)[-4000:]}"
        )
    return proc


class _ScriptSession:
    def __init__(self, command: str, key: str) -> None:
        self.command = command
        self.session_key = key

    def run_range(self, partition: str, start: int, count: int, out_dir: Path) -> RangeResult:
        out_dir.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ)
        env.update(
            SHARDQ_PARTITION=partition,
            SHARDQ_START=str(start),
            SHARDQ_COUNT=str(count),
            SHARDQ_OUT_DIR=str(out_dir.resolve()),
        )
        began = time.time()
        _run(self.command, env, "script run_command")
        records_file = out_dir / "records.jsonl"
        if not records_file.is_file():
            raise RuntimeError(f"run_command did not create {records_file}")
        return RangeResult(count=count, elapsed_seconds=time.time() - began, records_file=records_file)

    def close(self) -> None:
        return None


class ScriptBackend(Backend):
    name = "script"

    def describe(self) -> List[Partition]:
        configured = self.backend_args.get("partitions")
        if configured:
            return [Partition(str(name), int(size)) for name, size in configured.items()]
        command = self.backend_args.get("describe_command")
        if not command:
            raise ValueError("script backend requires partitions or describe_command")
        proc = _run(str(command), dict(os.environ), "script describe_command")
        payload = json.loads(proc.stdout)
        if not isinstance(payload, list):
            raise ValueError("describe_command stdout must be a JSON array")
        return [
            Partition(
                str(row["name"]),
                int(row["size"]),
                float(row["unit_seconds_hint"]) if row.get("unit_seconds_hint") is not None else None,
                dict(row.get("traits") or {}),
            )
            for row in payload
        ]

    def session_key(self) -> str:
        return session_fingerprint(
            self.name,
            str(self.backend_args.get("run_command", "")),
            str(self.backend_args.get("session_key", "any")),
        )

    def open_session(self) -> _ScriptSession:
        command = self.backend_args.get("run_command")
        if not command:
            raise ValueError("script backend requires run_command")
        return _ScriptSession(str(command), self.session_key())

    def validate_range(self, partition: str, start: int, count: int, result: RangeResult) -> None:
        validate_unit_ids(result.records_file, start, count)

    def merge(self, partition: Partition, records: List[dict], out_dir: Path, meta: dict) -> dict:
        out_dir.mkdir(parents=True, exist_ok=True)
        records_path = out_dir / "records.jsonl"
        records_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in records),
            encoding="utf-8",
        )
        meta_path = out_dir / "meta.json"
        atomic_write_json(meta_path, meta)
        command = self.backend_args.get("merge_command")
        if not command:
            raise ValueError("script backend requires merge_command")
        env = dict(os.environ)
        env.update(
            SHARDQ_PARTITION=partition.name,
            SHARDQ_OUT_DIR=str(out_dir.resolve()),
            SHARDQ_RECORDS=str(records_path.resolve()),
            SHARDQ_META=str(meta_path.resolve()),
        )
        _run(str(command), env, "script merge_command")
        results_path = out_dir / "results.json"
        if not results_path.is_file():
            raise RuntimeError(f"merge_command did not create {results_path}")
        result = json.loads(results_path.read_text(encoding="utf-8"))
        return {"metrics": (result.get("results") or {}).get(partition.name, {})}
