from __future__ import annotations

import json
import time
from pathlib import Path
from typing import List

from ..ledger import atomic_write_json
from .base import Backend, Partition, RangeResult, RangeValidationError, session_fingerprint, validate_unit_ids


class _FakeSession:
    def __init__(self, unit_seconds: float, poison_units: dict, session_key: str) -> None:
        self.unit_seconds = unit_seconds
        self.poison_units = poison_units
        self.session_key = session_key

    def run_range(self, partition: str, start: int, count: int, out_dir: Path) -> RangeResult:
        out_dir.mkdir(parents=True, exist_ok=True)
        records_file = out_dir / "records.jsonl"
        poison = {int(value) for value in self.poison_units.get(partition, [])}
        began = time.time()
        with records_file.open("w", encoding="utf-8") as handle:
            for unit_id in range(start, start + count):
                time.sleep(self.unit_seconds)
                if unit_id in poison:
                    raise RuntimeError("poison unit")
                handle.write(json.dumps({"unit_id": unit_id, "score": unit_id % 2}, separators=(",", ":")) + "\n")
                handle.flush()
        return RangeResult(
            count=count,
            elapsed_seconds=time.time() - began,
            records_file=records_file,
            extra={},
        )

    def close(self) -> None:
        return None


class FakeBackend(Backend):
    name = "fake"

    def __init__(self, backend_args: dict, pool: dict, pins: dict) -> None:
        super().__init__(backend_args, pool, pins)
        self.unit_seconds = float(backend_args.get("unit_seconds", 0.05))
        self.poison_units = dict(backend_args.get("poison_units") or {})

    def describe(self) -> List[Partition]:
        partitions = self.backend_args.get("partitions") or {}
        return [Partition(str(name), int(size), self.unit_seconds) for name, size in partitions.items()]

    def session_key(self) -> str:
        return session_fingerprint(self.name, self.unit_seconds, self.poison_units)

    def open_session(self) -> _FakeSession:
        return _FakeSession(self.unit_seconds, self.poison_units, self.session_key())

    def validate_range(self, partition: str, start: int, count: int, result: RangeResult) -> None:
        validate_unit_ids(result.records_file, start, count)
        if result.count != count:
            raise RangeValidationError(f"fake backend returned count={result.count}, expected {count}")

    def merge(self, partition: Partition, records: List[dict], out_dir: Path, meta: dict) -> dict:
        out_dir.mkdir(parents=True, exist_ok=True)
        mean = sum(float(row["score"]) for row in records) / len(records) if records else 0.0
        result = {
            "results": {partition.name: {"mean,none": mean, "alias": partition.name}},
            "n-samples": {
                partition.name: {"original": partition.size, "effective": len(records)}
            },
        }
        atomic_write_json(out_dir / "results.json", result)
        return {"metrics": {"mean,none": mean}, "selfcheck": {"shards_checked": 0, "mismatches": []}}
