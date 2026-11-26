from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Protocol


class RangeValidationError(RuntimeError):
    pass


class WorkerRestartRequired(RuntimeError):
    """An earlier session loaded process-global state incompatible with this job."""


def session_fingerprint(*settings: object) -> str:
    payload = json.dumps(settings, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Partition:
    name: str
    size: int
    unit_seconds_hint: Optional[float] = None
    traits: dict = field(default_factory=dict)


@dataclass
class RangeResult:
    count: int
    elapsed_seconds: float
    records_file: Path
    extra: dict = field(default_factory=dict)


class Session(Protocol):
    session_key: str

    def run_range(self, partition: str, start: int, count: int, out_dir: Path) -> RangeResult:
        ...

    def close(self) -> None:
        ...


class Backend(ABC):
    name = ""

    def __init__(self, backend_args: dict, pool: dict, pins: dict) -> None:
        self.backend_args = backend_args
        self.pool = pool
        self.pins = pins

    @classmethod
    def pins(
        cls, pool: "PoolSpec", root: Path, job_id: str, pin_mode: str
    ) -> dict:
        del cls, pool, root, job_id, pin_mode
        return {}

    @classmethod
    def preview_pins(
        cls, pool: "PoolSpec", root: Path, job_id: str, pin_mode: str
    ) -> dict:
        """Return dry-run pins without creating backend resources.

        Third-party backends only need to override this when pins() has side
        effects. The public pin hook remains pins(pool, root, job_id, pin_mode).
        """
        return cls.pins(pool, root, job_id, pin_mode)

    @abstractmethod
    def describe(self) -> List[Partition]:
        raise NotImplementedError

    @abstractmethod
    def session_key(self) -> str:
        """Identify the effective settings captured by open_session()."""
        raise NotImplementedError

    @abstractmethod
    def open_session(self) -> Session:
        raise NotImplementedError

    @abstractmethod
    def validate_range(self, partition: str, start: int, count: int, result: RangeResult) -> None:
        raise NotImplementedError

    @abstractmethod
    def merge(self, partition: Partition, records: List[dict], out_dir: Path, meta: dict) -> dict:
        raise NotImplementedError

    def traits(self, partition: str) -> dict:
        return {}


def validate_unit_ids(records_file: Path, start: int, count: int) -> None:
    values = []
    with records_file.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RangeValidationError(f"invalid JSON at {records_file}:{line_number}: {exc}") from exc
            value = row.get("unit_id")
            if isinstance(value, bool) or not isinstance(value, int):
                raise RangeValidationError(f"invalid unit_id at {records_file}:{line_number}: {value!r}")
            values.append(value)
    expected = list(range(start, start + count))
    if sorted(values) != expected:
        raise RangeValidationError(
            f"unit_id coverage mismatch for [{start}, {start + count}): "
            f"got {sorted(values)[:20]} expected {expected[:20]}"
        )
