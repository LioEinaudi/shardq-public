from __future__ import annotations

import hashlib
import heapq
import io
import json
from contextlib import ExitStack
from operator import itemgetter
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, List, Set

from .backends.reports import attempt_report, report_for_job
from .backend_exec import run_backend_op
from .fs import sha256_file, write_records
from .ledger import JobView, Ledger, PartitionView, ShardState, atomic_write_json, read_json
from .spec import PoolSpec


def _selected_attempts(partition_view, shards_root):
    selected = []
    for shard in sorted(partition_view.shards, key=lambda item: item.start):
        if shard.state != ShardState.DONE:
            continue
        candidates = []
        shard_dir = shards_root / shard.shard_id
        for path in (shard_dir / "attempts").glob("*/complete.json"):
            payload = read_json(path, {})
            if isinstance(payload, dict):
                candidates.append((float(payload.get("completed_unix", 0)), path.parent.name, payload, path.parent))
        if not candidates:
            raise RuntimeError(f"DONE shard has no complete attempt: {shard.shard_id}")
        _, attempt_id, complete, attempt_dir = min(candidates, key=lambda row: (row[0], row[1]))
        records_file = attempt_dir / "records.jsonl"
        if not records_file.is_file():
            raise FileNotFoundError(records_file)
        extra = complete.get("backend_extra") or {}
        selected.append((
            {
                **attempt_report(extra),
                "shard_id": shard.shard_id,
                "attempt_id": attempt_id,
                "handle": complete.get("handle", ""),
                "elapsed_seconds": float(complete.get("elapsed_seconds", 0)),
                "records_file": str(records_file.resolve()),
                "complete_file": str((attempt_dir / "complete.json").resolve()),
            }, complete.get("records_sha256"),
        ))
    return selected


def select_attempts(partition_view: PartitionView, shards_root: Path) -> List[dict]:
    """Select and verify complete attempts for callers that only need their metadata."""
    attempts = _selected_attempts(partition_view, shards_root)
    for attempt, checksum in attempts:
        path = Path(attempt["records_file"])
        if sha256_file(path) != checksum:
            raise ValueError(f"records checksum mismatch: {path}")
    return [attempt for attempt, _ in attempts]


def dedupe_records(rows: Iterable[dict]) -> List[dict]:
    winners = {}
    order = []
    for row in rows:
        unit_id = row.get("unit_id")
        if isinstance(unit_id, bool) or not isinstance(unit_id, int):
            raise ValueError(f"record has invalid unit_id: {unit_id!r}")
        key = (unit_id, str(row.get("filter", "none")))
        if key in winners:
            continue
        winners[key] = row
        order.append(key)
    order.sort(key=lambda key: key[0])
    return [winners[key] for key in order]


def coverage(records: List[dict], size: int, quarantined_units: Set[int]) -> dict:
    del quarantined_units
    present = {int(row["unit_id"]) for row in records}
    expected = set(range(size))
    return {
        "missing_units": sorted(expected - present),
        "extra_units": sorted(present - expected),
    }


# Cap each in-memory sort by input bytes. A single JSON line must fit in memory.
_SORT_BYTES = 4 * 1024 * 1024
_MERGE_FAN_IN = 16


class _Unordered(Exception):
    """The selected attempts cannot be concatenated in unit order."""


class _DigestReader(io.RawIOBase):
    """Hash the original bytes underneath universal-newline UTF-8 decoding."""

    def __init__(self, source, digest):
        self.source = source
        self.digest = digest

    def readable(self):
        return True

    def readinto(self, buffer):
        size = self.source.readinto(buffer)
        self.digest.update(memoryview(buffer)[:size])
        return size


def _verified_rows(attempt, checksum):
    path = Path(attempt["records_file"])
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        hashed = io.BufferedReader(_DigestReader(source, digest))
        with io.TextIOWrapper(hashed, encoding="utf-8", newline="") as lines:
            for line in lines:
                if line.strip():
                    row = json.loads(line)
                    unit = row.get("unit_id")
                    if type(unit) is not int:
                        raise ValueError(f"record has invalid unit_id: {unit!r}")
                    length = len(line) if line.isascii() else len(line.encode("utf-8"))
                    yield length, row
    if digest.hexdigest() != checksum:
        raise ValueError(f"records checksum mismatch: {path}")


def _range_rows(attempts):
    # Contiguous ranges are naturally ordered across attempts. Within a small
    # range the backend may emit any order. A large, already ordered range can
    # stream directly; the publisher detects disorder across either boundary.
    for attempt, checksum in attempts:
        source = _verified_rows(attempt, checksum)
        if Path(attempt["records_file"]).stat().st_size > _SORT_BYTES:
            for _, row in source:
                yield row
            continue
        rows = []
        size = 0
        for length, row in source:
            size += length
            if size > _SORT_BYTES:
                raise _Unordered  # File grew since stat. Use bounded runs.
            rows.append(row)
        rows.sort(key=itemgetter("unit_id"))
        yield from rows


def _merge_runs(paths):
    with ExitStack() as stack:
        inputs = [map(json.loads, stack.enter_context(path.open("rb"))) for path in paths]
        # Stable merge uses input order for ties, preserving first-seen filters.
        yield from heapq.merge(*inputs, key=itemgetter("unit_id"))


def _external_order(attempts):
    # Oversized unordered ranges and overlapping legacy attempts use stable
    # external sorting. Bound both open descriptors and decoded rows in memory.
    with TemporaryDirectory(prefix="shardq-sort-") as directory:
        serial = 0

        def spill(rows):
            nonlocal serial
            serial += 1
            path = Path(directory) / str(serial)
            with path.open("w", encoding="utf-8") as output:
                for row in rows:
                    output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            return path

        runs = []
        chunk = []
        size = 0
        for attempt, checksum in attempts:
            for length, row in _verified_rows(attempt, checksum):
                chunk.append(row)
                size += length
                if size >= _SORT_BYTES:
                    chunk.sort(key=itemgetter("unit_id"))
                    runs.append(spill(chunk))
                    chunk.clear()
                    size = 0
        if chunk:
            chunk.sort(key=itemgetter("unit_id"))
            runs.append(spill(chunk))
            chunk.clear()
        while len(runs) > _MERGE_FAN_IN:
            reduced = []
            for start in range(0, len(runs), _MERGE_FAN_IN):
                group = runs[start:start + _MERGE_FAN_IN]
                reduced.append(spill(_merge_runs(group)))
                for path in group:
                    path.unlink()
            runs = reduced
        yield from _merge_runs(runs)


def _publish_records(path, attempts, partition, size, allow_partial):
    missing = []
    count = 0

    def unique(rows):
        nonlocal count
        previous = -1
        filters = set()
        gaps = []
        for row in rows:
            unit = row["unit_id"]
            if not 0 <= unit < size:
                raise ValueError(f"partition {partition} has extra units: [{unit}]")
            if unit < previous:
                raise _Unordered
            if unit != previous:
                if previous + 1 < unit:
                    gaps.append(range(previous + 1, unit))
                previous = unit
                filters.clear()
            key = str(row.get("filter", "none"))
            if key not in filters:
                filters.add(key)
                count += 1
                yield row
        gaps.append(range(previous + 1, size))
        for gap in gaps:
            missing.extend(gap)
        if missing and not allow_partial:
            raise ValueError(f"partition {partition} is missing {len(missing)} units: {missing[:20]}")

    try:
        digest = write_records(path, unique(_range_rows(attempts)))
    except _Unordered:
        # atomic_output discarded the speculative output. Replay the same
        # selected attempts and verify their digests as they enter sorted runs.
        count = 0
        digest = write_records(path, unique(_external_order(attempts)))
    return digest, count, missing


def merge_partition(
    root: Path,
    job: JobView,
    partition: str,
    pool: PoolSpec,
    allow_partial: bool,
    now: float,
) -> dict:
    if partition not in job.partitions:
        raise ValueError(f"unknown partition: {job.job_id}:{partition}")
    view = job.partitions[partition]
    job_dir = Path(root) / "jobs" / job.job_id
    shards_root = job_dir / "partitions" / partition / "shards"
    attempts = _selected_attempts(view, shards_root)
    selected = [attempt for attempt, _ in attempts]
    quarantined_shards = [shard.shard_id for shard in view.shards if shard.state == ShardState.QUARANTINED]
    out_dir = job_dir / "merged" / partition
    records_file = out_dir / "records.jsonl"
    records_sha256, record_count, missing = _publish_records(records_file, attempts, partition, view.size, allow_partial)
    completed = view.size - len(missing)
    backend_meta = {
        "job_id": job.job_id,
        "partition": partition,
        "shards_used": selected,
        "missing_units": missing,
        "quarantined_shards": quarantined_shards,
        "expected_units": view.size,
        "completed_units": completed,
        "record_count": record_count,
        "artifacts": {"records": {"path": str(records_file.resolve()), "sha256": records_sha256}},
    }
    backend_result = run_backend_op(
        pool,
        "merge",
        {
            "backend": job.spec["backend"],
            "backend_args": job.spec.get("backend_args") or {},
            "pool": pool.snapshot(),
            "pins": job.spec.get("pins") or {},
            "partition": {"name": partition, "size": view.size, "traits": {}},
            "records_file": str(records_file.resolve()),
            "out_dir": str(out_dir.resolve()),
            "meta": backend_meta,
        },
    )
    results_file = out_dir / "results.json"
    if not results_file.is_file():
        raise RuntimeError(f"backend merge did not create {results_file}")
    attempts_total = sum(
        sum(1 for path in (shards_root / shard.shard_id / "attempts").iterdir() if path.is_dir())
        if (shards_root / shard.shard_id / "attempts").is_dir()
        else 0
        for shard in view.shards
    )
    elapsed_sum = sum(float(item["elapsed_seconds"]) for item in selected)
    public_selected = [
        {key: value for key, value in item.items() if key not in {"records_file", "complete_file"}}
        for item in selected
    ]
    state = "partial" if missing else "complete"
    artifacts = dict(backend_meta["artifacts"], results={
        "path": str(results_file.resolve()), "sha256": sha256_file(results_file),
    })
    merge_payload = {
        "status": state,
        "job_id": job.job_id,
        "partition": partition,
        "size": view.size,
        "effective": completed,
        "record_count": record_count,
        "artifacts": artifacts,
        "missing_units": missing,
        "quarantined_shards": quarantined_shards,
        "shards_used": public_selected,
        "elapsed_seconds_sum": elapsed_sum,
        "attempts_total": attempts_total,
        "preemptions_total": sum(shard.preemptions for shard in view.shards),
        "sampling_note": (
            "complete contiguous shard coverage"
            if not missing
            else f"data gap: {len(missing)} missing units"
        ),
        "selfcheck": backend_result.get("selfcheck", {"shards_checked": 0, "mismatches": []}),
        "merged_unix": now,
    }
    atomic_write_json(out_dir / "merge.json", merge_payload)
    marker_dir = job_dir / "markers" / job.column
    marker = {
        **report_for_job(job.spec).get("marker", {}),
        "artifacts": artifacts,
        "schema_version": 2 if "backend_report" in job.spec else 1,
        "status": state,
        "task": partition,
        "column": job.column,
        "completed_unix": now,
        "elapsed_seconds": elapsed_sum,
        "result_file": str(results_file.resolve()),
        "output_dir": str(out_dir.resolve()),
        "backend": job.spec["backend"],
        "protocol_id": job.spec["protocol_id"],
        "shardq": {
            "job_id": job.job_id,
            "shards": len(selected),
            "attempts": attempts_total,
            "missing_units": missing,
        },
    }
    suffix = "partial.json" if missing else "complete.json"
    atomic_write_json(marker_dir / f"{partition}.{suffix}", marker)
    Ledger(root).set_partition_status(job.job_id, partition, "partial" if missing else "merged")
    merge_payload["result_file"] = str(results_file.resolve())
    merge_payload["marker_file"] = str((marker_dir / f"{partition}.{suffix}").resolve())
    return merge_payload
