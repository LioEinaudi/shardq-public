from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, List, Set

from .backends.reports import attempt_report, report_for_job
from .backend_exec import run_backend_op
from .fs import write_records
from .ledger import JobView, Ledger, PartitionView, ShardState, atomic_write_json, read_json
from .spec import PoolSpec


def select_attempts(partition_view: PartitionView, shards_root: Path) -> List[dict]:
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
        digest = hashlib.sha256(records_file.read_bytes()).hexdigest()
        if digest != complete.get("records_sha256"):
            raise ValueError(f"records checksum mismatch: {records_file}")
        extra = complete.get("backend_extra") or {}
        selected.append(
            {
                **attempt_report(extra),
                "shard_id": shard.shard_id,
                "attempt_id": attempt_id,
                "handle": complete.get("handle", ""),
                "elapsed_seconds": float(complete.get("elapsed_seconds", 0)),
                "records_file": str(records_file.resolve()),
                "complete_file": str((attempt_dir / "complete.json").resolve()),
            }
        )
    return selected


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
    selected = select_attempts(view, shards_root)
    rows = []
    for attempt in selected:
        with Path(attempt["records_file"]).open(encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle if line.strip())
    records = dedupe_records(rows)
    quarantined_units = set()
    quarantined_shards = []
    for shard in view.shards:
        if shard.state == ShardState.QUARANTINED:
            quarantined_shards.append(shard.shard_id)
            quarantined_units.update(range(shard.start, shard.start + shard.count))
    coverage_report = coverage(records, view.size, quarantined_units)
    if coverage_report["extra_units"]:
        raise ValueError(f"partition {partition} has extra units: {coverage_report['extra_units'][:20]}")
    missing = coverage_report["missing_units"]
    if missing and not allow_partial:
        raise ValueError(f"partition {partition} is missing {len(missing)} units: {missing[:20]}")
    out_dir = job_dir / "merged" / partition
    records_file = out_dir / "records.jsonl"
    records_sha256 = write_records(records_file, records)
    completed = view.size - len(missing)
    backend_meta = {
        "job_id": job.job_id,
        "partition": partition,
        "shards_used": selected,
        "missing_units": missing,
        "quarantined_shards": quarantined_shards,
        "expected_units": view.size,
        "completed_units": completed,
        "record_count": len(records),
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
        "path": str(results_file.resolve()), "sha256": hashlib.sha256(results_file.read_bytes()).hexdigest(),
    })
    merge_payload = {
        "status": state,
        "job_id": job.job_id,
        "partition": partition,
        "size": view.size,
        "effective": completed,
        "record_count": len(records),
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
