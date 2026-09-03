from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from .ledger import Ledger, ShardState, read_json


def _serialize_job(job) -> dict:
    payload = asdict(job)
    for partition in payload["partitions"].values():
        for shard in partition["shards"]:
            state = shard["state"]
            shard["state"] = state.value if isinstance(state, ShardState) else str(state)
    return payload


def render_status(root: Path, job_id: Optional[str], as_json: bool) -> str:
    root = Path(root).resolve()
    ledger = Ledger(root)
    job_ids = [job_id] if job_id else ledger.list_jobs()
    jobs = [_serialize_job(ledger.scan_job(value, time.time())) for value in job_ids]
    workers = []
    for path in sorted((root / "workers").glob("*.json")):
        payload = read_json(path, {})
        if isinstance(payload, dict) and payload.get("worker_id") and payload.get("launcher"):
            workers.append(payload)
    if as_json:
        return json.dumps({"jobs": jobs, "workers": workers}, ensure_ascii=False, indent=2)
    lines = []
    for job in jobs:
        lines.extend(
            [
                f"Job {job['job_id']} (pool={job['pool']}, column={job['column']}, stopped={job['stopped']})",
                "partition\tsize\tdone_units\tdone\tleased\tpending\tbackoff\tquarantined\test_s/unit\tstatus",
            ]
        )
        for name, row in job["partitions"].items():
            lines.append(
                "\t".join(
                    str(value)
                    for value in (
                        name,
                        row["size"],
                        row["done_units"],
                        row["done"],
                        row["leased"],
                        row["pending"],
                        row["backoff"],
                        row["quarantined"],
                        f"{row['unit_seconds_est']:.3f}",
                        row["status"],
                    )
                )
            )
        lines.append("")
    lines.append("Workers")
    lines.append("worker_id\tpool\tlauncher\thandle\tstatus")
    for worker in workers:
        lines.append(
            "\t".join(
                str(worker.get(key, ""))
                for key in ("worker_id", "pool", "launcher", "handle", "last_status")
            )
        )
    return "\n".join(lines).rstrip() + "\n"
