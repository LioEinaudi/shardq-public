from __future__ import annotations

import json
import random
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

from .base import CommandError, TransientSubmitError


FLOW_CONTROL_REJECTION = "request was rejected: beyond the current flow control limit"
SUBMIT_RETRIES = 5
TERMINAL = {
    "Success",
    "Failed",
    "Cancelled",
    "Terminated",
    "Killed",
    "Stopped",
}
GPU_PER_REPLICA = {
    "ml.pni3ln.4xlarge": 1,
    "ml.pni3ln.5xlarge": 1,
    "ml.pni3ln.17xlarge": 4,
    "ml.pni3ln.45xlarge": 8,
}
_TASK_ID_PATTERN = re.compile(r"task_id=(t-[A-Za-z0-9-]+)")
_TEXT_RECEIPT = re.compile(r"^(?:submitted )?task_id=(t-[A-Za-z0-9-]+)[ \t]*$", re.M)
_ARRAY_START = re.compile(r"^[ \t]*\[\s*(?=[{\]])", re.M)
_DOCUMENT_START = re.compile(r'^[ \t]*(?:\[\s*(?=[{\]])|\{\s*(?="))', re.M)


def _unique_fields(pairs):
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ValueError("duplicate fields in Volc JSON response")
    return fields


def parse_volc_json(output: str) -> List[Dict[str, Any]]:
    start = _ARRAY_START.search(output)
    if not start:
        raise ValueError(f"Volc output did not contain a JSON array: {output[-1000:]}")
    return json.loads(output[start.start():], object_pairs_hook=_unique_fields)


def _run_output(arguments: List[str], timeout: int = 120) -> str:
    proc = subprocess.run(
        ["volc", "ml_task", *arguments],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=timeout,
    )
    if proc.returncode:
        raise CommandError(f"volc {' '.join(arguments)}", proc.returncode, proc.stdout)
    return proc.stdout


def run_volc(
    arguments: List[str], timeout: int = 120
) -> List[Dict[str, Any]]:
    return parse_volc_json(_run_output(arguments, timeout))


def _parse_submit_task_id(output: str) -> str:
    ids = set(_TEXT_RECEIPT.findall(output))
    # The CLI can prefix its JSON with log messages. Once the document starts,
    # parse it as one receipt: skipping to a later object can hide a conflict.
    start = _DOCUMENT_START.search(output)
    if start:
        payload = json.loads(output[start.start():], object_pairs_hook=_unique_fields)
        candidates = payload if isinstance(payload, list) else [payload]
        for candidate in candidates:
            if not isinstance(candidate, dict) or not candidate.keys() & {"Id", "JobId", "task_id"}:
                raise ValueError(f"invalid submission receipt: {output[-1000:]}")
            for key in ("Id", "JobId", "task_id"):
                if key in candidate:
                    value = candidate[key]
                    if not isinstance(value, str) or not re.fullmatch(r"t-[A-Za-z0-9-]+", value):
                        raise ValueError(f"invalid task id in submission output: {output[-1000:]}")
                    ids.add(value)
    if len(ids) != 1 or set(_TASK_ID_PATTERN.findall(output)) - ids:
        raise ValueError(f"expected one task id in submission output: {output[-1000:]}")
    return ids.pop()


def submit(conf_path: Path) -> str:
    for attempt in range(1, SUBMIT_RETRIES + 1):
        try:
            output = _run_output(
                ["submit", "--conf", str(Path(conf_path)), "-o", "json"]
            )
        except RuntimeError as exc:
            response = exc.output if isinstance(exc, CommandError) else str(exc)
            if response.strip().lower() != FLOW_CONTROL_REJECTION:
                raise
            if attempt == SUBMIT_RETRIES:
                raise TransientSubmitError(str(exc)) from exc
            time.sleep(random.uniform(1.0, 3.0) * attempt)
        else:
            return _parse_submit_task_id(output)


def get_task(task_id: str) -> dict:
    rows = run_volc(["get", "-i", task_id, "-o", "json"])
    if len(rows) != 1:
        raise RuntimeError(
            f"expected one Volc task for {task_id}, got {len(rows)}"
        )
    row = rows[0]
    if not isinstance(row, dict) or any(
        row[key] != task_id for key in ("Id", "JobId", "task_id") if key in row
    ):
        raise ValueError(f"Volc task identity does not match {task_id}")
    return row


def list_tasks(queue: str, creator: str) -> List[dict]:
    summaries = []
    seen = set()
    offset = 0
    while True:
        page = run_volc(
            ["list", "--limit", "100", "--offset", str(offset), "-o", "json"]
        )
        for row in page:
            task_id = row.get("JobId") if isinstance(row, dict) else None
            if (not isinstance(task_id, str) or not task_id or task_id in seen
                    or not row.get("ResourceQueueId")):
                raise ValueError("incomplete Volc capacity listing: missing identity/queue or repeated task")
            seen.add(task_id)
            if row.get("ResourceQueueId") != queue:
                continue
            if not row.get("Creator"):
                raise ValueError("incomplete Volc capacity listing: missing task creator")
            if row.get("Creator") != creator:
                continue
            if str(row.get("Status")) in TERMINAL:
                continue
            summaries.append(row)
        if len(page) < 100:
            break
        offset += 100
    return [get_task(str(row["JobId"])) for row in summaries]


def requested_gpu_slots(tasks: List[dict]) -> int:
    total = 0
    for task in tasks:
        if str(task.get("Status")) in TERMINAL:
            continue
        for role in task.get("TaskRoleSpecs", []):
            if not isinstance(role, dict):
                continue
            replicas = int(role.get("RoleReplicas", 0))
            if replicas <= 0:
                continue
            flavor = str(
                role.get("ResourceSpecId")
                or (role.get("ResourceSpec") or {}).get("FlavorID")
                or ""
            )
            total += replicas * GPU_PER_REPLICA.get(flavor, 8)
    return total


def cancel(task_id: str) -> None:
    _run_output(["cancel", "--id", task_id])
