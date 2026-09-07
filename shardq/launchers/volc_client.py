from __future__ import annotations

import json
import random
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

from .base import TransientSubmitError


FLOW_CONTROL_MARKER = "beyond the current flow control limit"
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


def parse_volc_json(output: str) -> List[Dict[str, Any]]:
    for index, character in enumerate(output):
        if character != "[":
            continue
        try:
            payload = json.loads(output[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            return payload
    raise ValueError(
        f"Volc output did not contain a JSON array: {output[-1000:]}"
    )


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
        raise RuntimeError(
            f"volc {' '.join(arguments)} failed ({proc.returncode}): "
            f"{proc.stdout[-2000:]}"
        )
    return proc.stdout


def run_volc(
    arguments: List[str], timeout: int = 120
) -> List[Dict[str, Any]]:
    return parse_volc_json(_run_output(arguments, timeout))


def _parse_submit_task_id(output: str) -> str:
    match = _TASK_ID_PATTERN.search(output)
    if match:
        return match.group(1)
    for index, character in enumerate(output):
        if character not in "[{":
            continue
        try:
            payload = json.loads(output[index:])
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            task_id = (
                candidate.get("Id")
                or candidate.get("JobId")
                or candidate.get("task_id")
            )
            if task_id:
                return str(task_id)
    raise ValueError(f"submission output has no task id: {output[-1000:]}")


def submit(conf_path: Path) -> str:
    for attempt in range(1, SUBMIT_RETRIES + 1):
        try:
            output = _run_output(
                ["submit", "--conf", str(Path(conf_path)), "-o", "json"]
            )
        except RuntimeError as exc:
            if FLOW_CONTROL_MARKER not in str(exc):
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
    return rows[0]


def list_tasks(queue: str, creator: str) -> List[dict]:
    summaries = []
    seen = set()
    offset = 0
    while True:
        page = run_volc(
            ["list", "--limit", "100", "--offset", str(offset), "-o", "json"]
        )
        for row in page:
            task_id = str(row.get("JobId", ""))
            if not task_id or task_id in seen:
                continue
            seen.add(task_id)
            if row.get("ResourceQueueId") != queue:
                continue
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
