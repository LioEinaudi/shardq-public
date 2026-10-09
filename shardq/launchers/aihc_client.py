from __future__ import annotations

import random
import re
import subprocess
import time
from typing import List

from .base import TransientSubmitError


TERMINAL = {
    "Succeeded",
    "Failed",
    "Stopped",
    "ManualTermination",
    "Terminated",
    "Cancelled",
    "Canceled",
    "Deleted",
    "Timeout",
}
TRANSIENT_MARKERS = (
    "flow control",
    "quota",
    "too many requests",
    "rate limit",
    "限流",
    "配额",
    "429",
)
SUBMIT_RETRIES = 5
_JOB_ID_PATTERN = re.compile(r"job-[a-z0-9]+")
_STATUS_PATTERN = re.compile(r"(?m)^    status: (\S+)\s*$")


def _run_output(
    args: List[str], *, binary: str = "aihc", config_file: str = "", timeout: int = 120
) -> str:
    command = [binary]
    if config_file:
        command.extend(["-C", config_file])
    command.extend(args)
    proc = subprocess.run(
        command,
        input="q\n",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=timeout,
    )
    if proc.returncode:
        raise RuntimeError(
            f"{' '.join(command)} failed ({proc.returncode}): {proc.stdout[-2000:]}"
        )
    return proc.stdout


def create_job(
    job_file: str,
    code_dir: str,
    pool: str,
    queue: str,
    *,
    binary: str = "aihc",
    config_file: str = "",
    idle_task: bool = False,
) -> str:
    args = [
        "job",
        "create",
        "-f",
        str(job_file),
        "-p",
        str(pool),
        "-q",
        str(queue),
        "--local-code",
        str(code_dir),
        "--code-dir",
        "/workspace",
    ]
    if idle_task:
        args.append("--idle-task")
    for attempt in range(1, SUBMIT_RETRIES + 1):
        try:
            output = _run_output(
                args, binary=binary, config_file=config_file, timeout=180
            )
            lowered = output.lower()
            ids = set(_JOB_ID_PATTERN.findall(lowered))
            if "error:" in lowered or "创建失败" in output or len(ids) != 1:
                raise RuntimeError(
                    f"cannot parse one AIHC job id from submission output: {output[-2000:]}"
                )
            return next(iter(ids))
        except RuntimeError as exc:
            lowered = str(exc).lower()
            if not any(marker in lowered for marker in TRANSIENT_MARKERS):
                raise
            if attempt == SUBMIT_RETRIES:
                raise TransientSubmitError(str(exc)) from exc
            time.sleep(random.uniform(1.0, 3.0) * attempt)
    raise AssertionError("unreachable")


def get_job_status(
    job_id: str,
    pool: str,
    queue: str,
    *,
    binary: str = "aihc",
    config_file: str = "",
) -> str:
    output = _run_output(
        ["job", "get", job_id, "-p", pool, "-q", queue],
        binary=binary,
        config_file=config_file,
    )
    match = _STATUS_PATTERN.search(output)
    if not match:
        raise RuntimeError("cannot parse status")
    return match.group(1)


def stop_job(
    job_id: str,
    pool: str,
    queue: str,
    *,
    binary: str = "aihc",
    config_file: str = "",
) -> None:
    _run_output(
        ["job", "stop", job_id, "-p", pool, "-q", queue],
        binary=binary,
        config_file=config_file,
    )


def normalize_job_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9-]", "-", str(value).lower())
    normalized = re.sub(r"-+", "-", normalized).strip("-")
    return normalized[:60].rstrip("-")
