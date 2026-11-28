from __future__ import annotations

import json
import os
import re
import signal
import socket
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

from .base import Launcher


_GENERATION = re.compile(r"local:([1-9][0-9]*):([^:]+):([0-9a-fA-F-]{36}):([0-9]+)")


def _process_stat(pid: int) -> List[str]:
    # The comm field is parenthesized and can contain both spaces and ')'.
    record = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="surrogateescape")
    fields = record.rpartition(")")[2].split()
    if len(fields) < 20 or not fields[19].isdigit():
        raise OSError(f"cannot read process generation for PID {pid}")
    return fields


def _boot_id() -> str:
    return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()


def local_process_handle(pid: int) -> str:
    """Identify a process generation on the scheduler/worker's Linux host."""
    return f"local:{pid}:{socket.gethostname()}:{_boot_id()}:{_process_stat(pid)[19]}"


class LocalLauncher(Launcher):
    name = "local"
    initial_status = "running"

    @classmethod
    def startup_receipt(cls):
        try:
            return {"local_handle": local_process_handle(os.getpid())}
        except (OSError, ValueError, IndexError) as error:
            return {"identity_error": str(error)}

    def __init__(self, root: Path, pool, repo_root: Path) -> None:
        super().__init__(root, pool, repo_root)
        self.processes: Dict[int, subprocess.Popen] = {}

    def submit(self, worker_id: str, worker_argv: List[str], env: Dict[str, str]) -> str:
        workers = self.root / "workers"
        workers.mkdir(parents=True, exist_ok=True)
        child_env = dict(os.environ)
        child_env.update({str(key): str(value) for key, value in env.items()})
        with (workers / f"{worker_id}.log").open("ab") as log:
            process = subprocess.Popen(
                [sys.executable, "-m", "shardq", "worker", *worker_argv],
                cwd=str(self.repo_root),
                env=child_env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        self.processes[process.pid] = process
        if env.get("SHARDQ_REQUEST_ID"):
            try:
                return local_process_handle(process.pid)
            except OSError:
                # The child exists and its PID is known even without Linux procfs.
                pass
        return f"local:{process.pid}"

    def submission_inputs(self) -> dict:
        return {"python": sys.executable, "working_directory": str(self.repo_root),
                "host": socket.gethostname()}

    def lookup_submission(self, worker_id: str) -> List[str]:
        workers = self.root / "workers"
        row = json.loads((workers / f"{worker_id}.json").read_text(encoding="utf-8"))
        if not row.get("request_id") or (row["worker_id"], row["pool"], row["launcher"]) != (
            worker_id, self.pool.name, self.pool.launcher
        ):
            raise ValueError(f"invalid local submission identity for {worker_id}")
        handles = set()
        receipt_dir = workers / f"{worker_id}.starts"
        try:
            paths = list(receipt_dir.iterdir())
        except FileNotFoundError:
            return []
        for path in paths:
            if path.suffix != ".json":
                continue
            receipt = json.loads(path.read_text(encoding="utf-8"))
            if any(receipt.get(key) != row[key] for key in ("request_id", "worker_id", "pool", "launcher")):
                raise ValueError(f"mismatched local startup receipt: {path}")
            handle = receipt.get("local_handle")
            if not isinstance(handle, str) or not _GENERATION.fullmatch(handle):
                raise ValueError(f"missing or invalid local process generation: {path}")
            handles.add(handle)
        return sorted(handles)

    def status(self, handle: str) -> str:
        generation = _GENERATION.fullmatch(handle)
        if generation:
            raw_pid, host, boot, ticks = generation.groups()
            if host != socket.gethostname():
                raise RuntimeError(f"cannot inspect worker on different host {host}")
            if boot != _boot_id():
                return "terminal:reboot"
            pid = int(raw_pid)
            try:
                stat = _process_stat(pid)
            except (FileNotFoundError, ProcessLookupError):
                return "terminal:missing"
            if stat[19] != ticks:
                return "terminal:reused"
        elif re.fullmatch(r"local:[1-9][0-9]*", handle):
            pid = int(handle.split(":")[1])
        else:
            return "unknown"
        process = self.processes.get(pid)
        if process is not None:
            code = process.poll()
            return "running" if code is None else f"terminal:{code}"
        if generation:
            return "terminal:zombie" if stat[0] == "Z" else "running"
        try:
            if _process_stat(pid)[0] == "Z":
                return "terminal:zombie"
        except FileNotFoundError:
            pass  # A legacy PID can also come from a host without procfs.
        try:
            os.kill(pid, 0)
            return "running"
        except (FileNotFoundError, ProcessLookupError):
            return "terminal:missing"

    def kill(self, handle: str) -> None:
        if re.fullmatch(r"local:[1-9][0-9]*", handle):
            os.kill(int(handle.split(":")[1]), signal.SIGTERM)
            return
        generation = _GENERATION.fullmatch(handle)
        if not generation:
            raise ValueError(f"invalid local handle: {handle}")
        # Pin the PID before checking its birth stamp so reuse cannot redirect the signal.
        fd = os.pidfd_open(int(generation.group(1)))
        try:
            if self.status(handle) == "running":
                signal.pidfd_send_signal(fd, signal.SIGTERM)
        finally:
            os.close(fd)
