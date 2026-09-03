from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

from .base import Launcher


class LocalLauncher(Launcher):
    name = "local"

    def __init__(self, root: Path, pool, repo_root: Path) -> None:
        super().__init__(root, pool, repo_root)
        self.processes: Dict[int, subprocess.Popen] = {}

    def submit(self, worker_id: str, worker_argv: List[str], env: Dict[str, str]) -> str:
        workers = self.root / "workers"
        workers.mkdir(parents=True, exist_ok=True)
        log = (workers / f"{worker_id}.log").open("ab")
        child_env = dict(os.environ)
        child_env.update({str(key): str(value) for key, value in env.items()})
        process = subprocess.Popen(
            [sys.executable, "-m", "shardq", "worker", *worker_argv],
            cwd=str(self.repo_root),
            env=child_env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        log.close()
        self.processes[process.pid] = process
        return f"local:{process.pid}"

    def status(self, handle: str) -> str:
        try:
            pid = int(handle.split(":", 1)[1])
        except (IndexError, ValueError):
            return "unknown"
        process = self.processes.get(pid)
        if process is not None:
            code = process.poll()
            return "running" if code is None else f"terminal:{code}"
        try:
            state = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()[2]
            if state == "Z":
                return "terminal:zombie"
            os.kill(pid, 0)
            return "running"
        except (FileNotFoundError, ProcessLookupError, PermissionError, IndexError):
            return "terminal:missing"

    def kill(self, handle: str) -> None:
        pid = int(handle.split(":", 1)[1])
        os.kill(pid, signal.SIGTERM)
