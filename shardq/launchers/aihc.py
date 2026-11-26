from __future__ import annotations

import hashlib
import shlex
from pathlib import Path
from typing import Dict, List, Optional

from ..fs import atomic_output, atomic_write_json
from . import aihc_client
from .base import Launcher


class AihcLauncher(Launcher):
    name = "aihc"

    def submission_inputs(self) -> dict:
        path = self.pool.aihc.get("aihc_config")
        if not path:
            return {}
        return {"config_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()}

    def _entrypoint(self, env: Dict[str, str]) -> str:
        combined = {str(key): str(value) for key, value in self.pool.env.items()}
        combined.update({str(key): str(value) for key, value in env.items()})
        assignments = " ".join(
            f"{key}={shlex.quote(value)}"
            for key, value in sorted(combined.items())
        )
        entry = Path(self.pool.entry).expanduser()
        if not entry.is_absolute():
            raise ValueError(
                f"aihc pool {self.pool.name} entry must be an absolute path"
            )
        command = f"bash {shlex.quote(str(entry))}"
        return f"{assignments} {command}".strip()

    def submit(
        self, worker_id: str, worker_argv: List[str], env: Dict[str, str]
    ) -> str:
        del worker_argv
        configured = self.pool.aihc
        for key in ("pool_id", "queue_id", "image", "datasources"):
            if key not in configured:
                raise ValueError(
                    f"aihc pool {self.pool.name} requires aihc.{key}"
                )
        datasources = configured["datasources"]
        if not isinstance(datasources, list):
            raise ValueError(f"aihc pool {self.pool.name} aihc.datasources must be a list")

        kill_after = int(configured.get("kill_after_seconds", 120))
        deadline = self.pool.worker_max_wall_seconds + 1800
        entry_line = self._entrypoint(env)
        command = (
            f"timeout -s TERM -k {kill_after} {deadline} "
            f"bash -c {shlex.quote(entry_line)}"
        )
        gpu_slots = int(configured.get("gpu_slots", 1))
        resources = []
        if gpu_slots > 0:
            gpu_resource = configured.get("gpu_resource")
            if not gpu_resource:
                raise ValueError(f"aihc pool {self.pool.name} requires aihc.gpu_resource")
            resources.append({"name": str(gpu_resource), "quantity": gpu_slots})
        resources.extend(
            [
                {"name": "cpu", "quantity": int(configured.get("cpu", 16))},
                {
                    "name": "memory",
                    "quantity": int(configured.get("memory_gb", 128)),
                },
                {
                    "name": "sharedMemory",
                    "quantity": int(configured.get("shared_memory_gb", 16)),
                },
            ]
        )
        payload = {
            "name": aihc_client.normalize_job_name(worker_id),
            "queue": str(configured["queue_id"]),
            "jobFramework": "pytorch",
            "jobSpec": {
                "command": command,
                "image": str(configured["image"]),
                "replicas": 1,
                "resources": resources,
                "envs": [],
                "enableRDMA": False,
                "hostNetwork": False,
            },
            "faultTolerance": False,
            "enableBccl": False,
            "codeSource": {"mountPath": "/workspace"},
            "datasources": datasources,
        }
        # Idle (闲时) submission: AIHC CLI >= v1.6.4 `--idle-task` on a managed pool whose queue allows idle jobs.
        # The server forces priority to lowest, so the request must not carry any priority.
        idle_task = bool(configured.get("idle_task", False))
        if idle_task:
            payload["idleTask"] = True
        else:
            payload["priority"] = str(configured.get("priority", "low"))

        workers = self.root / "workers"
        job_file = workers / f"{worker_id}.aihc.json"
        atomic_write_json(job_file, payload)

        code_dir = workers / f"{worker_id}.code"
        train = code_dir / "train.sh"
        with atomic_output(train) as temporary:
            temporary.write_text(f"#!/bin/bash\nset -euo pipefail\n{command}\n", encoding="utf-8")
            temporary.chmod(0o755)

        return aihc_client.create_job(
            str(job_file),
            str(code_dir),
            str(configured["pool_id"]),
            str(configured["queue_id"]),
            binary=str(configured.get("aihc_binary", "aihc")),
            config_file=str(configured.get("aihc_config", "")),
            **({"idle_task": True} if idle_task else {}),
        )

    def status(self, handle: str) -> str:
        configured = self.pool.aihc
        status = aihc_client.get_job_status(
            handle,
            str(configured["pool_id"]),
            str(configured["queue_id"]),
            binary=str(configured.get("aihc_binary", "aihc")),
            config_file=str(configured.get("aihc_config", "")),
        )
        if status == "Running":
            return "running"
        if status in aihc_client.TERMINAL:
            return f"terminal:{status}"
        return "queued"

    def kill(self, handle: str) -> None:
        configured = self.pool.aihc
        aihc_client.stop_job(
            handle,
            str(configured["pool_id"]),
            str(configured["queue_id"]),
            binary=str(configured.get("aihc_binary", "aihc")),
            config_file=str(configured.get("aihc_config", "")),
        )

    def capacity_slots(self) -> Optional[int]:
        return None
