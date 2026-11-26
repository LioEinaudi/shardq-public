from __future__ import annotations

import shlex
from importlib import resources
from pathlib import Path
from typing import Dict, List, Optional

from ..fs import atomic_write_text
from . import volc_client
from .base import Launcher


class VolcLauncher(Launcher):
    name = "volc"

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
                f"volc pool {self.pool.name} entry must be an absolute path"
            )
        command = f"bash {shlex.quote(str(entry))}"
        return f"{assignments} {command}".strip()

    def _template_text(self) -> str:
        configured = self.pool.volc.get("template")
        if configured:
            return Path(str(configured)).expanduser().read_text(
                encoding="utf-8"
            )
        template = resources.files("shardq.templates").joinpath(
            "volc_worker.yaml.tmpl"
        )
        return template.read_text(encoding="utf-8")

    def submit(
        self, worker_id: str, worker_argv: List[str], env: Dict[str, str]
    ) -> str:
        del worker_argv
        values = {
            "TASK_NAME": worker_id,
            "DESCRIPTION": (
                f"ShardQ preemptible worker for pool {self.pool.name}"
            ),
            "ENTRYPOINT": self._entrypoint(env),
            "QUEUE": self.pool.volc["queue"],
            "FLAVOR": self.pool.volc["flavor"],
            "IMAGE": self.pool.volc["image"],
            "PREEMPTIBLE": str(
                bool(self.pool.volc.get("preemptible", True))
            ).lower(),
            "PRIORITY": int(self.pool.volc.get("priority", 4)),
            "ACTIVE_DEADLINE": self.pool.worker_max_wall_seconds + 1800,
            "VEPFS_ID": self.pool.volc["vepfs_id"],
        }
        rendered = self._template_text().format_map(values)
        config = self.root / "workers" / f"{worker_id}.yaml"
        atomic_write_text(config, rendered)
        return volc_client.submit(config)

    def status(self, handle: str) -> str:
        status = str(volc_client.get_task(handle).get("Status", "Unknown"))
        if status == "Running":
            return "running"
        if status in volc_client.TERMINAL or status == "Exception":
            return f"terminal:{status}"
        return "queued"

    def kill(self, handle: str) -> None:
        volc_client.cancel(handle)

    def capacity_slots(self) -> Optional[int]:
        if self.pool.volc.get("capacity_mode", "probe") != "audit":
            return None
        queue = str(self.pool.volc["queue"])
        creator = str(self.pool.volc["creator"])
        gpu_cap = int(self.pool.volc["gpu_cap"])
        used = volc_client.requested_gpu_slots(
            volc_client.list_tasks(queue, creator)
        )
        return max(0, gpu_cap - used)
