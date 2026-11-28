from __future__ import annotations

import hashlib
import shlex
from importlib import resources
from pathlib import Path
from typing import Dict, List, Optional

from ..fs import atomic_write_text
from ..ledger import read_json
from . import volc_client
from .base import Admission, Launcher, SubmissionChangedError


class VolcLauncher(Launcher):
    name = "volc"
    initial_status = "queued"
    admission_only_args = ("max_queued", "gpu_cap", "capacity_mode")

    @classmethod
    def admission(cls, pool, available_slots):
        args = pool.launcher_args
        queue_limit = args.get("max_queued", 0 if available_slots is None else None)
        scope = ("volc", str(args.get("queue", "")))
        if args.get("creator") is not None:
            scope += (str(args["creator"]),)
        return Admission(
            units_per_worker=int(args.get("gpu_slots", 1)),
            scope=scope,
            max_queued=int(queue_limit) if queue_limit is not None else None,
            recheck_shared_capacity=args.get("capacity_mode") == "audit",
        )

    @classmethod
    def execution_source(cls, pool):
        args = pool.launcher_args
        return {key: args.get(key) for key in ("image", "flavor", "queue")}

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
        configured = self.pool.launcher_args.get("template")
        if configured:
            return Path(str(configured)).expanduser().read_text(
                encoding="utf-8"
            )
        template = resources.files("shardq.templates").joinpath(
            "volc_worker.yaml.tmpl"
        )
        return template.read_text(encoding="utf-8")

    def submission_inputs(self) -> dict:
        return {"template_sha256": hashlib.sha256(self._template_text().encode("utf-8")).hexdigest()}

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
            "QUEUE": self.pool.launcher_args["queue"],
            "FLAVOR": self.pool.launcher_args["flavor"],
            "IMAGE": self.pool.launcher_args["image"],
            "PREEMPTIBLE": str(
                bool(self.pool.launcher_args.get("preemptible", True))
            ).lower(),
            "PRIORITY": int(self.pool.launcher_args.get("priority", 4)),
            "ACTIVE_DEADLINE": self.pool.worker_max_wall_seconds + 1800,
            "VEPFS_ID": self.pool.launcher_args["vepfs_id"],
        }
        template = self._template_text()
        if env.get("SHARDQ_REQUEST_ID"):
            record = read_json(self.root / "workers" / f"{worker_id}.json", {})
            digest = hashlib.sha256(template.encode("utf-8")).hexdigest()
            if (record.get("request_id") != env["SHARDQ_REQUEST_ID"]
                    or (record.get("launcher_inputs") or {}).get("template_sha256") != digest):
                raise SubmissionChangedError("Volc template differs from the recorded submission intent")
        rendered = template.format_map(values)
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
        if self.pool.launcher_args.get("capacity_mode", "probe") != "audit":
            return None
        queue = str(self.pool.launcher_args["queue"])
        creator = str(self.pool.launcher_args["creator"])
        gpu_cap = int(self.pool.launcher_args["gpu_cap"])
        used = volc_client.requested_gpu_slots(
            volc_client.list_tasks(queue, creator)
        )
        return max(0, gpu_cap - used)
