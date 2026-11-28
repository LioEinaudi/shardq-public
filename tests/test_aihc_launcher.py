from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shardq.launchers import get_launcher
from shardq.launchers.aihc import AihcLauncher
from shardq.spec import pool_from_dict
from shardq.submission import fingerprint


DATASOURCES = [
    {"type": "bos", "sourcePath": "example-bucket/", "mountPath": "/mnt/bos/example-bucket"},
    {"type": "pfsl2", "sourcePath": "/example-data", "mountPath": "/example-data", "name": "pfs-example", "options": {"datasetVersion": "1"}},
]


def _pool():
    return pool_from_dict(
        "example",
        {
            "backend": "fake", "launcher": "aihc", "entry": "/abs/entry.sh",
            "aihc": {
                "pool_id": "pool-id", "queue_id": "queue-id", "image": "image:tag",
                "gpu_resource": "baidu.com/gpu", "gpu_slots": 1, "cpu": 16,
                "memory_gb": 128, "shared_memory_gb": 16, "priority": "low",
                "datasources": DATASOURCES,
            },
            "env": {"POOL_ENV": "pool value"}, "worker_max_wall_seconds": 14400,
        },
    )


class AihcLauncherTest(unittest.TestCase):
    def test_same_path_aihc_config_change_alters_submission_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pool = _pool()
            launcher = AihcLauncher(root, pool, Path("/package"))
            with patch.object(Path, "read_bytes", side_effect=AssertionError("implicit config read")):
                self.assertEqual(launcher.submission_inputs(), {})
            config = root / "client.conf"
            pool.aihc["aihc_config"] = str(config)
            config.write_bytes(b"account: first")
            inputs = launcher.submission_inputs()
            before = fingerprint(pool, inputs, ([], {}))
            config.write_bytes(b"account: other")
            self.assertNotEqual(before, fingerprint(pool, launcher.submission_inputs(), ([], {})))
            self.assertEqual(inputs, {"config_sha256": hashlib.sha256(b"account: first").hexdigest()})

    def test_submit_writes_cli_v1_json_and_code(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch(
            "shardq.launchers.aihc_client.create_job", return_value="job-a1"
        ) as create_job:
            root = Path(directory)
            launcher = AihcLauncher(root, _pool(), Path("/package"))
            handle = launcher.submit(
                "ShardQ-Example-Worker_01", ["--unused"],
                {"SHARDQ_ROOT": "/ledger", "Z_VALUE": "a b"},
            )
            self.assertEqual(handle, "job-a1")
            job_file = root / "workers" / "ShardQ-Example-Worker_01.aihc.json"
            code_dir = root / "workers" / "ShardQ-Example-Worker_01.code"
            payload = json.loads(job_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["name"], "shardq-example-worker-01")
            self.assertEqual(payload["queue"], "queue-id")
            self.assertEqual(payload["jobFramework"], "pytorch")
            self.assertEqual(payload["jobSpec"]["image"], "image:tag")
            self.assertEqual(payload["jobSpec"]["replicas"], 1)
            self.assertEqual(
                payload["jobSpec"]["resources"],
                [
                    {"name": "baidu.com/gpu", "quantity": 1},
                    {"name": "cpu", "quantity": 16},
                    {"name": "memory", "quantity": 128},
                    {"name": "sharedMemory", "quantity": 16},
                ],
            )
            self.assertEqual(payload["jobSpec"]["envs"], [])
            self.assertFalse(payload["jobSpec"]["enableRDMA"])
            self.assertFalse(payload["jobSpec"]["hostNetwork"])
            self.assertEqual(payload["priority"], "low")
            self.assertFalse(payload["faultTolerance"])
            self.assertFalse(payload["enableBccl"])
            self.assertEqual(payload["codeSource"], {"mountPath": "/workspace"})
            self.assertEqual(payload["datasources"], DATASOURCES)
            command = payload["jobSpec"]["command"]
            self.assertIn("timeout -s TERM -k 120 16200 bash -c", command)
            self.assertIn("bash /abs/entry.sh", command)
            self.assertIn("SHARDQ_ROOT=/ledger", command)
            self.assertIn("POOL_ENV=", command)
            self.assertIn("pool value", command)
            train = code_dir / "train.sh"
            self.assertTrue(train.is_file())
            self.assertTrue(os.access(train, os.X_OK))
            self.assertEqual(train.read_text(encoding="utf-8"), f"#!/bin/bash\nset -euo pipefail\n{command}\n")
            self.assertFalse((root / "workers" / "ShardQ-Example-Worker_01.json").exists())
            create_job.assert_called_once_with(
                str(job_file), str(code_dir), "pool-id", "queue-id",
                binary="aihc", config_file="",
            )

    def test_idle_task_drops_priority_and_passes_flag(self) -> None:
        pool = _pool()
        pool.aihc["idle_task"] = True
        with tempfile.TemporaryDirectory() as directory, patch(
            "shardq.launchers.aihc_client.create_job", return_value="job-idle1"
        ) as create_job:
            root = Path(directory)
            handle = AihcLauncher(root, pool, Path("/package")).submit("w-idle", [], {})
            self.assertEqual(handle, "job-idle1")
            job_file = root / "workers" / "w-idle.aihc.json"
            payload = json.loads(job_file.read_text(encoding="utf-8"))
            self.assertNotIn("priority", payload)
            self.assertIs(payload["idleTask"], True)
            create_job.assert_called_once_with(
                str(job_file), str(root / "workers" / "w-idle.code"), "pool-id", "queue-id",
                binary="aihc", config_file="", idle_task=True,
            )

    def test_status_mapping_and_kill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            launcher = AihcLauncher(Path(directory), _pool(), Path("/package"))
            with patch(
                "shardq.launchers.aihc_client.get_job_status",
                side_effect=["Running", "Succeeded", "Scheduled"],
            ):
                self.assertEqual(launcher.status("job-a"), "running")
                self.assertEqual(launcher.status("job-b"), "terminal:Succeeded")
                self.assertEqual(launcher.status("job-c"), "queued")
            with patch("shardq.launchers.aihc_client.stop_job") as stop_job:
                launcher.kill("job-a")
            stop_job.assert_called_once_with(
                "job-a", "pool-id", "queue-id", binary="aihc", config_file=""
            )

    def test_pool_and_registry_accept_aihc(self) -> None:
        pool = _pool()
        self.assertEqual(pool.launcher, "aihc")
        self.assertEqual(pool.aihc["pool_id"], "pool-id")
        self.assertIs(get_launcher("aihc"), AihcLauncher)
        self.assertEqual(get_launcher("aihc").name, "aihc")


if __name__ == "__main__":
    unittest.main()
