from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from shardq.controller import run_scheduler
from shardq.ledger import Ledger
from shardq.register import register
from shardq.spec import PoolSpec
from shardq.worker import run_worker


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _pool() -> PoolSpec:
    return PoolSpec(
        name="local-fake",
        backend="fake",
        entry="",
        launcher="local",
        volc={},
        env={},
        local_python=None,
        max_workers=3,
        worker_max_wall_seconds=60,
        worker_idle_exit_seconds=1,
        extra={"column": "X"},
    )


class FakeEndToEndTests(unittest.TestCase):
    def test_fake_scheduler_merges_and_isolates_poison(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pools = {"local-fake": _pool()}
            register(
                root,
                PROJECT_ROOT / "examples" / "fake-demo.json",
                pools=pools,
            )
            code = run_scheduler(
                root,
                "local",
                1,
                False,
                True,
                {
                    "max_submits_per_tick": 3,
                    "require_complete": False,
                    "pools": pools,
                },
            )
            self.assertEqual(code, 0)
            job = Ledger(root).scan_job("fake-demo", time.time())
            self.assertEqual(job.partitions["p0"].status, "merged")
            self.assertEqual(job.partitions["p1"].status, "partial")
            results = json.loads(
                (
                    root
                    / "jobs/fake-demo/merged/p0/results.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(
                results["results"]["p0"]["mean,none"], 0.5
            )
            merge = json.loads(
                (
                    root / "jobs/fake-demo/merged/p1/merge.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(merge["missing_units"], [7])
            quarantined = []
            shard_root = (
                root / "jobs/fake-demo/partitions/p1/shards"
            )
            for path in shard_root.glob("*/quarantine.json"):
                shard = json.loads(
                    (path.parent / "shard.json").read_text(
                        encoding="utf-8"
                    )
                )
                quarantined.append(shard)
            self.assertTrue(
                any(
                    row["count"] == 1 and row["start"] == 7
                    for row in quarantined
                )
            )

    def test_sigterm_attempt_returns_then_another_worker_completes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spec = root / "preempt.json"
            spec.write_text(
                json.dumps(
                    {
                        "job_id": "preempt-demo",
                        "backend": "fake",
                        "pool": "local-fake",
                        "protocol_id": "x_fake_v1",
                        "backend_args": {
                            "partitions": {"p": 2},
                            "unit_seconds": 1.0,
                        },
                        "shard": {
                            "target_seconds": 2,
                            "min_units": 2,
                            "max_units": 2,
                            "lookahead": 1,
                        },
                        "retry": {
                            "max_errors": 3,
                            "max_preemptions": 20,
                            "backoff_seconds": 0,
                            "bisect": True,
                            "lease_ttl_seconds": 2,
                        },
                    }
                ),
                encoding="utf-8",
            )
            register(root, spec, pools={"local-fake": _pool()})
            ledger = Ledger(root)
            ledger.cut("preempt-demo", "p", 2, 2.0, time.time())
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "shardq",
                    "worker",
                    "--root",
                    str(root),
                    "--pool",
                    "local-fake",
                    "--worker-id",
                    "victim",
                    "--job",
                    "preempt-demo",
                    "--max-wall-seconds",
                    "30",
                    "--idle-exit-seconds",
                    "1",
                ],
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            worker_file = root / "workers/victim.json"
            worker_file.parent.mkdir(parents=True, exist_ok=True)
            worker_file.write_text(
                json.dumps(
                    {
                        "worker_id": "victim",
                        "pool": "local-fake",
                        "launcher": "local",
                        "handle": f"local:{process.pid}",
                    }
                ),
                encoding="utf-8",
            )
            deadline = time.time() + 10
            lease = None
            shard_root = (
                root / "jobs/preempt-demo/partitions/p/shards"
            )
            while lease is None and time.time() < deadline:
                leases = list(shard_root.glob("*/lease"))
                lease = leases[0] if leases else None
                time.sleep(0.05)
            self.assertIsNotNone(lease)
            self.assertTrue(lease.is_dir())
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=10)
            failures = list(
                shard_root.glob("*/attempts/*/failed.json")
            )
            self.assertTrue(failures)
            self.assertEqual(
                json.loads(
                    failures[0].read_text(encoding="utf-8")
                )["reason"],
                "preempted",
            )
            if process.stdout is not None:
                process.stdout.close()
            replacement = root / "workers/replacement.json"
            replacement.write_text(
                json.dumps(
                    {
                        "worker_id": "replacement",
                        "pool": "local-fake",
                        "launcher": "local",
                        "handle": f"local:{os.getpid()}",
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                run_worker(
                    root,
                    "local-fake",
                    "replacement",
                    30,
                    1,
                    heartbeat_seconds=1,
                    job_ids=["preempt-demo"],
                ),
                0,
            )
            partition = ledger.scan_job(
                "preempt-demo", time.time()
            ).partitions["p"]
            self.assertEqual(partition.done, 1)


if __name__ == "__main__":
    unittest.main()
