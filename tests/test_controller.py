from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from shardq import controller
from shardq.ledger import atomic_write_json, read_json
from shardq.spec import PoolSpec


def _pool(name: str = "p") -> PoolSpec:
    return PoolSpec(
        name=name,
        backend="fake",
        entry="",
        launcher="local",
        volc={},
        env={},
        local_python=None,
        max_workers=4,
        worker_max_wall_seconds=60,
        worker_idle_exit_seconds=5,
        extra={},
    )


class _StubLauncher:
    def __init__(self, statuses: dict, fail: set) -> None:
        self.statuses = statuses
        self.fail = fail
        self.calls = []

    def status(self, handle: str) -> str:
        self.calls.append(handle)
        if handle in self.fail:
            raise RuntimeError("cli exploded")
        return self.statuses[handle]

    def capacity_slots(self):
        return None


class RefreshWorkersTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve()
        self.pool = _pool()
        self.pools = {self.pool.name: self.pool}
        self.stub = _StubLauncher(
            {"local:1": "running", "local:2": "terminal:0", "local:3": "running"},
            fail={"local:3"},
        )
        controller._LAUNCHERS[(str(self.root), "local", self.pool.name)] = self.stub
        self.old = time.time() - 3600
        rows = {
            "w-dead": {"handle": "local:9", "last_status": "terminal:Success", "dead": True},
            "w-run": {"handle": "local:1", "last_status": "queued", "dead": False},
            "w-exit": {"handle": "local:2", "last_status": "running", "dead": False},
            "w-err": {"handle": "local:3", "last_status": "running", "dead": False},
        }
        for worker_id, row in rows.items():
            row.update(
                {
                    "worker_id": worker_id,
                    "pool": self.pool.name,
                    "launcher": "local",
                    "last_checked_unix": self.old,
                }
            )
            atomic_write_json(self.root / "workers" / f"{worker_id}.json", row)

    def tearDown(self) -> None:
        controller._LAUNCHERS.pop((str(self.root), "local", self.pool.name), None)
        self._tmp.cleanup()

    def test_dead_workers_are_not_polled_and_live_ones_are_updated(self) -> None:
        now = time.time()
        workers = controller._refresh_workers(self.root, "local", self.pools, now)
        self.assertEqual(sorted(self.stub.calls), ["local:1", "local:2", "local:3"])
        self.assertEqual(set(workers), {"local:9", "local:1", "local:2", "local:3"})

        dead = read_json(self.root / "workers" / "w-dead.json")
        self.assertEqual(dead["last_checked_unix"], self.old)
        self.assertEqual(workers["local:9"]["last_status"], "terminal:Success")

        self.assertEqual(workers["local:1"]["last_status"], "running")
        self.assertFalse(workers["local:1"]["dead"])
        self.assertEqual(workers["local:2"]["last_status"], "terminal:0")
        self.assertTrue(workers["local:2"]["dead"])
        for worker_id in ("w-run", "w-exit", "w-err"):
            self.assertEqual(
                read_json(self.root / "workers" / f"{worker_id}.json")["last_checked_unix"],
                now,
            )

    def test_status_error_keeps_previous_status(self) -> None:
        workers = controller._refresh_workers(self.root, "local", self.pools, time.time())
        row = workers["local:3"]
        self.assertEqual(row["last_status"], "running")
        self.assertFalse(row["dead"])
        self.assertIn("cli exploded", row["status_error"])
        on_disk = json.loads((self.root / "workers" / "w-err.json").read_text())
        self.assertIn("status_error", on_disk)


if __name__ == "__main__":
    unittest.main()
