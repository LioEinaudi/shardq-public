from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from shardq import controller
from shardq.launchers.base import TransientSubmitError
from shardq.ledger import Ledger, atomic_write_json, read_json
from shardq.register import register
from shardq.spec import PoolSpec


PROJECT_ROOT = Path(__file__).resolve().parent.parent


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


class _SubmitStubLauncher:
    def __init__(self) -> None:
        self.calls = []
        self.handles = set()
        self.fail_on = None
        self.submit_error = RuntimeError("submit exploded")
        self.barrier = None
        self._lock = threading.Lock()

    def submit(self, worker_id, worker_argv, env):
        with self._lock:
            self.calls.append((worker_id, worker_argv, env))
            number = len(self.calls)
        if self.barrier is not None:
            self.barrier.wait()
        if number == self.fail_on:
            raise self.submit_error
        handle = f"local:{number}"
        with self._lock:
            self.handles.add(handle)
        return handle

    def status(self, handle: str) -> str:
        return "queued" if handle in self.handles else "unknown"

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


class CutAndSubmitTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve()
        self.pool = _pool("local-fake")
        self.pools = {self.pool.name: self.pool}
        self.stub = _SubmitStubLauncher()
        controller._LAUNCHERS[(str(self.root), "local", self.pool.name)] = self.stub
        spec_path = PROJECT_ROOT / "examples" / "fake-demo.json"
        spec = read_json(spec_path)
        self.job_id = spec["job_id"]
        self.partition = "p0"
        # The demo has two partitions; isolate one to assert per-partition counts.
        register(
            self.root,
            spec_path,
            sizes={self.partition: spec["backend_args"]["partitions"][self.partition]},
            pools=self.pools,
        )
        self.ledger = Ledger(self.root)
        self.opts = {"max_submits_per_tick": 20}

    def tearDown(self) -> None:
        controller._LAUNCHERS.pop((str(self.root), "local", self.pool.name), None)
        self._tmp.cleanup()

    def _configure_shards(self, lookahead: int) -> None:
        path = self.root / "jobs" / self.job_id / "job.json"
        job = read_json(path)
        job["shard"].update({"min_units": 1, "max_units": 1, "lookahead": lookahead})
        atomic_write_json(path, job)

    def test_cut_fills_lookahead_in_one_tick(self) -> None:
        self._configure_shards(lookahead=6)
        self.pool.max_workers = 0
        now = time.time()
        state = controller.tick(self.root, "local", self.pools, self.opts, now)
        cuts = [action for action in state["actions"] if action["action"] == "cut"]
        self.assertEqual(len(cuts), 6)
        self.assertEqual(state["shards_cut"], 6)
        partition = self.ledger.scan_job(self.job_id, now).partitions[self.partition]
        self.assertEqual(partition.size, 40)
        self.assertEqual(partition.pending, 6)
        self.assertEqual(partition.next_cut, 6)
        self.assertEqual([shard.start for shard in partition.shards], list(range(6)))

        state = controller.tick(self.root, "local", self.pools, self.opts, time.time())
        self.assertEqual([action for action in state["actions"] if action["action"] == "cut"], [])
        self.assertEqual(state["shards_cut"], 0)

    def test_cut_respects_max_cuts_per_tick(self) -> None:
        self._configure_shards(lookahead=1000)
        self.pool.max_workers = 0
        self.opts["max_cuts_per_tick"] = 3
        state = controller.tick(self.root, "local", self.pools, self.opts, time.time())
        self.assertEqual(len([action for action in state["actions"] if action["action"] == "cut"]), 3)
        self.assertEqual(state["shards_cut"], 3)

    def test_submit_matches_pending_shards(self) -> None:
        self._configure_shards(lookahead=2)
        self.pool.max_workers = 50
        state = controller.tick(self.root, "local", self.pools, self.opts, time.time())
        self.assertEqual(len(self.stub.calls), 2)
        self.assertEqual(state["workers_submitted"], 2)

        state = controller.tick(self.root, "local", self.pools, self.opts, time.time())
        self.assertEqual(len(self.stub.calls), 2)
        self.assertEqual(state["workers_submitted"], 0)
        self.assertEqual(state["workers_polled"], 2)
        self.assertTrue(all(row["last_status"] == "queued" for row in state["workers"]))

    def test_submit_failure_sets_backoff_and_keeps_successes(self) -> None:
        self._configure_shards(lookahead=3)
        self.stub.fail_on = 2
        # All three calls must overlap, including the one that fails.
        self.stub.barrier = threading.Barrier(3, timeout=5)
        now = time.time()
        with self.assertLogs(controller.LOGGER, level="ERROR") as logs:
            state = controller.tick(self.root, "local", self.pools, self.opts, now)
        self.assertIn("worker submission failed for pool local-fake", logs.output[0])
        self.assertEqual(len(self.stub.calls), 3)
        self.assertGreater(state["pool_backoff"][self.pool.name], now)
        self.assertEqual(
            state["warnings"],
            [f"{self.pool.name}: submit failed: RuntimeError: submit exploded"],
        )
        rows = [read_json(path) for path in (self.root / "workers").glob("*.json")]
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["handle"] for row in rows}, {"local:1", "local:3"})
        self.assertEqual(state["workers_submitted"], 2)
        submissions = [action for action in state["actions"] if action["action"] == "submit"]
        self.assertEqual(len(submissions), 2)
        self.assertEqual(
            [action["worker_id"] for action in submissions],
            sorted(row["worker_id"] for row in rows),
        )

    def test_throttled_submit_does_not_backoff(self) -> None:
        self._configure_shards(lookahead=3)
        self.stub.fail_on = 2
        self.stub.submit_error = TransientSubmitError("flow control")
        now = time.time()
        with self.assertLogs(controller.LOGGER, level="WARNING") as logs:
            state = controller.tick(self.root, "local", self.pools, self.opts, now)
        self.assertEqual(len(self.stub.calls), 3)
        self.assertLessEqual(state["pool_backoff"].get(self.pool.name, 0), now)
        self.assertEqual(state["warnings"], [f"{self.pool.name}: submit throttled: flow control"])
        rows = [read_json(path) for path in (self.root / "workers").glob("*.json")]
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["handle"] for row in rows}, {"local:1", "local:3"})
        self.assertEqual(state["workers_submitted"], 2)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(logs.records[0].levelname, "WARNING")
        self.assertIsNone(logs.records[0].exc_info)
        self.assertIn("worker submission throttled for pool local-fake: flow control", logs.output[0])

        state = controller.tick(self.root, "local", self.pools, self.opts, now + 1)
        self.assertEqual(len(self.stub.calls), 4)
        self.assertEqual(state["workers_submitted"], 1)


if __name__ == "__main__":
    unittest.main()
