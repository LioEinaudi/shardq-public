from __future__ import annotations

import json
import logging
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

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
    def submission_inputs(self):
        return {}

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
    def test_equal_handles_in_two_pools_preserve_both_workers(self) -> None:
        other = _pool("other")
        path = self.root / "workers" / "other-worker.json"
        atomic_write_json(path, {"worker_id": "other-worker", "pool": "other", "launcher": "local",
                                 "handle": "local:1", "last_status": "running"})
        other_stub = _StubLauncher({"local:1": "terminal:0"}, set())
        key = (str(self.root), "local", other.name)
        controller._LAUNCHERS[key] = other_stub
        self.addCleanup(controller._LAUNCHERS.pop, key)
        rows = controller._refresh_workers(self.root, "local", {**self.pools, "other": other}, time.time())
        states = {r["worker_id"]: r["last_status"] for r in rows.values()}
        self.assertEqual(states.get("w-run"), "running")
        self.assertEqual(states.get("other-worker"), "terminal:0")
        self.assertTrue(controller._owner_alive(rows, self.pool.name, {"worker_id": "w-run", "handle": ""}))
        self.assertFalse(controller._owner_alive(rows, "other", {"handle": "local:1"}))

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
        self.assertEqual(set(workers), {"w-dead", "w-run", "w-exit", "w-err"})

        dead = read_json(self.root / "workers" / "w-dead.json")
        self.assertEqual(dead["last_checked_unix"], self.old)
        self.assertEqual(workers["w-dead"]["last_status"], "terminal:Success")

        self.assertEqual(workers["w-run"]["last_status"], "running")
        self.assertFalse(workers["w-run"]["dead"])
        self.assertEqual(workers["w-exit"]["last_status"], "terminal:0")
        self.assertTrue(workers["w-exit"]["dead"])
        for worker_id in ("w-run", "w-exit", "w-err"):
            self.assertEqual(
                read_json(self.root / "workers" / f"{worker_id}.json")["last_checked_unix"],
                now,
            )

    def test_status_error_keeps_previous_status(self) -> None:
        workers = controller._refresh_workers(self.root, "local", self.pools, time.time())
        row = workers["w-err"]
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

    def test_partial_publication_with_uncut_units_still_supplies_work(self) -> None:
        self._configure_shards(lookahead=2)
        self.ledger.set_partition_status(self.job_id, self.partition, "partial")
        state = controller.tick(self.root, "local", self.pools, self.opts, time.time())
        self.assertEqual(state["shards_cut"], 2)
        self.assertGreater(state["workers_submitted"], 0)

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
        self.assertEqual(len(rows), 3)
        confirmed = [row for row in rows if row["submission_state"] == "confirmed"]
        self.assertEqual({row["handle"] for row in confirmed}, {"local:1", "local:3"})
        self.assertEqual(sum(row["submission_state"] == "uncertain" for row in rows), 1)
        self.assertEqual(state["workers_submitted"], 2)
        submissions = [action for action in state["actions"] if action["action"] == "submit"]
        self.assertEqual(len(submissions), 2)
        self.assertEqual({action["worker_id"] for action in submissions}, {row["worker_id"] for row in confirmed})

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
        self.assertEqual(len(rows), 3)
        self.assertEqual({row["handle"] for row in rows if row["submission_state"] == "confirmed"},
                         {"local:1", "local:3"})
        rejected, = [row for row in rows if row["submission_state"] == "rejected"]
        self.assertEqual(state["workers_submitted"], 2)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(logs.records[0].levelname, "WARNING")
        self.assertIsNone(logs.records[0].exc_info)
        self.assertIn("worker submission throttled for pool local-fake: flow control", logs.output[0])

        state = controller.tick(self.root, "local", self.pools, self.opts, now + 1)
        self.assertEqual(len(self.stub.calls), 4)
        self.assertEqual(self.stub.calls[-1][0], rejected["request_id"])
        self.assertEqual(state["workers_submitted"], 1)


class AihcSubmitTest(unittest.TestCase):
    def test_aihc_starts_queued_and_respects_max_queued(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            pool = _pool("local-fake")
            pool.launcher = "aihc"
            pool.aihc = {"gpu_slots": 1, "max_queued": 1}
            pools = {pool.name: pool}
            stub = _SubmitStubLauncher()
            key = (str(root), "aihc", pool.name)
            controller._LAUNCHERS[key] = stub
            try:
                register(
                    root,
                    PROJECT_ROOT / "examples" / "fake-demo.json",
                    sizes={"p0": 40},
                    pools=pools,
                )
                opts = {"max_submits_per_tick": 20}
                first = controller.tick(root, "aihc", pools, opts, time.time())
                self.assertEqual(first["workers_submitted"], 1)
                self.assertEqual(first["workers"][0]["last_status"], "queued")
                second = controller.tick(root, "aihc", pools, opts, time.time())
                self.assertEqual(second["workers_submitted"], 0)
                self.assertEqual(len(stub.calls), 1)
            finally:
                controller._LAUNCHERS.pop(key, None)


class RunSchedulerTests(unittest.TestCase):
    def test_exit_when_idle_drains_replacement_after_late_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = Ledger(root)
            job = ledger.job_dir("late")
            atomic_write_json(job / "job.json", {
                "job_id": "late", "backend": "fake", "pool": "p", "retry": {"lease_ttl_seconds": 5},
            })
            atomic_write_json(job / "partitions/p/partition.json", {"name": "p", "size": 1})
            ledger.cut("late", "p", 1, 1, 10)
            old = ledger.claim("p", [], {"worker_id": "old"}, None, None, 11)
            ledger.expire_leases("late", 5, lambda _: False, 25)
            replacement = ledger.claim("p", [], {"worker_id": "new"}, None, None, 26)
            records = old.attempt_dir / "records.jsonl"
            records.write_text('{"unit_id":0}\n', encoding="utf-8")
            ledger.complete(old, records, 1, 11, 1, {}, 27)
            ledger.set_partition_status("late", "p", "merged")

            with patch.object(controller.time, "time", return_value=28) as clock:
                def advance(seconds):
                    self.assertEqual(clock.return_value, 28)
                    clock.return_value += seconds

                with patch.object(controller.time, "sleep", side_effect=advance) as sleep:
                    code = controller.run_scheduler(root, "local", 10, False, True, {"pools": {"p": _pool()}})
            self.assertEqual(code, 0)
            sleep.assert_called_once_with(10)
            self.assertFalse((replacement.shard_dir / "lease").exists())

    def setUp(self) -> None:
        self.old_handlers = controller.LOGGER.handlers[:]
        self.old_level = controller.LOGGER.level
        self.old_log_root = controller._LOG_ROOT
        controller.LOGGER.handlers = []
        controller._LOG_ROOT = None

    def tearDown(self) -> None:
        for handler in controller.LOGGER.handlers:
            handler.close()
        controller.LOGGER.handlers = self.old_handlers
        controller.LOGGER.setLevel(self.old_level)
        controller._LOG_ROOT = self.old_log_root

    def test_exit_when_idle_with_stopped_unfinished_job(self) -> None:
        from shardq.cli import main

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pools = {"local-fake": _pool("local-fake")}
            register(root, PROJECT_ROOT / "examples" / "fake-demo.json",
                     sizes={"p0": 4}, pools=pools)
            self.assertEqual(main(["stop", "--root", str(root), "--job", "fake-demo"]), 0)
            with patch.object(controller.time, "sleep", side_effect=AssertionError(
                "idle scheduler keeps polling a stopped job"
            )):
                self.assertEqual(controller.run_scheduler(
                    root, "local", 1, False, True, {"pools": pools}
                ), 0)
            job = Ledger(root).scan_job("fake-demo", time.time())
            self.assertTrue(job.stopped)
            self.assertNotIn(job.partitions["p0"].status, {"merged", "partial"})

    def test_exit_when_idle_waits_for_work_workers_leases_and_merges(self) -> None:
        idle = {"jobs": [], "workers": [], "actions": [], "warnings": []}
        stopped = {"stopped": True, "partitions": {"p0": {"status": "cut", "leased": 0}}}
        active = {"stopped": False, "partitions": {"p0": {"status": "cut", "leased": 0}}}
        cases = [
            {"jobs": [stopped, active]},
            {"jobs": [stopped], "merges_in_flight": [{"job_id": "stopped", "partition": "p0"}]},
            {"jobs": [{"stopped": True, "partitions": {"p0": {"status": "cut", "leased": 1}}}]},
        ]
        cases.extend({"jobs": [stopped], "workers": [{"last_status": status}]}
                     for status in ["queued", "running", "unknown"])
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                with patch.object(controller, "tick", side_effect=[dict(idle, **case), idle]) as tick:
                    with patch.object(controller.time, "sleep") as sleep:
                        code = controller.run_scheduler(
                            Path(directory), "local", 1, False, True, {"pools": {}}
                        )
                self.assertEqual(code, 0)
                self.assertEqual(tick.call_count, 2)
                sleep.assert_called_once_with(1)

    def test_exit_when_idle_accepts_merged_and_partial_with_stopped_jobs(self) -> None:
        state = {
            "jobs": [
                {"stopped": False, "partitions": {"p0": {"status": "merged"}, "p1": {"status": "partial"}}},
                {"stopped": True, "partitions": {"old": {"status": "cut"}}},
            ],
            "workers": [{"last_status": "terminal:Succeeded"}], "actions": [], "warnings": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(controller, "tick", return_value=state):
                with patch.object(controller.time, "sleep", side_effect=AssertionError("idle loop")):
                    self.assertEqual(controller.run_scheduler(
                        Path(directory), "local", 1, False, True, {"pools": {}}
                    ), 0)

    def test_run_scheduler_logs_and_continues_after_tick_exception(self) -> None:
        idle_state = {
            "jobs": [], "workers": [], "actions": [], "warnings": [], "merges_in_flight": [],
            "workers_polled": 0, "shards_cut": 0, "workers_submitted": 0, "tick_seconds": 0.0,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            # Configure first so run_scheduler does not replace assertLogs' handler.
            controller._configure_logging(root)
            file_handler = next(
                handler for handler in controller.LOGGER.handlers if isinstance(handler, logging.FileHandler)
            )
            with patch.object(controller, "tick", side_effect=[RuntimeError("boom"), idle_state]) as tick:
                with self.assertLogs(controller.LOGGER, level="ERROR") as logs:
                    # assertLogs replaces handlers; keep the real file output too.
                    controller.LOGGER.addHandler(file_handler)
                    code = controller.run_scheduler(
                        root, "local", 0, False, True,
                        {"pools": {"local-fake": _pool("local-fake")},
                         "max_submits_per_tick": 1, "require_complete": False},
                    )
            self.assertEqual(code, 0)
            self.assertEqual(tick.call_count, 2)
            output = "\n".join(logs.output)
            self.assertIn("tick failed; retrying in 0s", output)
            self.assertIn("RuntimeError: boom", output)
            log_text = (root / "scheduler" / "log" / "scheduler.log").read_text(encoding="utf-8")
            self.assertIn("tick failed; retrying in 0s", log_text)
            self.assertIn("Traceback (most recent call last):", log_text)
            self.assertIn("RuntimeError: boom", log_text)

    def test_run_scheduler_once_reraises_tick_exception(self) -> None:
        error = RuntimeError("boom")
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(controller, "tick", side_effect=error) as tick:
                with self.assertRaises(RuntimeError) as raised:
                    controller.run_scheduler(
                        Path(directory), "local", 0, True, True,
                        {"pools": {"local-fake": _pool("local-fake")},
                         "max_submits_per_tick": 1, "require_complete": False},
                    )
            self.assertIs(raised.exception, error)
            self.assertEqual(tick.call_count, 1)


if __name__ == "__main__":
    unittest.main()
