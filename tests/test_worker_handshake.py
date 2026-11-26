from __future__ import annotations

import multiprocessing
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from shardq.backends.fake import FakeBackend
from shardq.cli import main
from shardq.launchers.local import LocalLauncher
from shardq.ledger import Ledger, atomic_write_json, read_json
from shardq.spec import pool_from_dict
from shardq.worker import run_worker


def _run_early_worker(root, worker_id, now, entered, renewed, finish):
    open_session = FakeBackend.open_session
    heartbeat = Ledger.heartbeat

    def hold_session(backend):
        entered.set()
        if not finish.wait(15):
            raise RuntimeError("test platform did not release worker")
        return open_session(backend)

    def observe_heartbeat(ledger, claim, timestamp):
        heartbeat(ledger, claim, timestamp)
        if timestamp >= 110:
            renewed.set()

    with patch.dict(os.environ, {"SHARDQ_REQUEST_ID": worker_id, "SHARDQ_HANDLE": "stale-inherited"}), \
         patch("time.time", side_effect=lambda: now.value), \
         patch.object(FakeBackend, "open_session", hold_session), \
         patch.object(Ledger, "heartbeat", observe_heartbeat):
        raise SystemExit(run_worker(root, "pool", worker_id, 60, 0, heartbeat_seconds=0.02))


class WorkerHandshakeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.ledger = Ledger(self.root)
        job = self.ledger.job_dir("job")
        atomic_write_json(job / "job.json", {
            "job_id": "job", "pool": "pool", "backend": "fake",
            "pool_snapshot": {"launcher": "volc", "backend": "fake"},
            "backend_args": {"unit_seconds": 0}, "retry": {"lease_ttl_seconds": 2},
        })
        atomic_write_json(job / "partitions/p/partition.json", {"name": "p", "size": 2})

    def worker_record(self, worker_id):
        return self.root / "workers" / f"{worker_id}.json"

    def submitting(self, worker_id):
        row = {"worker_id": worker_id, "request_id": worker_id, "pool": "pool",
               "launcher": "volc", "submission_state": "submitting"}
        atomic_write_json(self.worker_record(worker_id), row)
        return row

    def test_two_workers_renew_before_submit_returns(self):
        ctx = multiprocessing.get_context("spawn")
        now = ctx.Value("d", 100)
        acknowledge, finish = ctx.Event(), ctx.Event()
        processes = []
        entered, renewed = {}, {}
        workers = ("early-1", "early-2")
        pending = {worker_id: self.submitting(worker_id) for worker_id in workers}
        for worker_id in workers:
            self.ledger.cut("job", "p", 1, 1, 100)
            entered[worker_id], renewed[worker_id] = ctx.Event(), ctx.Event()

        def platform_submit(worker_id):
            process = ctx.Process(target=_run_early_worker, args=(
                self.root, worker_id, now, entered[worker_id], renewed[worker_id], finish,
            ))
            process.start()
            processes.append(process)
            if not acknowledge.wait(15):
                raise RuntimeError("missing submit acknowledgement")
            return f"platform-{worker_id}"

        with ThreadPoolExecutor(2) as executor:
            submits = {w: executor.submit(platform_submit, w) for w in workers}
            try:
                for w in workers:
                    self.assertTrue(entered[w].wait(10))
                now.value = 110  # Both workers outlive the former five-second handle wait.
                for w in workers:
                    self.assertTrue(renewed[w].wait(10))
                    self.assertFalse(submits[w].done())
                    self.assertEqual(read_json(self.worker_record(w)), pending[w])
                    receipt, = (self.root / "workers" / f"{w}.starts").glob("*.json")
                    self.assertEqual(read_json(receipt)["request_id"], w)
                    self.assertNotIn("local_handle", read_json(receipt))
                owners = [r.lease_owner for r in self.ledger.scan_job("job", 110).partitions["p"].shards]
                self.assertEqual({o["worker_id"] for o in owners}, set(workers))
                self.assertTrue(all(o["handle"] == "" for o in owners))
                self.assertEqual(self.ledger.expire_leases("job", 2, lambda _: False, 110), [])
                acknowledge.set()
                for w in workers:
                    atomic_write_json(self.worker_record(w), {
                        **pending[w], "submission_state": "confirmed", "handle": submits[w].result(10),
                    })
                for o in owners:
                    self.assertEqual(read_json(self.worker_record(o["worker_id"]))["handle"],
                                     f"platform-{o['worker_id']}")
            finally:
                acknowledge.set()
                finish.set()
                for process in processes:
                    process.join(10)
                    if process.is_alive():
                        process.kill()
                        process.join(5)
        self.assertEqual([p.exitcode for p in processes], [0, 0])
        self.assertEqual(self.ledger.scan_job("job", 110).partitions["p"].done, 2)

    def test_direct_worker_without_scheduler_record_starts_immediately(self):
        self.ledger.cut("job", "p", 1, 1, 100)

        def sample_sleep(seconds):
            self.assertEqual(seconds, 0, "direct worker waited for a scheduler record")

        with patch.dict(os.environ, {"SHARDQ_REQUEST_ID": ""}), patch("time.sleep", sample_sleep):
            self.assertEqual(run_worker(self.root, "pool", "direct", 30, 0), 0)
        self.assertFalse(self.worker_record("direct").exists())
        self.assertFalse((self.root / "workers/direct.starts").exists())
        self.assertEqual(self.ledger.scan_job("job", 110).partitions["p"].done, 1)

    def test_managed_cli_uses_request_id_when_entry_omits_worker_id(self):
        self.submitting("early")
        self.ledger.cut("job", "p", 1, 1, 100)
        with patch.dict(os.environ, {"SHARDQ_REQUEST_ID": "early"}):
            self.assertEqual(main(["worker", "--root", str(self.root), "--pool", "pool",
                                   "--max-wall-seconds", "60", "--idle-exit-seconds", "1"]), 0)
        receipt, = (self.root / "workers/early.starts").glob("*.json")
        self.assertEqual(read_json(receipt)["worker_id"], "early")
        self.assertEqual(self.ledger.scan_job("job", 110).partitions["p"].done, 1)

    def test_local_worker_receipt_matches_submitted_process_after_exit(self):
        config = {"launcher": "local", "backend": "fake"}
        atomic_write_json(self.root / "pools.json", {"pools": {"pool": config}})
        w = "local-worker"
        row = self.submitting(w)
        atomic_write_json(self.worker_record(w), {**row, "launcher": "local"})
        pool = pool_from_dict("pool", config)
        repo = Path(__file__).resolve().parents[1]
        launcher = LocalLauncher(self.root, pool, repo)
        handle = launcher.submit(w, ["--root", str(self.root), "--pool", "pool", "--worker-id", w,
                                     "--max-wall-seconds", "60", "--idle-exit-seconds", "1"],
                                 {"SHARDQ_REQUEST_ID": w})
        child, = launcher.processes.values()
        try:
            self.assertEqual(child.wait(10), 0)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(5)
        restarted = LocalLauncher(self.root, pool, repo)
        self.assertEqual(restarted.lookup_submission(w), [handle])
        self.assertTrue(restarted.status(handle).startswith("terminal:"))

    def test_mismatched_request_cannot_claim(self):
        self.ledger.cut("job", "p", 1, 1, 100)
        self.submitting("managed")
        with patch.dict(os.environ, {"SHARDQ_REQUEST_ID": "different"}):
            with self.assertRaisesRegex(ValueError, "request"):
                run_worker(self.root, "pool", "managed", 60, 0)
        self.assertEqual(self.ledger.scan_job("job", 110).partitions["p"].pending, 1)

    def test_late_start_records_receipt_without_claiming_after_resolution(self):
        self.ledger.cut("job", "p", 1, 1, 100)
        for state in ("intent", "conflict", "rejected", "terminal"):
            with self.subTest(state=state):
                row = self.submitting(state)
                atomic_write_json(self.worker_record(state), {**row, "submission_state": state})
                with patch.dict(os.environ, {"SHARDQ_REQUEST_ID": state}):
                    self.assertEqual(run_worker(self.root, "pool", state, 30, 0), 0)
                self.assertEqual(self.ledger.scan_job("job", 110).partitions["p"].pending, 1)
                receipt, = (self.root / "workers" / f"{state}.starts").glob("*.json")
                self.assertEqual(read_json(receipt)["worker_id"], state)

    def test_worker_finishes_current_range_then_observes_conflict(self):
        for _ in range(2):
            self.ledger.cut("job", "p", 1, 1, 100)
        row = self.submitting("already-running")
        validate = FakeBackend.validate_range

        def detect_conflict(backend, *args):
            validate(backend, *args)
            atomic_write_json(self.worker_record("already-running"), {**row, "submission_state": "conflict"})

        with patch.dict(os.environ, {"SHARDQ_REQUEST_ID": "already-running"}), \
             patch.object(FakeBackend, "validate_range", detect_conflict):
            self.assertEqual(run_worker(self.root, "pool", "already-running", 30, 0), 0)
        partition = self.ledger.scan_job("job", 110).partitions["p"]
        self.assertEqual((partition.done, partition.pending), (1, 1))
