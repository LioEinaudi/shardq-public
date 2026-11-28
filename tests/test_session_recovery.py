import fcntl
import io
import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

from shardq import cli, controller, ledger as ledger_module, worker
from shardq.backends.fake import FakeBackend
from shardq.ledger import Ledger, atomic_write_json, read_json
from shardq.register import register
from shardq.spec import pool_from_dict
from shardq.status import render_status


class SessionRecoveryTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = Ledger(self.root)
        pool = pool_from_dict("p", {"launcher": "local", "backend": "fake"})
        self.pools = {"p": pool}
        self.launcher = Mock()
        self.launcher.submission_inputs.return_value = {}
        self.launcher.status.return_value = "terminal:exit"
        self.launcher.capacity_slots.return_value = None
        self.launcher.submit.side_effect = lambda worker_id, *args: worker_id
        self.launcher_patch = patch.object(controller, "_launcher", return_value=self.launcher)
        self.launcher_patch.start()
        self.addCleanup(self.launcher_patch.stop)

    def add_job(self, job_id="broken", *, partitions=None, retry=None):
        partitions = partitions or {"a": 2, "b": 2}
        spec = {
            "job_id": job_id, "pool": "p", "backend": "fake", "protocol_id": "demo",
            "backend_args": {"unit_seconds": 0},
            "priority": 10 if job_id == "broken" else 0,
            "shard": {"min_units": 1, "max_units": 2},
            "retry": {"max_errors": 1, "backoff_seconds": 10, **(retry or {})},
        }
        path = self.root / (job_id + ".json")
        atomic_write_json(path, spec)
        register(self.root, path, pools=self.pools, sizes=partitions)
        for name, size in partitions.items():
            self.ledger.cut(job_id, name, size, 1, 0)

    def run_worker(self, name, now, jobs=None):
        atomic_write_json(self.root / "workers" / (name + ".json"), {
            "worker_id": name, "pool": "p", "launcher": "local", "handle": name,
        })
        with patch("time.time", return_value=now):
            return worker.run_worker(self.root, "p", name, 1000, 0, job_ids=jobs)

    def tick(self, now):
        # Fresh scheduler options on every tick discard all process-local state.
        return controller.tick(self.root, "local", self.pools, {}, now)

    def resume(self):
        with redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(["resume", "--root", str(self.root), "--job", "broken"]), 0)

    def test_setup_failure_never_splits_or_quarantines(self):
        self.add_job(partitions={"a": 2, "b": 1})
        with patch.object(FakeBackend, "open_session", side_effect=RuntimeError("missing model")):
            self.assertEqual(self.run_worker("w", 100), 2)
        state = self.tick(101)
        self.assertFalse({"split", "quarantine"} & {a["action"] for a in state["actions"]})
        job = self.ledger.scan_job("broken", 101)
        self.assertTrue(all(shard.errors == 0 for p in job.partitions.values() for shard in p.shards))
        failure, = self.root.glob("jobs/broken/partitions/*/shards/*/attempts/*/failed.json")
        self.assertIn("missing model", read_json(failure)["error"])

    def test_backoff_blocks_other_shards_and_worker_replacement_on_restart(self):
        self.add_job(retry={"max_errors": 10})
        with patch.object(FakeBackend, "open_session", side_effect=ValueError("bad env")):
            self.assertEqual(self.run_worker("first", 100), 2)
        self.assertEqual(self.tick(101)["workers_submitted"], 0)
        self.assertIsNone(Ledger(self.root).claim("p", [], {"worker_id": "new"}, None, None, 109))
        self.assertEqual(self.tick(110)["workers_submitted"], 1)

    def test_stale_selection_cannot_claim_after_job_backoff_or_pause(self):
        for limit, status in ((3, "backoff"), (1, "paused")):
            job_id = "blocked-" + status
            with self.subTest(status=status):
                self.add_job(job_id, retry={"max_session_errors": limit})
                scan = self.ledger.scan_job
                injected = False

                def scan_then_fail(selected_job, now):
                    nonlocal injected
                    old = scan(selected_job, now)
                    if not injected:
                        injected = True
                        other = Ledger(self.root)
                        claim = other.claim("p", [job_id], {"worker_id": "other"}, None, None, now)
                        other.fail(claim, "session_error", True, "setup failed", 10, now)
                    return old

                with patch.object(self.ledger, "scan_job", side_effect=scan_then_fail):
                    claim = self.ledger.claim("p", [job_id], {"worker_id": "stale"}, None, None, 100)
                self.assertIsNone(claim)
                self.assertEqual(scan(job_id, 100).setup_status, status)
                self.assertEqual(list(self.ledger.job_dir(job_id).glob("**/attempts/stale-*")), [])

    def test_session_failure_and_claim_publication_are_serialized(self):
        self.add_job(retry={"max_session_errors": 1})
        old = self.ledger.claim("p", ["broken"], {"worker_id": "old"}, None, None, 100)
        progressed = threading.Event()
        main = threading.current_thread()
        flock = fcntl.flock
        symlink = ledger_module.os.symlink
        events = []

        def fail():
            self.ledger.fail(old, "session_error", True, "setup failed", 10, 101)
            events.append("failure")

        def watch_contention(fd, flags):
            if threading.current_thread() is not main and flags == fcntl.LOCK_EX:
                try:
                    return flock(fd, flags | fcntl.LOCK_NB)
                except BlockingIOError:
                    progressed.set()
            return flock(fd, flags)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = None

            def publish(*args, **kwargs):
                nonlocal future
                future = executor.submit(fail)
                future.add_done_callback(lambda finished: progressed.set())
                self.assertTrue(progressed.wait(timeout=5))
                symlink(*args, **kwargs)
                events.append("lease")

            with patch.object(ledger_module.os, "symlink", publish), patch.object(fcntl, "flock", watch_contention):
                claim = self.ledger.claim("p", ["broken"], {"worker_id": "new"}, None, None, 101)
                self.assertIsNotNone(claim)
                future.result(timeout=5)
        self.assertEqual(events, ["lease", "failure"])
        self.assertEqual(self.ledger.scan_job("broken", 101).setup_status, "paused")

    def test_pause_is_persistent_and_job_local_then_resume_recovers(self):
        self.add_job(retry={"max_errors": 20, "max_session_errors": 2})
        with patch.object(FakeBackend, "open_session", side_effect=ValueError("bad env")):
            self.assertEqual(self.run_worker("one", 100), 2)
            self.assertEqual(self.run_worker("two", 120), 2)
        self.assertEqual(self.tick(150)["workers_submitted"], 0)
        self.assertEqual(self.tick(1000)["workers_submitted"], 0)
        status = json.loads(render_status(self.root, "broken", True))["jobs"][0]
        self.assertEqual(status["setup_status"], "paused")
        self.assertIn("paused", render_status(self.root, "broken", False))
        self.add_job("healthy", partitions={"a": 2})
        self.assertEqual(self.tick(1001)["workers_submitted"], 1)
        self.assertEqual(self.run_worker("shared", 1010), 0)
        self.assertEqual(self.ledger.scan_job("healthy", 1011).partitions["a"].done_units, 2)
        self.assertEqual(list(self.root.glob("jobs/broken/**/complete.json")), [])
        self.resume()
        self.assertEqual(self.run_worker("repaired", 1012, ["broken"]), 0)
        job = self.ledger.scan_job("broken", 1013)
        self.assertEqual(sum(p.done_units for p in job.partitions.values()), 4)
        self.assertEqual(len(list(self.root.glob("jobs/broken/**/failed.json"))), 2)

    def test_legacy_failure_and_late_failure_after_resume(self):
        self.add_job(retry={"max_errors": 20, "max_session_errors": 1})
        a = self.ledger.claim("p", [], {"worker_id": "a"}, None, None, 100)
        b = self.ledger.claim("p", [], {"worker_id": "b"}, None, None, 100)
        self.ledger.fail(a, "session_error", True, "old worker", 10, 101)
        failure_file = a.attempt_dir / "failed.json"
        legacy = read_json(failure_file)
        legacy.pop("setup_epoch", None)
        atomic_write_json(failure_file, legacy)
        self.assertIsNone(self.ledger.claim("p", [], {"worker_id": "c"}, None, None, 120))
        self.resume()
        self.ledger.fail(b, "session_error", True, "from before resume", 10, 121)
        self.assertEqual(self.ledger.scan_job("broken", 122).session_errors, 0)
        self.assertIsNotNone(self.ledger.claim("p", [], {"worker_id": "d"}, None, None, 122))

    def test_new_scheduler_processes_rebuild_backoff_and_pause_from_old_job(self):
        self.add_job(retry={"max_errors": 20})
        job_file = self.root / "jobs" / "broken" / "job.json"
        old_spec = read_json(job_file)
        old_spec["retry"].pop("max_session_errors", None)
        atomic_write_json(job_file, old_spec)
        atomic_write_json(self.root / "pools.json", {"pools": {"p": {"launcher": "local", "backend": "fake"}}})
        now = time.time()
        for index, expected in enumerate(("backoff", "backoff", "paused")):
            attempt_time = now + index * 301
            claim = self.ledger.claim("p", [], {"worker_id": str(index)}, None, None, attempt_time)
            self.ledger.fail(claim, "session_error", True, "setup failed", 300, attempt_time)
            # Each invocation starts a new interpreter. Detect submissions without
            # launching any workers, including when this test fails on regression.
            script = (
                "import sys\nfrom unittest.mock import patch\nfrom shardq.cli import main\n"
                "with patch('shardq.launchers.local.LocalLauncher.submit', return_value='local:1') as submit:\n"
                "    code = main(sys.argv[1:])\n    submit.assert_not_called()\n    sys.exit(code)\n"
            )
            result = subprocess.run(
                [sys.executable, "-c", script, "scheduler", "--root", str(self.root),
                 "--launcher", "local", "--exit-when-idle" if expected == "paused" else "--once"],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            saved = read_json(self.root / "scheduler" / "state.json")
            self.assertEqual(saved["jobs"][0]["setup_status"], expected)
            self.assertEqual(saved["jobs"][0]["session_errors"], index + 1)

    def test_backend_constructor_failure_is_audited_and_bounded(self):
        self.add_job(retry={"max_session_errors": 1})
        with patch.object(FakeBackend, "__init__", side_effect=ValueError("bad runtime")):
            self.assertEqual(self.run_worker("w", 100), 2)
        failure, = self.root.glob("jobs/broken/**/failed.json")
        self.assertEqual(read_json(failure)["reason"], "session_error")
        self.assertEqual(self.tick(130)["workers_submitted"], 0)

    def test_resume_between_selection_and_claim_fences_old_selection(self):
        self.add_job()
        scan = self.ledger.scan_job
        initial_scan = True

        def scan_then_resume(job_id, now):
            nonlocal initial_scan
            old = scan(job_id, now)
            if initial_scan:
                initial_scan = False
                self.resume()
            return old

        with patch.object(self.ledger, "scan_job", side_effect=scan_then_resume):
            claim = self.ledger.claim("p", ["broken"], {"worker_id": "old"}, None, None, 101)
        self.assertIsNone(claim)
        fresh = self.ledger.claim("p", ["broken"], {"worker_id": "new"}, None, None, 102)
        self.assertEqual(read_json(fresh.attempt_dir / "owner.json")["setup_epoch"], scan("broken", 102).setup_epoch)
