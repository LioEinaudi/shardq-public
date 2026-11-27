import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from shardq import controller
from shardq.ledger import Ledger, atomic_write_json, read_json
from shardq.spec import load_pools


class SetupDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = Ledger(self.root)
        pools_file = self.root / "pools.json"
        atomic_write_json(pools_file, {"pools": {"p": {"launcher": "local", "backend": "fake"}}})
        self.pools = load_pools(pools_file)

    def add_job(self, job_id, **retry):
        job = self.ledger.job_dir(job_id)
        atomic_write_json(job / "job.json", {"job_id": job_id, "backend": "fake", "pool": "p", "retry": retry})
        partition = job / "partitions" / "units"
        atomic_write_json(partition / "partition.json", {"name": "units", "size": 1})
        shard_id = self.ledger.cut(job_id, "units", 1, 1, 10)
        return partition / "shards" / shard_id

    def setup_failure(self, job_id, now):
        claim = self.ledger.claim("p", [job_id], {"worker_id": "worker"}, None, None, now)
        self.ledger.fail(claim, "session_error", True, "model missing", 30, now)

    def add_finished_legacy_job(self, job_id, status, max_session_errors=1):
        shard = self.add_job(job_id, max_session_errors=max_session_errors)
        self.setup_failure(job_id, 100)
        # Older ledgers can have successful work after setup failures exhausted
        # the job-wide budget introduced by this version.
        atomic_write_json(shard / "attempts" / "success" / "complete.json",
                          {"count": 1, "elapsed_seconds": 1, "completed_unix": 101})
        self.ledger.update_partition(shard.parent.parent / "partition.json", status=status)

    def tick(self, now):
        controller.tick(self.root, "local", self.pools,
                        {"max_submits_per_tick": 0, "require_complete": True}, now)
        return read_json(self.root / "scheduler" / "state.json")

    def test_quarantine_reports_the_latest_budgeted_failure(self):
        shard = self.add_job("mixed", max_errors=2)
        # Replay an existing shard with mixed attempt history, including a
        # later worker process restart for a change of lmms source.
        for stamp, reason in [(10, "backend_error"), (40, "worker_restart"), (50, "session_error")]:
            atomic_write_json(shard / "attempts" / str(stamp) / "failed.json",
                              {"reason": reason, "failed_unix": stamp})
        self.tick(60)
        self.assertFalse((shard / "quarantine.json").exists())
        atomic_write_json(shard / "attempts" / "30" / "failed.json",
                          {"reason": "validation_error", "failed_unix": 30})
        self.tick(61)
        quarantined = read_json(shard / "quarantine.json")
        self.assertEqual((quarantined["reason"], quarantined["errors"], quarantined["preemptions"]),
                         ("validation_error", 2, 0))

    def test_setup_warnings_follow_the_current_job_epoch(self):
        self.add_job("load-model", max_session_errors=2)
        self.setup_failure("load-model", 100)
        for now in (105, 129):
            state = self.tick(now)
            self.assertEqual(state["jobs"][0]["setup_status"], "backoff")
            warning, = state["warnings"]
            self.assertIn("load-model", warning)
            self.assertIn("130", warning)
        self.assertEqual(self.tick(130)["warnings"], [])
        self.setup_failure("load-model", 131)
        paused = self.tick(200)
        self.assertEqual(paused["jobs"][0]["setup_status"], "paused")
        warning, = paused["warnings"]
        self.assertIn("shardq resume --job load-model", warning)
        self.ledger.resume("load-model")
        self.assertEqual(self.tick(201)["warnings"], [])

    def test_idle_exit_names_only_jobs_paused_by_setup(self):
        for job_id in ("load-model", "load-tokenizer"):
            self.add_job(job_id, max_session_errors=1)
            self.setup_failure(job_id, 100)
        self.add_job("manual-stop")
        (self.ledger.job_dir("manual-stop") / "STOP").touch()
        self.add_finished_legacy_job("old-merged", "merged")
        result = subprocess.run([sys.executable, "-m", "shardq", "scheduler", "--root", str(self.root),
                                 "--launcher", "local", "--exit-when-idle"],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        exits = [line for line in result.stderr.splitlines() if "exit" in line.lower()]
        self.assertEqual(len(exits), 1, result.stderr)
        self.assertIn("paused", exits[0])
        for job_id in ("load-model", "load-tokenizer"):
            self.assertIn(job_id, exits[0])
        self.assertNotIn("manual-stop", exits[0])
        self.assertNotIn("old-merged", exits[0])

    def test_finished_jobs_do_not_warn_about_historical_setup_failures(self):
        for status in ("merged", "partial"):
            for limit in (1, 2):
                self.add_finished_legacy_job(f"old-{status}-{limit}", status, limit)
        state = self.tick(105)
        self.assertEqual({job["setup_status"] for job in state["jobs"]}, {"backoff", "paused"})
        self.assertEqual(state["warnings"], [])
