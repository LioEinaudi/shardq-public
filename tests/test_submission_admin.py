from __future__ import annotations

import fcntl
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from shardq.cli import main
from shardq.ledger import atomic_write_json, read_json
from shardq.status import render_status
from shardq.submission import occupies


class SubmissionAdminTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def record(self, worker="w", **updates):
        path = self.root / "workers" / f"{worker}.json"
        value = {"worker_id": worker, "request_id": worker, "pool": "p", "launcher": "volc",
                 "submission_state": "uncertain", "last_status": "uncertain", "dead": False,
                 "submission_error": "platform lookup timed out", "reconcile_after_unix": 123,
                 "request_fingerprint": "original", **updates}
        atomic_write_json(path, value)
        return path

    def resolve(self, *args):
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return main(["resolve-submission", "--root", str(self.root), "--worker-id", "w", *args])

    def test_conflict_binding_keeps_audit_and_pending_capacity(self):
        path = self.record(submission_state="conflict", match_handles=["h1", "h2"],
                           resolutions=[{"action": "previous"}])
        self.record("different-pool", pool="other", submission_state="confirmed", handle="h2")
        self.assertEqual(occupies(read_json(path)), 2)
        self.assertEqual(self.resolve("--handle", "h2", "--evidence", "h2 is running"), 2)
        self.assertEqual(occupies(read_json(path)), 2)
        self.assertEqual(self.resolve("--handle", "h2", "--terminal-match", "h1",
                                      "--evidence", "h1 terminated; h2 running"), 0)
        row = read_json(path)
        self.assertEqual(occupies(row), 1)
        self.assertEqual((row["submission_state"], row["handle"], row["last_status"], row["dead"]),
                         ("confirmed", "h2", "unknown", False))
        self.assertEqual(row["request_fingerprint"], "original")
        previous, entry = row["resolutions"]
        self.assertEqual(previous, {"action": "previous"})
        self.assertEqual(entry["previous_state"], "conflict")
        self.assertEqual(entry["match_handles"], ["h1", "h2"])
        self.assertEqual(entry["terminal_matches"], ["h1"])
        self.assertEqual(entry["evidence"], "h1 terminated; h2 running")
        self.assertEqual(entry["handle"], "h2")
        self.assertGreater(entry["resolved_unix"], 0)
        self.assertGreater(self.resolve("--handle", "h3", "--evidence", "repeat"), 0)

    def test_evidence_for_releasing_reservation_is_persisted(self):
        for choice, expected in (("--not-created", "rejected"), ("--terminal", "terminal")):
            with self.subTest(choice=choice):
                path = self.record()
                self.assertEqual(self.resolve(choice, "--evidence", "   "), 2)
                self.assertEqual(self.resolve(choice, "--evidence", "provider audit ticket 123"), 0)
                row = read_json(path)
                self.assertEqual(row["submission_state"], expected)
                self.assertTrue(row["last_status"].startswith("terminal:"))
                self.assertTrue(row["dead"])
                self.assertEqual(row["resolutions"][0]["evidence"], "provider audit ticket 123")

    def test_conflicting_handle_active_scheduler_and_foreign_identity_are_refused(self):
        path = self.record()
        other = self.record("other", handle="claimed", submission_state="confirmed")
        self.assertEqual(self.resolve("--handle", "claimed", "--evidence", "operator check"), 2)
        self.assertEqual(read_json(path)["submission_state"], "uncertain")
        before = path.read_bytes()
        scheduler = self.root / "scheduler"
        scheduler.mkdir(exist_ok=True)
        with (scheduler / "lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.assertEqual(self.resolve("--terminal", "--evidence", "provider history"), 2)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(read_json(other)["submission_state"], "confirmed")
        self.record(request_id="another")
        self.assertEqual(self.resolve("--not-created", "--evidence", "bad identity"), 2)

    def test_status_displays_diagnostics_and_started_count_once(self):
        self.record()
        receipt = {"worker_id": "w", "request_id": "w", "pool": "p", "launcher": "volc"}
        atomic_write_json(self.root / "workers/w.starts/first.json", receipt)
        atomic_write_json(self.root / "workers/w.aihc.json", {"worker_id": "w", "launcher": "volc"})
        worker, = json.loads(render_status(self.root, None, True))["workers"]
        self.assertEqual(worker["started_count"], 1)
        self.assertEqual(worker["submission_state"], "uncertain")
        self.assertEqual(worker["reconcile_after_unix"], 123)
        rendered = render_status(self.root, None, False)
        self.assertIn("platform lookup timed out", rendered)
        self.assertIn("123", rendered)
        self.assertIn("started_count", rendered)

    def test_started_worker_cannot_be_declared_not_created(self):
        path = self.record()
        atomic_write_json(self.root / "workers/w.starts/r.json", {"request_id": "w"})
        self.assertEqual(self.resolve("--not-created", "--evidence", "platform list is empty"), 2)
        self.assertEqual(read_json(path)["submission_state"], "uncertain")

    def test_handle_matched_by_another_unresolved_request_is_reserved(self):
        path = self.record()
        self.record("other", submission_state="conflict", match_handles=["h1", "h2"])
        self.assertEqual(self.resolve("--handle", "h2", "--evidence", "found in provider audit"), 2)
        self.assertEqual(read_json(path)["submission_state"], "uncertain")

    def test_binding_requires_exactly_the_other_terminal_matches(self):
        path = self.record(submission_state="conflict", match_handles=["a", "b", "c"])
        for wrong in (["b"], ["a", "b", "c"], ["b", "d"]):
            args = [v for match in wrong for v in ("--terminal-match", match)]
            with self.subTest(wrong=wrong):
                self.assertEqual(self.resolve("--handle", "a", *args, "--evidence", "audit 123"), 2)
                self.assertEqual(occupies(read_json(path)), 3)
        self.assertEqual(self.resolve("--handle", "a", "--terminal-match", "c", "--terminal-match", "b",
                                      "--evidence", "audit 123 verifies b and c terminated"), 0)
        self.assertEqual(read_json(path)["resolutions"][-1]["terminal_matches"], ["b", "c"])
