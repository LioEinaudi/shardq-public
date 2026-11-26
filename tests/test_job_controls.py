import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from shardq.cli import main
from shardq.ledger import Ledger, atomic_write_json


class JobControlTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.root = self.base / "ledger"
        self.ledger = Ledger(self.root)
        self.job_id = "run.1_a-b"
        self.job = self.ledger.job_dir(self.job_id)
        atomic_write_json(self.job / "job.json", {"job_id": self.job_id, "pool": "p"})

    def command(self, action, job_id):
        errors = io.StringIO()
        with redirect_stdout(io.StringIO()), redirect_stderr(errors):
            result = main([action, "--root", str(self.root), "--job", job_id])
        return result, errors.getvalue()

    def snapshot(self):
        return {path.relative_to(self.base): path.read_bytes() if path.is_file() else None
                for path in self.base.rglob("*")}

    def test_repeated_stop_and_resume_of_registered_job(self):
        for action, stopped in (("stop", True), ("stop", True), ("resume", False), ("resume", False)):
            self.assertEqual(self.command(action, self.job_id), (0, ""))
            self.assertEqual(self.ledger.scan_job(self.job_id, 1).claims_blocked, stopped)
        self.assertTrue(self.ledger.scan_job(self.job_id, 1).setup_epoch)

    def test_unregistered_job_does_not_create_or_change_anything(self):
        before = self.snapshot()
        for action in ("resume", "stop"):
            with self.subTest(action=action):
                code, error = self.command(action, "run.1_a-c")
                self.assertNotEqual(code, 0)
                self.assertIn("run.1_a-c", error)
                self.assertEqual(self.snapshot(), before)

    def test_path_forms_cannot_change_an_existing_job(self):
        # The relative and absolute paths point to the fixture. The STOP and epoch
        # contents expose erroneous writes from either command.
        (self.job / "STOP").write_text("original stop")
        atomic_write_json(self.job / "setup_epoch.json", "original epoch")
        before = self.snapshot()
        for job_id in ("../jobs/" + self.job_id, str(self.job), ".", ".."):
            for action in ("stop", "resume"):
                with self.subTest(action=action, job_id=job_id):
                    code, error = self.command(action, job_id)
                    self.assertNotEqual(code, 0)
                    self.assertIn("invalid", error.lower())
                    self.assertEqual(self.snapshot(), before)

    def test_job_directory_alias_cannot_control_another_job(self):
        self.ledger.job_dir("alias").symlink_to(self.job, target_is_directory=True)
        before = self.snapshot()
        for action in ("stop", "resume"):
            with self.subTest(action=action):
                code, error = self.command(action, "alias")
                self.assertNotEqual(code, 0)
                self.assertIn("alias", error)
                self.assertEqual(self.snapshot(), before)
