import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shardq.launchers.aihc import AihcLauncher
from shardq.launchers import aihc_client, local, volc_client
from shardq.launchers.base import TransientSubmitError
from shardq.launchers.volc import VolcLauncher
from shardq.spec import pool_from_dict


class CloudRecoveryTests(unittest.TestCase):
    def test_parse_failure_mentioning_quota_cannot_repeat_aihc_create(self):
        with patch.object(aihc_client, "_run_output", return_value="quota report 429: no job receipt") as invoke, \
             patch.object(aihc_client.time, "sleep"):
            with self.assertRaises(RuntimeError) as caught:
                aihc_client.create_job("job.json", "code", "pool", "queue")
        self.assertNotIsInstance(caught.exception, TransientSubmitError)
        self.assertEqual(invoke.call_count, 1)

    def test_post_create_error_mentioning_throttling_does_not_resubmit(self):
        for client, invoke, error in [
            (aihc_client, lambda: aihc_client.create_job("j", "c", "p", "q"),
             "Successfully created job job-abc; status query: rate limit 429"),
            (volc_client, lambda: volc_client.submit(Path("worker.yaml")),
             "task_id=t-abc; status query: Request was rejected: beyond the current flow control limit"),
        ]:
            with self.subTest(client=client.__name__), \
                 patch.object(client, "_run_output", side_effect=RuntimeError(error)) as command, \
                 patch.object(client.time, "sleep"):
                with self.assertRaises(RuntimeError) as caught:
                    invoke()
                self.assertNotIsInstance(caught.exception, TransientSubmitError)
                self.assertEqual(command.call_count, 1)

    def test_conflicting_volc_receipts_do_not_confirm_a_handle(self):
        for reply in ('[{"JobId":"t-1"},{"JobId":"t-2"}]', "task_id=t-1 task_id=t-2",
                      '{"JobId":"t-1"}\n{"JobId":"t-2"}'):
            with self.subTest(reply=reply), patch.object(volc_client, "_run_output", return_value=reply) as invoke:
                with self.assertRaises(ValueError):
                    volc_client.submit(Path("worker.yaml"))
                self.assertEqual(invoke.call_count, 1)

    def test_unrelated_aihc_job_reference_is_not_a_create_receipt(self):
        with patch.object(aihc_client, "_run_output", return_value="uploaded diagnostics for old job-abc") as invoke:
            with self.assertRaises(RuntimeError):
                aihc_client.create_job("j", "c", "p", "q")
            self.assertEqual(invoke.call_count, 1)

    def test_explicit_provider_rejection_is_classified_from_response_only(self):
        failures = [
            (volc_client, lambda: volc_client.submit(Path("worker.yaml")),
             "Request was rejected: beyond the current flow control limit"),
            (aihc_client, lambda: aihc_client.create_job("j", "c", "p", "q"),
             "server rejected request: rate limit 429"),
        ]
        for client, submit, rejection in failures:
            with self.subTest(client=client.__name__), \
                 patch.object(client.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, rejection)) as run, \
                 patch.object(client.time, "sleep"):
                with self.assertRaises(TransientSubmitError):
                    submit()
                self.assertEqual(run.call_count, client.SUBMIT_RETRIES)

    def test_cloud_lookup_needs_more_than_a_worker_name(self):
        for name, cls in (("volc", VolcLauncher), ("aihc", AihcLauncher)):
            with self.subTest(name=name):
                pool = pool_from_dict("p", {"launcher": name, "backend": "fake", "entry": "/entry"})
                with self.assertRaises(NotImplementedError):
                    cls(Path("."), pool, Path(".")).lookup_submission("w")

    def test_volc_fingerprint_tracks_template_contents(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "task.yaml"
            pool = pool_from_dict("p", {"launcher": "volc", "backend": "fake", "entry": "/entry",
                                        "volc": {"template": str(path)}})
            launcher = VolcLauncher(Path(tmp), pool, Path("."))
            path.write_text("TaskName: {TASK_NAME}\n")
            before = launcher.submission_inputs()
            path.write_text("TaskName: other\n")
            self.assertNotEqual(before, launcher.submission_inputs())


class LocalLookupTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.workers = self.root / "workers"
        self.starts = self.workers / "w.starts"
        self.starts.mkdir(parents=True)
        self.row = {"worker_id": "w", "pool": "p", "launcher": "local", "request_id": "r"}
        (self.workers / "w.json").write_text(json.dumps(self.row))
        self.launcher = local.LocalLauncher(self.root, pool_from_dict("p", {"backend": "fake", "launcher": "local"}), self.root)

    def receipt(self, name, handle, **updates):
        payload = dict(self.row, local_handle=handle)
        payload.update(updates)
        (self.starts / name).write_text(json.dumps(payload))

    def test_recovery_distinguishes_missing_live_terminal_and_conflicting_receipts(self):
        self.assertEqual(self.launcher.lookup_submission("w"), [])
        with subprocess.Popen(["sleep", "60"]) as child:
            try:
                handle = local.local_process_handle(child.pid)
                self.receipt("one.json", handle)
                self.assertEqual(self.launcher.lookup_submission("w"), [handle])
                self.assertEqual(self.launcher.status(handle), "running")
            finally:
                child.terminate()
        self.assertTrue(self.launcher.status(handle).startswith("terminal:"))
        self.assertEqual(self.launcher.lookup_submission("w"), [handle])
        other = local.local_process_handle(os.getpid())
        self.receipt("two.json", other)
        self.assertEqual(set(self.launcher.lookup_submission("w")), {handle, other})

    def test_mismatched_or_unreadable_receipt_prevents_unique_match(self):
        handle = local.local_process_handle(os.getpid())
        self.receipt("one.json", handle)
        self.receipt("two.json", handle, request_id="other")
        with self.assertRaises(ValueError):
            self.launcher.lookup_submission("w")
        (self.starts / "two.json").write_text("{")
        with self.assertRaises(ValueError):
            self.launcher.lookup_submission("w")

    def test_wrong_process_generation_is_terminal(self):
        handle = local.local_process_handle(os.getpid())
        earlier = handle.rsplit(":", 1)[0] + ":0"
        self.assertTrue(self.launcher.status(earlier).startswith("terminal:"))
