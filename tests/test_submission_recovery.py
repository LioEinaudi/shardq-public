"""Process-exit tests for the scheduler's durable worker submission protocol."""

import multiprocessing
import os
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from shardq import controller
from shardq.launchers.base import Launcher, SubmissionChangedError, TransientSubmitError
from shardq.ledger import Ledger, atomic_write_json, read_json


CRASHED = 74
KEPT_POLLING = 75


class Platform(Launcher):
    """Each accepted call creates a separate task, even for a repeated request ID."""

    def __init__(self, directory, pool, scenario, batch):
        self.directory = directory / pool
        self.directory.mkdir(exist_ok=True)
        self.scenario = scenario
        self.batch = batch

    def submit(self, worker_id, argv, env):
        # These files belong to the simulated service, outside workers/ and the ledger.
        if self.scenario == "template-changed":
            raise SubmissionChangedError("template changed at dispatch")
        (self.directory / ("call-" + uuid4().hex)).write_text(worker_id)
        if self.scenario == "reject":
            raise TransientSubmitError("quota rejected before creation")
        if self.scenario == "exit-before-create":
            os._exit(CRASHED)
        handle = uuid4().hex
        atomic_write_json(self.directory / (handle + ".json"), {
            "worker_id": worker_id, "handle": handle, "status": "running",
        })
        if self.scenario == "accepted-crash":
            os._exit(CRASHED)
        if self.scenario == "parallel-crash":
            self.batch.wait(timeout=5)
            os._exit(CRASHED)
        if self.scenario == "timeout":
            raise TimeoutError("response lost after creation")
        if self.scenario == "empty-reply":
            return ""
        return handle

    def lookup_submission(self, worker_id):
        (self.directory / ("lookup-" + uuid4().hex)).write_text(worker_id)
        if self.scenario == "query-error":
            raise OSError("service unavailable")
        if self.scenario == "partial-list":
            raise ValueError("provider did not return all pages")
        if self.scenario == "unsupported":
            raise NotImplementedError("no lookup capability")
        if self.scenario == "invisible":
            return []
        tasks = [read_json(path) for path in self.directory.glob("*.json")]
        return [task["handle"] for task in tasks if task["worker_id"] == worker_id]

    def status(self, handle):
        (self.directory / ("status-" + uuid4().hex)).write_text(handle)
        if self.scenario == "status-unavailable":
            raise OSError("status endpoint unavailable")
        return read_json(self.directory / (handle + ".json"))["status"]

    def capacity_slots(self):
        if self.scenario == "quota-unavailable" and self.directory.name == "p":
            raise subprocess.TimeoutExpired(["quota"], 1)
        quota = self.directory / "available-slots"
        return int(quota.read_text()) if quota.exists() else None

    def kill(self, handle):
        raise AssertionError("recovery tried to remove a platform task")


def scheduler_process(root, service, now, scenario, launcher_name, until_idle, batch_size):
    # Spawn imports a new scheduler module and starts with empty in-memory caches.
    batch = threading.Barrier(batch_size)
    replace = os.replace

    def publish(src, dst):
        if Path(dst).parent == root / "workers" and Path(dst).suffix == ".json":
            row = read_json(Path(src), {})
            if scenario == "all-intents-ready" and row.get("submission_state") == "submitting":
                os._exit(CRASHED)
            if scenario == "before-confirm" and row.get("handle"):
                os._exit(CRASHED)
            replace(src, dst)
            if scenario == "after-intent" and row.get("submission_state") == "intent":
                os._exit(CRASHED)
            return
        replace(src, dst)

    def launcher(root, provider, pool):
        return Platform(service, pool.name, scenario, batch)

    with patch.object(controller, "_launcher", launcher), patch.object(os, "replace", publish), \
            patch("time.time", return_value=now), patch("time.sleep", side_effect=lambda _: os._exit(KEPT_POLLING)):
        code = controller.run_scheduler(root, launcher_name, 1, not until_idle, until_idle,
                                        {"pools_path": root / "pools.json"})
    raise SystemExit(code)


class SubmissionRecoveryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name)
        self.root = base / "ledger"
        self.service = base / "platform"
        self.service.mkdir()
        self.configure_pool()
        self.job("job")

    def configure_pool(self, **fields):
        self.pool = {"backend": "fake", "launcher": "local", "max_workers": 1, **fields}
        atomic_write_json(self.root / "pools.json", {"pools": {"p": self.pool}})

    def job(self, job_id, pool="p"):
        directory = self.root / "jobs" / job_id
        atomic_write_json(directory / "job.json", {
            "job_id": job_id, "pool": pool, "backend": "fake", "shard": {
                "target_seconds": 1, "min_units": 1, "max_units": 1, "lookahead": 6,
            }, "retry": {"max_session_errors": 1},
        })
        atomic_write_json(directory / "partitions" / "part" / "partition.json", {
            "name": "part", "size": 6, "next_cut": 0, "status": "cutting", "unit_seconds_est": 1.0,
        })

    def run_tick(self, now, scenario="", expected=0, until_idle=False, batch_size=1):
        process = multiprocessing.get_context("spawn").Process(target=scheduler_process, args=(
            self.root, self.service, now, scenario, self.pool["launcher"], until_idle, batch_size,
        ))
        process.start()
        try:
            process.join(timeout=15)
            self.assertEqual(process.exitcode, expected, scenario)
        finally:
            if process.is_alive():
                process.kill()
                process.join()

    def tasks(self):
        return [read_json(path) for path in sorted(self.service.glob("*/*.json"))]

    def rows(self):
        return [read_json(path) for path in sorted((self.root / "workers").glob("*.json"))]

    def row(self):
        row, = self.rows()
        return row

    def calls(self, operation):
        return [path.read_text() for path in self.service.glob("*/" + operation + "-*")]

    def test_crash_after_intent_recovers_same_request_before_any_platform_call(self):
        self.run_tick(100, "after-intent", CRASHED)
        intent = self.row()
        self.assertEqual(intent["submission_state"], "intent")
        self.assertEqual(intent["request_id"], intent["worker_id"])
        self.assertTrue(intent["request_fingerprint"])
        self.assertEqual(self.calls("call"), [])
        self.run_tick(500)
        task, = self.tasks()
        self.assertEqual(task["worker_id"], intent["worker_id"])
        self.assertEqual(self.row()["handle"], task["handle"])
        self.assertEqual(self.row()["submission_state"], "confirmed")

    def test_crash_after_platform_acceptance_recovers_original_task(self):
        self.run_tick(100, "accepted-crash", CRASHED)
        task, = self.tasks()
        before = self.rows()
        self.run_tick(1000)
        self.assertEqual(self.tasks(), [task], "restart created a second platform task")
        self.assertEqual(self.calls("call"), [task["worker_id"]])
        self.assertEqual(self.row()["handle"], task["handle"])
        self.assertEqual([r["submission_state"] for r in before], ["submitting"])

    def test_crash_before_confirmation_write_recovers_original_handle(self):
        self.run_tick(100, "before-confirm", CRASHED)
        task, = self.tasks()
        self.run_tick(1000)
        self.assertEqual(self.tasks(), [task])
        self.assertEqual(self.row()["handle"], task["handle"])

    def test_crash_just_before_platform_creation_keeps_the_reservation(self):
        self.run_tick(100, "exit-before-create", CRASHED)
        self.run_tick(1000)
        self.run_tick(1000000)
        self.assertEqual(self.tasks(), [])
        self.assertEqual(len(self.calls("call")), 1)
        self.assertEqual(self.row()["submission_state"], "uncertain")

    def test_lookup_failures_and_zero_results_hold_capacity_across_restarts(self):
        self.run_tick(100, "timeout")
        task, = self.tasks()
        for now, mode in enumerate(("query-error", "invisible", "partial-list", "unsupported"), start=1):
            with self.subTest(mode=mode):
                now *= 10000
                self.run_tick(now, mode)
                self.assertEqual(self.tasks(), [task])
                row = self.row()
                self.assertEqual(row["submission_state"], "uncertain")
                self.assertGreater(row["reconcile_after_unix"], now)
                queries = len(self.calls("lookup"))
                self.run_tick(now + 1, mode)
                self.assertEqual(len(self.calls("lookup")), queries)
                self.assertNotIn("", self.calls("status"))
        self.run_tick(100000)
        self.assertEqual(self.row()["handle"], task["handle"])
        self.assertEqual(self.tasks(), [task])

    def test_lookup_includes_fast_terminal_task_after_job_is_stopped(self):
        self.run_tick(100, "accepted-crash", CRASHED)
        task, = self.tasks()
        task["status"] = "terminal:success"
        atomic_write_json(self.service / "p" / (task["handle"] + ".json"), task)
        (self.root / "jobs" / "job" / "STOP").touch()
        self.run_tick(1000)
        self.run_tick(2000)
        self.assertEqual(self.tasks(), [task])
        self.assertEqual(self.row()["handle"], task["handle"])
        self.assertEqual(self.row()["submission_state"], "terminal")

    def test_multiple_service_matches_record_conflict_and_keep_capacity(self):
        self.run_tick(100, "accepted-crash", CRASHED)
        task, = self.tasks()
        second = dict(task, handle="duplicate")
        atomic_write_json(self.service / "p" / "duplicate.json", second)
        self.run_tick(1000)
        self.run_tick(1000000)
        self.assertEqual(self.row()["submission_state"], "conflict")
        self.assertFalse(self.row().get("handle"))
        self.assertEqual(len(self.calls("call")), 1)
        self.assertEqual(len(self.tasks()), 2)
        self.assertNotIn("", self.calls("status"))

    def test_parallel_calls_recover_all_three_separate_reservations(self):
        self.configure_pool(max_workers=3)
        self.run_tick(100, "parallel-crash", CRASHED, batch_size=3)
        tasks = self.tasks()
        self.assertEqual(len(tasks), 3)
        before = self.rows()
        self.run_tick(1000)
        self.assertEqual(self.tasks(), tasks)
        self.assertEqual({r["worker_id"] for r in before if r["submission_state"] == "submitting"},
                         {t["worker_id"] for t in tasks})
        self.assertEqual({r["request_id"]: r["handle"] for r in self.rows()},
                         {t["worker_id"]: t["handle"] for t in tasks})
        self.assertNotIn("", self.calls("status"))

    def test_uncertain_worker_prevents_idle_exit_with_a_stopped_job(self):
        self.run_tick(100, "timeout")
        (self.root / "jobs" / "job" / "STOP").touch()
        self.run_tick(1000, "invisible", KEPT_POLLING, until_idle=True)
        state = read_json(self.root / "scheduler" / "state.json")
        self.assertEqual(state["updated_unix"], 1000)
        self.assertEqual(len(self.tasks()), 1)

    def test_changed_execution_configuration_freezes_uncalled_intent(self):
        self.run_tick(100, "after-intent", CRASHED)
        intent = self.row()
        self.configure_pool(env={"MODEL_PATH": "/different"})
        self.run_tick(1000)
        self.assertEqual(self.calls("call"), [])
        self.assertEqual(self.row()["submission_state"], "conflict")
        self.assertEqual(self.row()["request_fingerprint"], intent["request_fingerprint"])

    def test_known_precreation_rejection_reuses_same_request(self):
        self.run_tick(100, "reject")
        request_id, = self.calls("call")
        self.assertEqual(self.tasks(), [])
        self.assertEqual(self.row()["submission_state"], "rejected")
        self.run_tick(1000)
        task, = self.tasks()
        self.assertEqual(task["worker_id"], request_id)
        self.assertEqual(self.calls("call"), [request_id, request_id])

    def test_empty_reply_after_creation_reconciles_without_empty_handle_query(self):
        self.run_tick(100, "empty-reply")
        task, = self.tasks()
        self.assertEqual(self.row()["submission_state"], "uncertain")
        self.run_tick(1000)
        self.assertEqual(self.row()["handle"], task["handle"])
        self.assertNotIn("", self.calls("status"))
        self.assertEqual(self.tasks(), [task])

    def test_probe_queue_slot_is_reserved_even_without_a_handle(self):
        self.configure_pool(launcher="aihc", max_workers=3, aihc={"max_queued": 1, "gpu_slots": 1})
        self.run_tick(100, "timeout")
        self.run_tick(1000, "invisible")
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual(self.row()["submission_state"], "uncertain")

    def test_recovered_handle_without_status_still_uses_probe_queue_slot(self):
        self.configure_pool(launcher="aihc", max_workers=3, aihc={"max_queued": 1, "gpu_slots": 1})
        self.run_tick(100, "accepted-crash", CRASHED)
        self.run_tick(1000, "status-unavailable")
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual(self.row()["submission_state"], "confirmed")
        self.assertIn("status endpoint unavailable", self.row()["status_error"])

    def test_uncalled_intent_is_withdrawn_after_stop_and_scheduler_can_exit(self):
        self.run_tick(100, "after-intent", CRASHED)
        (self.root / "jobs" / "job" / "STOP").touch()
        self.run_tick(1000, until_idle=True)
        self.assertEqual(self.calls("call"), [])
        self.assertEqual(self.row()["submission_state"], "terminal")

    def test_lower_pool_limit_withdraws_excess_intents_before_dispatch(self):
        self.configure_pool(max_workers=3)
        self.run_tick(100, "all-intents-ready", CRASHED)
        original_ids = {row["worker_id"] for row in self.rows()}
        self.assertEqual(len(original_ids), 3)
        self.assertEqual(self.calls("call"), [])
        self.configure_pool(max_workers=1)
        self.run_tick(1000)
        self.run_tick(2000)
        task, = self.tasks()
        self.assertIn(task["worker_id"], original_ids)
        states = sorted(row["submission_state"] for row in self.rows())
        self.assertEqual(states, ["confirmed", "terminal", "terminal"])

    def test_audit_resource_slots_are_reserved_until_reconciled(self):
        self.configure_pool(launcher="volc", max_workers=3, volc={"gpu_slots": 2})
        (self.service / "p").mkdir()
        (self.service / "p" / "available-slots").write_text("2")
        self.run_tick(100, "timeout")
        self.run_tick(1000, "invisible")
        self.assertEqual(len(self.tasks()), 1)
        self.assertEqual(self.row()["gpu_slots"], 2)

    def test_shared_volc_audit_charges_original_scope_even_after_config_change(self):
        audit = {"capacity_mode": "audit", "gpu_slots": 2, "gpu_cap": 2,
                 "queue": "research", "creator": "tester"}
        self.configure_pool(launcher="volc", max_workers=3, volc=audit)
        for pool_name in ("p", "q", "other"):
            # Simulate audit listings that have not picked up the accepted task yet.
            directory = self.service / pool_name
            directory.mkdir()
            (directory / "available-slots").write_text("2")
        self.run_tick(100, "timeout")
        original = self.pool.copy()
        other = dict(original, volc=dict(audit, queue="independent"))
        self.configure_pool(launcher="volc", max_workers=3, volc=dict(audit, queue="renamed"))
        atomic_write_json(self.root / "pools.json", {"pools": {"p": self.pool, "q": original, "other": other}})
        self.job("second", pool="q")
        self.job("independent", pool="other")
        (self.root / "jobs" / "job" / "STOP").touch()
        self.run_tick(1000)
        self.assertEqual(len(list((self.service / "p").glob("*.json"))), 1)
        self.assertEqual(len(list((self.service / "other").glob("*.json"))), 1)
        self.assertEqual(len(list((self.service / "q").glob("*.json"))), 0,
                         "a second task exceeded the original shared audit budget")

    def test_capacity_query_error_does_not_block_another_pool(self):
        atomic_write_json(self.root / "pools.json", {"pools": {"p": self.pool, "q": self.pool}})
        self.job("other-job", pool="q")
        self.run_tick(100, "quota-unavailable")
        self.assertEqual(list((self.service / "p").glob("*.json")), [])
        self.assertEqual(len(list((self.service / "q").glob("*.json"))), 1)

    def test_dispatch_detects_changed_request_before_platform_call(self):
        self.run_tick(100, "template-changed")
        self.run_tick(1000)
        self.assertEqual(self.row()["submission_state"], "conflict")
        self.assertEqual(self.calls("call"), [])
        self.assertEqual(self.tasks(), [])

    def test_changing_a_pool_launcher_keeps_its_original_reservation(self):
        self.run_tick(100, "accepted-crash", CRASHED)
        original, = self.tasks()
        self.configure_pool(launcher="aihc", aihc={"max_queued": 1})
        self.run_tick(1000)
        self.assertEqual(self.tasks(), [original])
        self.assertEqual(self.row()["submission_state"], "conflict")
        self.assertEqual(self.calls("status"), [])

    def test_setup_pause_still_reconciles_then_healthy_job_drives_new_demand(self):
        self.run_tick(100, "accepted-crash", CRASHED)
        ledger = Ledger(self.root)
        claim = ledger.claim("p", ["job"], {"worker_id": "failed-setup"}, None, None, 101)
        ledger.fail(claim, "session_error", True, "model unavailable", 10, 102)
        task, = self.tasks()
        task["status"] = "terminal:exit"
        atomic_write_json(self.service / "p" / (task["handle"] + ".json"), task)
        self.run_tick(1000)
        self.run_tick(2000)
        self.assertEqual(self.row()["submission_state"], "terminal")
        self.assertEqual(self.tasks(), [task])
        job = ledger.scan_job("job", 2000)
        self.assertTrue(job.claims_blocked)
        self.assertEqual(job.session_errors, 1)
        self.job("healthy")
        self.run_tick(3000)
        self.assertEqual(len(self.tasks()), 2)
        self.assertEqual(ledger.scan_job("job", 3000).setup_status, "paused")
        ledger.resume("job")
        self.assertFalse(ledger.scan_job("job", 3001).claims_blocked)
