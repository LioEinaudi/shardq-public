import importlib
import json
import multiprocessing
import shlex
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from shardq.backends.base import RangeResult
from shardq.backends.fake import FakeBackend
from shardq.backends.lmms_eval import LmmsEvalBackend, _LmmsSession
from shardq.backends.script import ScriptBackend
from shardq.ledger import Ledger, atomic_write_json, read_json
from shardq.register import register
from shardq.spec import pool_from_dict
from shardq.worker import run_worker


class OtherScriptBackend(ScriptBackend):
    """An independently configured backend that advertises the same key."""


def marker_command(marker):
    row = json.dumps({"unit_id": 0, "marker": marker})
    return f'echo {shlex.quote(row)} > "$SHARDQ_OUT_DIR/records.jsonl"'


def run_lmms_fixture_worker(root, worker_id, preload):
    if preload is not None:
        sys.path.insert(0, str(preload))
        importlib.import_module("lmms_eval")
    def write_marker(session, partition, start, count, out_dir):
        records = out_dir / "records.jsonl"
        records.write_text(json.dumps({"unit_id": start, "source": session.lm}) + "\n")
        return RangeResult(count, 0.0, records)

    # Exercise real backend imports and model construction with a tiny model package.
    with patch.object(_LmmsSession, "run_range", write_marker):
        raise SystemExit(run_worker(root, "pool", worker_id, 60, 0))


class SessionCompatibilityTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = Ledger(self.root)
        atomic_write_json(self.root / "workers/w.json", {"worker_id": "w"})

    def register_job(self, job_id, backend, args, saved_key=None):
        # Registration may use a new pool configuration between jobs.
        pool = pool_from_dict("pool", {"backend": backend, "launcher": "local"})
        spec_path = self.root / f"{job_id}.json"
        atomic_write_json(spec_path, {
            "job_id": job_id, "backend": backend, "pool": "pool",
            "protocol_id": "test", "backend_args": {"partitions": {"p": 1}, **args},
        })
        job = register(self.root, spec_path, pools={"pool": pool})
        if saved_key is not None:
            job["session_key"] = saved_key
            atomic_write_json(self.ledger.job_dir(job_id) / "job.json", job)
        self.ledger.cut(job_id, "p", 1, 1.0, time.time())

    def run_one_worker(self):
        self.assertEqual(run_worker(self.root, "pool", "w", 60, 0), 0)

    def check_script_pair(self, markers, expected_opens, session_key=None, saved_key=None,
                          backends=("script", "script")):
        for job_id, marker, backend in zip(("alpha", "beta"), markers, backends):
            args = {"run_command": marker_command(marker)}
            if session_key is not None:
                args["session_key"] = session_key
            self.register_job(job_id, backend, args, saved_key)
        with patch.object(ScriptBackend, "open_session", autospec=True,
                          side_effect=ScriptBackend.open_session) as opened:
            self.run_one_worker()
        for job_id, marker in zip(("alpha", "beta"), markers):
            job_dir = self.ledger.job_dir(job_id)
            (complete,) = job_dir.glob("partitions/p/shards/*/attempts/*/complete.json")
            self.assertEqual(read_json(complete.parent / "records.jsonl")["marker"], marker)
        self.assertEqual(opened.call_count, expected_opens)

    def test_script_default_key_runs_each_command(self):
        self.check_script_pair(("alpha", "beta"), 2)

    def test_script_explicit_key_runs_each_command(self):
        self.check_script_pair(("alpha", "beta"), 2, session_key="shared")

    def test_script_legacy_any_runs_each_command(self):
        self.check_script_pair(("alpha", "beta"), 2, saved_key="any")

    def test_script_same_command_reuses_one_session(self):
        self.check_script_pair(("same", "same"), 1)

    def test_backend_identity_is_part_of_reuse_decision(self):
        self.check_script_pair(("same", "same"), 2,
                               backends=("script", f"{__name__}:OtherScriptBackend"))

    def test_fake_worker_honors_changed_poison_units(self):
        self.register_job("alpha", "fake", {"unit_seconds": 0}, saved_key="fake")
        self.register_job("beta", "fake", {"unit_seconds": 0, "poison_units": {"p": [0]}},
                          saved_key="fake")
        self.run_one_worker()
        beta = self.ledger.job_dir("beta")
        failures = list(beta.glob("partitions/p/shards/*/attempts/*/failed.json"))
        self.assertEqual([read_json(path)["reason"] for path in failures], ["backend_error"])

    def test_fake_unit_duration_changes_session_key(self):
        self.assertNotEqual(FakeBackend({"unit_seconds": 0}, {}, {}).session_key(),
                            FakeBackend({"unit_seconds": 1}, {}, {}).session_key())

    def test_lmms_key_covers_captured_settings_and_source(self):
        original = LmmsEvalBackend({}, {}, {"lmms_root": "/one"}).session_key()
        for args, root in [({"force_simple": True}, "/one"), ({"bootstrap_iters": 0}, "/one"),
                           ({}, "/two")]:
            with self.subTest(args=args, root=root):
                other = LmmsEvalBackend(args, {}, {"lmms_root": root}).session_key()
                self.assertNotEqual(original, other)

    def lmms_source(self, name):
        source = self.root / "src" / name
        package = source / "lmms_eval"
        package.mkdir(parents=True)
        (package / "__init__.py").touch()
        (package / "models.py").write_text(
            "class Model:\n"
            "    @staticmethod\n"
            f"    def create_from_arg_string(*args): return {name!r}\n"
            "def get_model(*args): return Model\n"
        )
        (package / "tasks.py").write_text("def TaskManager(*args, **kwargs): return object()\n")
        return source

    def lmms_job(self, name):
        source = self.lmms_source(name)
        job = self.ledger.job_dir(name)
        atomic_write_json(job / "job.json", {
            "job_id": name, "pool": "pool", "backend": "lmms_eval",
            "pool_snapshot": {"backend": "lmms_eval", "launcher": "local"},
            "pins": {"lmms_root": str(source), "lmms_commit": name},
        })
        atomic_write_json(job / "partitions/p/partition.json", {"size": 1, "next_cut": 0})
        self.ledger.cut(name, "p", 1, 1.0, time.time())

    def spawn_lmms_worker(self, name, preload=None):
        atomic_write_json(self.root / f"workers/{name}.json", {"worker_id": name})
        process = multiprocessing.get_context("spawn").Process(
            target=run_lmms_fixture_worker, args=(self.root, name, preload),
        )
        process.start()
        self.addCleanup(process.kill)
        process.join(15)
        return process.exitcode

    def test_lmms_changed_pin_hands_off_to_a_fresh_worker(self):
        for name in ("alpha", "beta"):
            self.lmms_job(name)
        self.assertEqual(self.spawn_lmms_worker("one"), 0)
        beta = self.ledger.scan_job("beta", time.time())
        self.assertEqual(beta.partitions["p"].done, 0,
                         "the first process ran beta with cached alpha imports")
        self.assertEqual(beta.partitions["p"].pending, 1)
        self.assertEqual(beta.session_errors, 0)
        shard = beta.partitions["p"].shards[0]
        self.assertEqual((shard.errors, shard.preemptions), (0, 0))
        (failure,) = self.ledger.job_dir("beta").glob("partitions/p/shards/*/attempts/*/failed.json")
        self.assertEqual(read_json(failure)["reason"], "worker_restart")
        self.assertEqual(self.spawn_lmms_worker("two"), 0)
        for name in ("alpha", "beta"):
            (records,) = self.ledger.job_dir(name).glob("partitions/p/shards/*/attempts/*/records.jsonl")
            self.assertEqual(read_json(records)["source"], name)

    def test_lmms_foreign_preload_is_a_setup_failure(self):
        self.lmms_job("alpha")
        self.assertEqual(self.spawn_lmms_worker("one", self.lmms_source("other")), 2)
        job = self.ledger.scan_job("alpha", time.time())
        self.assertEqual(job.session_errors, 1)
        self.assertEqual(job.partitions["p"].done, 0)
