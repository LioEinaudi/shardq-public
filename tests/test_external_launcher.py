"""The public launcher contract exercised through the installed-style CLI."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from shardq.launchers import get_launcher
from shardq.spec import load_pools, pool_from_dict


PROJECT = Path(__file__).resolve().parents[1]
EXAMPLE = PROJECT / "examples" / "external_launcher"
EXTERNAL = "threaded_local:ThreadedLocal"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


class ExternalLauncherTests(unittest.TestCase):
    def test_legacy_pool_sections_and_snapshots(self):
        for alias in ("volc", "aihc"):
            with self.subTest(alias=alias):
                old = {"launcher": alias, "backend": "fake", alias: {"image": "image:v1"},
                       "column": "legacy", "lmms_root": "/shared/lmms"}
                pool = pool_from_dict("old", old)
                self.assertEqual(pool.launcher_args, {"image": "image:v1"})
                self.assertEqual(pool.extra["column"], "legacy")
                self.assertEqual(pool.extra["lmms_root"], "/shared/lmms")
                self.assertEqual(pool_from_dict("old", pool.snapshot()), pool)
                both = dict(old, launcher_args={"image": "other"})
                with self.assertRaisesRegex(ValueError, "launcher_args"):
                    pool_from_dict("conflicting", both)

    def test_invalid_extension_names_report_which_launcher_failed(self):
        for value in ("missing_shardq_launcher:Custom", "pathlib:MissingLauncher",
                      "pathlib:Path", "shardq.launchers.base:Launcher"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as error:
                    get_launcher(value)
                self.assertIn(value, str(error.exception))

    def test_default_scheduler_serves_mixed_pools_and_local_filter_skips_custom(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pools = read_json(EXAMPLE / "pools.json")
            pools["pools"]["stock"] = {"launcher": "local", "backend": "fake",
                                       "worker_idle_exit_seconds": 1}
            pools_path = root / "pools.json"
            pools_path.write_text(json.dumps(pools), encoding="utf-8")
            env = dict(os.environ, SHARDQ_ROOT=str(root), SHARDQ_POOLS=str(pools_path))
            env["PYTHONPATH"] = os.pathsep.join((str(EXAMPLE), str(PROJECT)))

            def cli(*args):
                proc = subprocess.run([sys.executable, "-m", "shardq", *args], cwd=root,
                                      env=env, text=True, capture_output=True, timeout=45)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                return proc.stdout

            job = json.loads(cli("register", str(EXAMPLE / "job.json")))
            self.assertEqual(job["pool_snapshot"]["launcher"], EXTERNAL)
            self.assertEqual(job["pool_snapshot"]["launcher_args"]["threads_per_worker"], 2)
            cli("scheduler", "--launcher", "local", "--once")
            self.assertEqual(read_json(root / "scheduler/state.json")["jobs"], [])
            self.assertFalse(list((root / "workers").glob("*.json")))

            stock_job = dict(read_json(EXAMPLE / "job.json"), job_id="stock-demo", pool="stock")
            spec = root / "stock.json"
            spec.write_text(json.dumps(stock_job), encoding="utf-8")
            cli("register", str(spec))
            cli("scheduler", "--exit-when-idle", "--poll-seconds", "1")

            workers = [read_json(path) for path in (root / "workers").glob("*.json")]
            self.assertEqual({worker["launcher"] for worker in workers}, {"local", EXTERNAL})
            for name in ("stock-demo", job["job_id"]):
                merged = root / "jobs" / name / "merged" / "sample"
                rows = (merged / "records.jsonl").read_text(encoding="utf-8").splitlines()
                self.assertEqual([json.loads(row)["unit_id"] for row in rows], list(range(4)))
                self.assertEqual(read_json(merged / "merge.json")["missing_units"], [])

            external_worker = next(worker for worker in workers if worker["launcher"] == EXTERNAL)
            # Query from the test process after the scheduler and worker exit.
            # Recovery uses the receipt and the configured module:Class identity.
            sys.path.insert(0, str(EXAMPLE))
            try:
                pool = load_pools(pools_path)["cpu"]
                launcher = get_launcher(pool.launcher)(root, pool, PROJECT)
                matches = launcher.lookup_submission(external_worker["worker_id"])
            finally:
                sys.path.remove(str(EXAMPLE))
            self.assertEqual(matches, [external_worker["handle"]])
