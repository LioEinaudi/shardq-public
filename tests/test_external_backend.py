import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT = Path(__file__).resolve().parents[1]
EXAMPLE = PROJECT / "examples" / "python_backend"


class ExternalBackendTests(unittest.TestCase):
    def test_cli_reuses_a_session_and_merges_domain_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "ledger"
            env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(EXAMPLE), str(PROJECT))),
                       SHARDQ_ROOT=str(root), SHARDQ_POOLS=str(EXAMPLE / "pools.json"))

            def cli(*args):
                proc = subprocess.run([sys.executable, "-m", "shardq", *args], cwd=directory,
                                      env=env, text=True, capture_output=True, timeout=45)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                return proc.stdout

            preview = json.loads(cli("register", str(EXAMPLE / "job.json"), "--dry-run"))
            self.assertFalse(root.exists())
            registered = json.loads(cli("register", str(EXAMPLE / "job.json")))
            self.assertEqual(preview["session_key"], registered["session_key"])
            cli("scheduler", "--poll-seconds", "1", "--exit-when-idle")
            job_dir = root / "jobs/ticket-tags"
            reports = list(job_dir.glob("partitions/tickets/shards/*/attempts/*/complete.json"))
            ordinal = [json.loads(path.read_text())["backend_extra"]["session_range"] for path in reports]
            self.assertEqual(sorted(ordinal), [1, 2])
            result = json.loads((job_dir / "merged/tickets/results.json").read_text())
            self.assertEqual(result, {"by_tag": {"billing": 2, "access": 1, "other": 1},
                                      "completed_units": 4, "missing_units": []})
            marker = json.loads(next((job_dir / "markers").glob("*/*.complete.json")).read_text())
            for key in ("batch_size", "generation_kwargs", "canonical_source_proof"):
                self.assertNotIn(key, marker)
            cli("publish", "--job", "ticket-tags", "--out-dir", str(root / "export"))
            self.assertEqual(json.loads((root / "export/acceptance.json").read_text())["status"], "accepted")
