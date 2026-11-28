import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from shardq.cli import _publish


PROJECT = Path(__file__).resolve().parents[1]
EXAMPLE = PROJECT / "examples" / "manifest_script"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


class ManifestScriptTests(unittest.TestCase):
    def test_register_run_status_and_publish_from_another_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            example = work / "input files"
            shutil.copytree(EXAMPLE, example)
            root = work / "ledger"
            env = dict(os.environ, SHARDQ_ROOT=str(root), SHARDQ_POOLS=str(example / "pools.json"),
                       FILE_TASK_DIR=str(example), PYTHONPATH=str(PROJECT))

            def cli(*args):
                process = subprocess.run([sys.executable, "-m", "shardq", *args], cwd=work,
                                         env=env, text=True, capture_output=True, timeout=45)
                self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
                return process.stdout

            cli("register", str(example / "job.json"))
            cli("scheduler", "--poll-seconds", "1", "--exit-when-idle")
            job, = json.loads(cli("status", "--job", "file-hashes", "--json"))["jobs"]
            self.assertEqual(job["partitions"]["files"]["status"], "merged")

            merged = root / "jobs/file-hashes/merged/files"
            data = (merged / "records.jsonl").read_bytes()
            names = (example / "manifest.txt").read_text(encoding="utf-8").splitlines()
            records = [json.loads(line) for line in data.splitlines()]
            self.assertEqual([row["file"] for row in records], names)
            for index, record in enumerate(records):
                source = (example / record["file"]).read_bytes()
                self.assertEqual(record["unit_id"], index)
                self.assertEqual(record["bytes"], len(source))
                self.assertEqual(record["sha256"], hashlib.sha256(source).hexdigest())

            report = read_json(merged / "results.json")
            self.assertEqual(report["job_id"], "file-hashes")
            self.assertEqual(report["partition"], "files")
            self.assertEqual(report["status"], "complete")
            for field in ("expected_units", "completed_units", "record_count"):
                self.assertEqual(report[field], len(names))
            self.assertEqual(report["missing_units"], [])
            records_info = report["artifacts"]["records"]
            self.assertEqual((merged / records_info["path"]).resolve(), merged / "records.jsonl")
            self.assertEqual(records_info["sha256"], hashlib.sha256(data).hexdigest())

            exported = work / "export"
            cli("publish", "--job", "file-hashes", "--out-dir", str(exported))
            self.assertEqual(read_json(exported / "acceptance.json")["status"], "accepted")
            artifact, = read_json(exported / "artifacts.json")["artifacts"]
            self.assertEqual(Path(artifact["records"]["path"]), merged / "records.jsonl")
            self.assertEqual(artifact["records"]["sha256"], records_info["sha256"])
            for kind in ("records", "results"):
                source = Path(artifact[kind]["path"])
                copy = exported / source.name
                shutil.copyfile(source, copy)
                self.assertEqual(hashlib.sha256(copy.read_bytes()).hexdigest(), artifact[kind]["sha256"])

            # The publication manifest must agree with the checksums fixed at merge time.
            (merged / "results.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                _publish(root, "file-hashes", work / "changed")
            self.assertFalse((work / "changed").exists())

    def test_script_only_opens_files_in_its_range(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "hash_files.py"
            shutil.copyfile(EXAMPLE / script.name, script)
            (root / "manifest.txt").write_text("missing-first\ninput\nmissing-last\n", encoding="utf-8")
            (root / "input").write_bytes(b"assigned")
            env = dict(os.environ, SHARDQ_START="1", SHARDQ_COUNT="1", SHARDQ_OUT_DIR=str(root))
            subprocess.run([sys.executable, str(script)], env=env, check=True)
            record = read_json(root / "records.jsonl")
            self.assertEqual(record["unit_id"], 1)
            self.assertEqual(record["file"], "input")
