import hashlib
import json
import shlex
import sys
import tempfile
import unittest
from pathlib import Path

from shardq.ledger import Ledger, atomic_write_json, read_json
from shardq.merge import merge_partition
from shardq.register import register
from shardq.spec import pool_from_dict


class ScriptSummaryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.pool = pool_from_dict("local", {"launcher": "local", "backend": "script"})
        self.ledger = Ledger(self.root)
        self.out = self.root / "jobs" / "files" / "merged" / "input"

    def register(self, size, **args):
        spec = self.root / "job.json"
        atomic_write_json(spec, {
            "job_id": "files", "protocol_id": "v1", "pool": "local", "backend": "script",
            "backend_args": {"partitions": {"input": size}, "run_command": "true", **args},
        })
        register(self.root, spec, pools={"local": self.pool})

    def complete(self, count, rows):
        self.ledger.cut("files", "input", count, 1, 1)
        claim = self.ledger.claim("local", ["files"], {"worker_id": "w"}, None, None, 2)
        records = claim.attempt_dir / "records.jsonl"
        records.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        self.ledger.complete(claim, records, count, 2, 1, {}, 3)

    def quarantine(self):
        shard = self.ledger.cut("files", "input", 1, 1, 4)
        self.ledger.quarantine("files", shard, "backend_error", 5)

    def merge(self, allow_partial):
        job = self.ledger.scan_job("files", 6)
        report = merge_partition(self.root, job, "input", self.pool, allow_partial, 6)
        return report, read_json(self.out / "results.json")

    def check_summary(self, size, missing, rows):
        report, summary = self.merge(bool(missing))
        status = "partial" if missing else "complete"
        expected = {
            "schema_version": 1, "job_id": "files", "partition": "input", "status": status,
            "expected_units": size, "completed_units": size - len(missing),
            "record_count": len(rows), "missing_units": missing,
        }
        self.assertEqual({key: summary[key] for key in expected}, expected)
        artifact = summary["artifacts"]["records"]
        records = self.out / "records.jsonl"
        self.assertEqual(Path(artifact["path"]), records)
        self.assertEqual(artifact["sha256"], hashlib.sha256(records.read_bytes()).hexdigest())
        self.assertEqual([json.loads(line) for line in records.read_text().splitlines()], rows)
        self.assertEqual(report["missing_units"], missing)
        self.assertEqual(read_json(Path(report["marker_file"]))["status"], status)
        self.assertEqual(self.ledger.scan_job("files", 7).partitions["input"].status,
                         "partial" if missing else "merged")

    def test_default_complete_counts_deduplicated_records_and_unique_units(self):
        self.register(2)
        first = {"unit_id": 0, "filter": "x", "value": "first"}
        second = {"unit_id": 0, "filter": "y"}
        last = {"unit_id": 1}
        # Exercise the merged-record contract on a historical multi-filter attempt.
        self.complete(2, [last, first, {**first, "value": "duplicate"}, second])
        self.check_summary(2, [], [first, second, last])

    def test_quarantine_keeps_default_summary_partial(self):
        self.register(2)
        row = {"unit_id": 0, "length": 42}
        self.complete(1, [row])
        self.quarantine()
        self.check_summary(2, [1], [row])

    def test_zero_valid_records_still_exports_a_partial_summary(self):
        self.register(1)
        self.quarantine()
        self.check_summary(1, [0], [])

    def test_custom_command_receives_records_and_meta_and_owns_its_result(self):
        script = self.root / "custom.py"
        script.write_text(
            "import json, os\n"
            "from pathlib import Path\n"
            "output = Path(os.environ['SHARDQ_OUT_DIR'])\n"
            "name = os.environ['SHARDQ_PARTITION']\n"
            "records = Path(os.environ['SHARDQ_RECORDS'])\n"
            "meta = json.loads(Path(os.environ['SHARDQ_META']).read_text())\n"
            "assert records == output / 'records.jsonl'\n"
            "assert meta['job_id'] == 'files' and meta['partition'] == name\n"
            "assert len(meta['shards_used']) == 1 and not meta['missing_units']\n"
            "value = sum(json.loads(line)['length'] for line in records.read_text().splitlines())\n"
            "(output / 'results.json').write_text(json.dumps({'results': {name: {'bytes': value}}}))\n",
            encoding="utf-8",
        )
        self.register(1, merge_command=shlex.join([sys.executable, str(script)]))
        self.complete(1, [{"unit_id": 0, "length": 7}])
        report, result = self.merge(False)
        self.assertEqual(result, {"results": {"input": {"bytes": 7}}})
        self.assertEqual(read_json(Path(report["marker_file"]))["status"], "complete")
