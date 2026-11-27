from __future__ import annotations

import json
import os
import sys
import tempfile
import textwrap
import unittest
from itertools import product
from pathlib import Path
from unittest.mock import patch

from shardq.backend_exec import run_backend_op
from shardq.backends.lmms_eval import _LmmsSession
from shardq.cli import lmms_merge_check
from shardq.ledger import Ledger, atomic_write_json
from shardq.merge import merge_partition
from shardq.spec import pool_from_dict


def _lmms_fixture(root: Path, factor: int) -> None:
    package = root / "lmms_eval"
    package.mkdir(parents=True)
    (package / "__init__.py").touch()
    (package / "utils.py").write_text("handle_non_serializable = str\n")
    (package / "tasks.py").write_text(textwrap.dedent("""
        class TaskManager:
            def __init__(self, *args, **kwargs):
                pass
            def match_tasks(self, tasks):
                return tasks
        def get_task_dict(tasks, task_manager):
            return tasks
    """))
    (package / "evaluator_utils.py").write_text(textwrap.dedent(f"""
        from collections import defaultdict
        from types import SimpleNamespace

        class Output:
            def __init__(self, name):
                self.task_name = name
                self.task_alias = name
                self.task_config = {{}}
                self.version = {factor}
                self.task = SimpleNamespace(
                    eval_docs=[None] * {factor}, aggregation=lambda: {{"score": sum}})
                self.sample_metrics = defaultdict(list)
                self.agg_metrics = {{}}
            def calculate_aggregate_metric(self, bootstrap_iters):
                for (metric, filter_key), values in self.sample_metrics.items():
                    self.agg_metrics[f"{{metric}},{{filter_key}}"] = sum(values) * {factor}

        def get_task_list(tasks):
            return [Output(name) for name in tasks]
    """))


class LmmsSourceIsolationTests(unittest.TestCase):
    def test_consecutive_merges_use_each_jobs_pinned_source(self) -> None:
        backends = ("lmms_eval", "shardq.backends.lmms_eval:LmmsEvalBackend")
        for backend, python in product(backends, (None, sys.executable)):
            with self.subTest(backend=backend, local_python=python), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ledger = Ledger(root)
                pool = pool_from_dict("p", {
                    "backend": backend, "launcher": "local", "local_python": python,
                })
                scores = []
                sizes = []
                checks = []
                before = {name: module for name, module in sys.modules.items()
                          if name == "lmms_eval" or name.startswith("lmms_eval.")}
                # Restore interpreter state when exercising the unfixed in-process path.
                with patch.dict(sys.modules), patch.object(sys, "path", list(sys.path)):
                    for factor in (1, 10):
                        source = root / f"source-{factor}"
                        _lmms_fixture(source, factor)
                        job_id = f"job-{factor}"
                        job_dir = ledger.job_dir(job_id)
                        pins = {"lmms_root": str(source), "lmms_commit": str(factor)}
                        args = {"tasks": ["task"], "bootstrap_iters": 0}
                        atomic_write_json(job_dir / "job.json", {
                            "job_id": job_id, "pool": "p", "column": "X", "backend": backend,
                            "protocol_id": "fixture", "backend_args": args, "pins": pins,
                            "session_key": str(factor), "retry": {}, "shard": {},
                        })
                        atomic_write_json(job_dir / "partitions/task/partition.json", {
                            "name": "task", "size": 1, "next_cut": 0, "status": "cutting",
                        })
                        ledger.cut(job_id, "task", 1, 1.0, 100.0)
                        claim = ledger.claim("p", [job_id], {"worker_id": "w"}, None, None, 101.0)
                        self.assertIsNotNone(claim)
                        records = claim.attempt_dir / "records.jsonl"
                        records.write_text('{"unit_id": 0, "score": 1}\n')
                        raw_results = claim.attempt_dir / "raw_results.json"
                        atomic_write_json(raw_results, {"results": {"task": {"score,none": factor}}})
                        ledger.complete(claim, records, 1, 101.0, 1.0,
                                        {"raw_results": str(raw_results)}, 102.0)
                        job = ledger.scan_job(job_id, 103.0)
                        merged = merge_partition(root, job, "task", pool, False, 103.0)
                        result = json.loads(Path(merged["result_file"]).read_text())
                        scores.append(result["results"]["task"]["score,none"])
                        checks.append(merged["selfcheck"])
                        self.assertEqual(json.loads(Path(merged["marker_file"]).read_text())["status"], "complete")
                        described = run_backend_op(pool, "describe", {"backend_args": args, "pins": pins})
                        sizes.append(described["partitions"][0]["size"])
                    self.assertEqual(scores, [1, 10])
                    self.assertEqual(sizes, [1, 10])
                    self.assertEqual(checks, [{"shards_checked": 1, "mismatches": []}] * 2)
                    self.assertEqual({name: module for name, module in sys.modules.items()
                                      if name == "lmms_eval" or name.startswith("lmms_eval.")}, before)


class LmmsMergeTests(unittest.TestCase):
    def test_session_defers_model_cleanup_until_close(self) -> None:
        class DummyModel:
            def __init__(self) -> None:
                self.clean_calls = 0

            def clean(self) -> None:
                self.clean_calls += 1

        class DummyBackend:
            def session_key(self) -> str:
                return "dummy"

        model = DummyModel()
        session = _LmmsSession(DummyBackend(), model, object())
        model.clean()
        self.assertEqual(model.clean_calls, 0)
        session.close()
        self.assertEqual(model.clean_calls, 1)

    def test_mmstar_fixture_reaggregates_exactly(self) -> None:
        names = (
            "SHARDQ_TEST_LMMS_PYTHON",
            "SHARDQ_TEST_LMMS_ROOT",
            "SHARDQ_TEST_LMMS_FIXTURE_DIR",
        )
        values = {name: os.environ.get(name) for name in names}
        if not all(values.values()):
            self.skipTest(
                "set SHARDQ_TEST_LMMS_PYTHON, SHARDQ_TEST_LMMS_ROOT "
                "and SHARDQ_TEST_LMMS_FIXTURE_DIR"
            )
        python = Path(str(values["SHARDQ_TEST_LMMS_PYTHON"]))
        lmms_root = Path(str(values["SHARDQ_TEST_LMMS_ROOT"]))
        fixture = Path(str(values["SHARDQ_TEST_LMMS_FIXTURE_DIR"]))
        sample_files = list(fixture.glob("*_samples_mmstar.jsonl"))
        self.assertEqual(len(sample_files), 1, "expected one MMStar samples file")
        samples = sample_files[0]
        run = samples.name.removesuffix("_samples_mmstar.jsonl")
        results = fixture / f"{run}_results.json"
        report = lmms_merge_check(
            samples,
            results,
            lmms_root,
            python,
        )
        self.assertTrue(report["match"], report["diffs"])


if __name__ == "__main__":
    unittest.main()
