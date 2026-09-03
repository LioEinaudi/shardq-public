from __future__ import annotations

import os
import unittest
from pathlib import Path

from shardq.backends.lmms_eval import _LmmsSession
from shardq.cli import lmms_merge_check


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
