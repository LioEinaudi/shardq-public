from __future__ import annotations

import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from shardq.ledger import atomic_write_json, read_json


class AtomicMetadataTests(unittest.TestCase):
    def test_two_threads_can_publish_the_same_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            at_replace = threading.Barrier(2, timeout=5)
            replace = os.replace

            def replace_together(src, dst):
                at_replace.wait()
                replace(src, dst)

            with patch.object(os, "replace", replace_together), ThreadPoolExecutor(2) as pool:
                list(pool.map(lambda n: atomic_write_json(path, {"n": n}), (0, 1)))
            self.assertIn(read_json(path), ({"n": 0}, {"n": 1}))
            self.assertEqual(list(path.parent.iterdir()), [path])

    def test_failed_replace_preserves_destination_and_cleans_temp(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            atomic_write_json(path, {"old": True})
            with patch.object(os, "replace", side_effect=OSError("injected failure")):
                with self.assertRaisesRegex(OSError, "injected failure"):
                    atomic_write_json(path, {})
            self.assertEqual(read_json(path), {"old": True})
            self.assertEqual(list(path.parent.iterdir()), [path])
