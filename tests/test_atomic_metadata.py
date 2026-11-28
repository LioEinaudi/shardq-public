from __future__ import annotations

import fcntl
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from shardq.controller import _update_history
from shardq.ledger import Ledger, atomic_write_json, read_json


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

    def test_history_cache_tracks_late_and_replaced_attempts_and_rebuilds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = Ledger(root)
            job = ledger.job_dir("j")
            partition = job / "partitions" / "p"
            atomic_write_json(job / "job.json", {"job_id": "j", "backend": "fake", "pool": "p"})
            atomic_write_json(partition / "partition.json", {"name": "p", "size": 1})
            shard = ledger.cut("j", "p", 1, 1, 1)
            attempts = partition / "shards" / shard / "attempts"
            cache = {}

            def complete(name, finished, elapsed):
                atomic_write_json(attempts / name / "complete.json",
                                  {"count": 1, "completed_unix": finished, "elapsed_seconds": elapsed})

            def update():
                _update_history(root, [ledger.scan_job("j", 50)], 50, [], cache)
                return read_json(root / "history.json")["fake::p"]

            complete("first", 20, 4)
            complete("last", 40, 20)
            self.assertEqual(update()["unit_seconds"], 12)
            with patch("shardq.controller.update_history", side_effect=AssertionError("unchanged EMA rebuilt")):
                self.assertEqual(update()["samples"], 2)
            complete("replay", 30, 12)
            self.assertEqual(update()["unit_seconds"], 14)
            complete("last", 40, 40)
            self.assertEqual(update()["unit_seconds"], 24)
            expected = update()
            cache.clear()
            (root / "history.json").unlink()
            self.assertEqual(update(), expected)

    def test_history_update_during_merge_keeps_estimate_and_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = Ledger(root)
            job = ledger.job_dir("j")
            metadata = job / "partitions" / "p" / "partition.json"
            atomic_write_json(job / "job.json", {"job_id": "j", "backend": "fake", "pool": "pool"})
            atomic_write_json(metadata, {
                "name": "p", "next_cut": 0, "status": "cutting", "size": 1, "unit_seconds_est": 1.0,
            })
            shard = ledger.cut("j", "p", 1, 1, 1)
            attempt = metadata.parent / "shards" / shard / "attempts" / "w"
            atomic_write_json(attempt / "complete.json", {"count": 1, "elapsed_seconds": 10.0})
            jobs = [ledger.scan_job("j", 10)]

            # Hold the merge just before it publishes partition.json. Let history
            # either contend for its lock or finish an unguarded update first.
            at_merge_publish = threading.Event()
            history_progress = threading.Event()
            replace, flock = os.replace, fcntl.flock
            main = threading.current_thread()

            def interleave(src, dst):
                if Path(dst) == metadata and threading.current_thread() is not main:
                    at_merge_publish.set()
                    self.assertTrue(history_progress.wait(5))
                replace(src, dst)
                if Path(dst) == metadata and threading.current_thread() is main:
                    history_progress.set()

            def track_flock(file, operation):
                if threading.current_thread() is main and operation & fcntl.LOCK_EX:
                    history_progress.set()
                return flock(file, operation)

            with patch.object(os, "replace", interleave), patch.object(fcntl, "flock", track_flock):
                with ThreadPoolExecutor(1) as pool:
                    merge = pool.submit(ledger.set_partition_status, "j", "p", "merged")
                    self.assertTrue(at_merge_publish.wait(5))
                    _update_history(root, jobs, 10, [])
                    merge.result(timeout=5)
            updated = read_json(metadata)
            self.assertEqual(updated["status"], "merged")
            self.assertEqual(updated["unit_seconds_est"], 10.0)
