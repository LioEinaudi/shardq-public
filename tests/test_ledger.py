from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from shardq.cutter import plan_cut
from shardq.ledger import Ledger, ShardState, _atomic_write_text, atomic_write_json, read_json, shard_state


def _competing_claim(root: str, ready, queue) -> None:
    ready.wait()
    claim = Ledger(Path(root)).claim(
        "pool", ["job"], {"worker_id": f"w-{multiprocessing.current_process().pid}", "launcher": "local", "handle": "h"}, None, None, 100.0
    )
    queue.put(claim.shard_id if claim else None)


class LedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.ledger = Ledger(self.root)
        job = self.ledger.job_dir("job")
        atomic_write_json(
            job / "job.json",
            {
                "job_id": "job", "pool": "pool", "column": "X", "backend": "fake",
                "priority": 0, "session_key": "fake", "traits": {}, "retry": {}, "shard": {},
            },
        )
        atomic_write_json(
            job / "partitions" / "p" / "partition.json",
            {"name": "p", "size": 8, "next_cut": 0, "unit_seconds_est": 1.0, "status": "cutting"},
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_read_json_returns_default_when_file_vanishes_after_is_file(self) -> None:
        path = self.root / "vanished.json"
        with patch.object(Path, "is_file", return_value=True):
            self.assertEqual(read_json(path, {"x": 1}), {"x": 1})

    def test_read_json_returns_default_for_partial_content(self) -> None:
        path = self.root / "partial.json"
        for content in ('{"a":', ""):
            with self.subTest(content=content):
                path.write_text(content, encoding="utf-8")
                self.assertEqual(read_json(path, {"x": 1}), {"x": 1})
        path.write_bytes(b'\xff')
        self.assertEqual(read_json(path, {"x": 1}), {"x": 1})
        atomic_write_json(path, {"a": 2})
        self.assertEqual(read_json(path, {"x": 1}), {"a": 2})

    def test_scan_job_survives_concurrent_lease_release(self) -> None:
        partition_dir = self.ledger.job_dir("job") / "partitions" / "p"
        metadata = read_json(partition_dir / "partition.json")
        metadata["size"] = 40
        atomic_write_json(partition_dir / "partition.json", metadata)
        leases = [
            partition_dir / "shards" / self.ledger.cut("job", "p", 1, 1.0, 100.0) / "lease"
            for _ in range(40)
        ]
        stop = threading.Event()
        errors = []
        releases = [0, 0]

        def churn_leases(index: int) -> None:
            owner = {
                "attempt_id": "a", "worker_id": f"w-{index}",
                "handle": f"local:{index}", "acquired_unix": 1.0,
            }
            try:
                while not stop.is_set():
                    for lease in leases[index * 20:(index + 1) * 20]:
                        if stop.is_set():
                            return
                        os.mkdir(lease)
                        atomic_write_json(lease / "owner.json", owner)
                        _atomic_write_text(lease / "heartbeat", "1.0\n")
                        shutil.rmtree(lease, ignore_errors=True)
                        releases[index] += 1
            except Exception as error:
                errors.append(error)
                stop.set()

        threads = [threading.Thread(target=churn_leases, args=(index,)) for index in range(2)]
        for thread in threads:
            thread.start()
        scans = 0
        try:
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline and not stop.is_set():
                self.ledger.scan_job("job", time.time())
                scans += 1
        finally:
            stop.set()
            for thread in threads:
                thread.join()
        self.assertEqual(errors, [])
        self.assertGreater(scans, 0)
        self.assertTrue(all(count > 0 for count in releases))

    def test_shard_state_all_six_and_precedence(self) -> None:
        shard = self.root / "states" / "s"
        atomic_write_json(shard / "shard.json", {})
        self.assertEqual(shard_state(shard, 10), ShardState.PENDING)
        atomic_write_json(
            shard / "attempts" / "a" / "failed.json",
            {"failed_unix": 9, "retry_after_unix": 20, "reason": "backend_error"},
        )
        self.assertEqual(shard_state(shard, 10), ShardState.PENDING_BACKOFF)
        (shard / "lease").mkdir()
        self.assertEqual(shard_state(shard, 10), ShardState.LEASED)
        atomic_write_json(shard / "quarantine.json", {})
        self.assertEqual(shard_state(shard, 10), ShardState.QUARANTINED)
        atomic_write_json(shard / "attempts" / "b" / "complete.json", {})
        self.assertEqual(shard_state(shard, 10), ShardState.DONE)
        atomic_write_json(shard / "superseded.json", {})
        self.assertEqual(shard_state(shard, 10), ShardState.SUPERSEDED)

    def test_cut_claim_complete(self) -> None:
        shard_id = self.ledger.cut("job", "p", 4, 4.0, 100.0)
        claim = self.ledger.claim(
            "pool", ["job"], {"worker_id": "w", "launcher": "local", "handle": "local:1"}, None, None, 101.0
        )
        self.assertIsNotNone(claim)
        records = claim.attempt_dir / "records.jsonl"
        records.write_text("".join(json.dumps({"unit_id": i}) + "\n" for i in range(4)), encoding="utf-8")
        self.ledger.complete(claim, records, 4, 101.0, 2.0, {}, 103.0)
        view = self.ledger.scan_job("job", 104.0)
        self.assertEqual(view.partitions["p"].shards[0].state, ShardState.DONE)
        self.assertEqual(view.partitions["p"].shards[0].shard_id, shard_id)

    def test_two_processes_only_one_claims(self) -> None:
        self.ledger.cut("job", "p", 4, 4.0, 99.0)
        ready = multiprocessing.Event()
        queue = multiprocessing.Queue()
        processes = [multiprocessing.Process(target=_competing_claim, args=(str(self.root), ready, queue)) for _ in range(2)]
        for process in processes:
            process.start()
        ready.set()
        results = [queue.get(timeout=10) for _ in processes]
        for process in processes:
            process.join(timeout=10)
        self.assertEqual(sum(result is not None for result in results), 1)


    def test_claim_leaves_threshold_exceeded_shard_for_scheduler(self) -> None:
        self.ledger.cut("job", "p", 2, 2.0, 100.0)
        worker = {"worker_id": "w", "launcher": "local", "handle": "h"}
        for index in range(3):
            claim = self.ledger.claim("pool", ["job"], worker, None, None, 101.0 + 2 * index)
            self.assertIsNotNone(claim)
            self.ledger.fail(claim, "backend_error", True, "boom", 0, 102.0 + 2 * index)
        self.assertIsNone(self.ledger.claim("pool", ["job"], worker, None, None, 108.0))

    def test_fail_backoff(self) -> None:
        self.ledger.cut("job", "p", 2, 2.0, 100.0)
        claim = self.ledger.claim("pool", ["job"], {"worker_id": "w", "launcher": "local", "handle": "h"}, None, None, 101.0)
        self.ledger.fail(claim, "backend_error", True, "boom", 10, 102.0)
        self.assertEqual(self.ledger.scan_job("job", 105.0).partitions["p"].shards[0].state, ShardState.PENDING_BACKOFF)
        self.assertEqual(self.ledger.scan_job("job", 113.0).partitions["p"].shards[0].state, ShardState.PENDING)

    def test_expire_leases_ttl_and_double_ttl(self) -> None:
        self.ledger.cut("job", "p", 2, 2.0, 90.0)
        claim = self.ledger.claim("pool", ["job"], {"worker_id": "w", "launcher": "local", "handle": "h"}, None, None, 100.0)
        self.assertEqual(self.ledger.expire_leases("job", 10, lambda owner: False, 109.0), [])
        self.assertEqual(self.ledger.expire_leases("job", 10, lambda owner: True, 111.0), [])
        self.assertEqual(self.ledger.expire_leases("job", 10, lambda owner: False, 112.0), [claim.shard_id])
        second = self.ledger.claim("pool", ["job"], {"worker_id": "w2", "launcher": "local", "handle": "h2"}, None, None, 120.0)
        self.assertEqual(self.ledger.expire_leases("job", 10, lambda owner: True, 131.0), [])
        self.assertEqual(self.ledger.expire_leases("job", 10, lambda owner: True, 141.0), [second.shard_id])
        failed = json.loads((claim.attempt_dir / "failed.json").read_text(encoding="utf-8"))
        self.assertEqual(failed["reason"], "lease_expired")

    def test_split_creates_children(self) -> None:
        parent = self.ledger.cut("job", "p", 8, 8.0, 100.0)
        left, right = self.ledger.split("job", parent, 101.0)
        view = self.ledger.scan_job("job", 102.0)
        by_id = {shard.shard_id: shard for shard in view.partitions["p"].shards}
        self.assertEqual(by_id[parent].state, ShardState.SUPERSEDED)
        self.assertEqual((by_id[left].count, by_id[right].count), (4, 4))
        self.assertEqual((by_id[left].depth, by_id[right].depth), (1, 1))

    def test_plan_cut_clamps_and_honors_limit(self) -> None:
        config = {"target_seconds": 100, "min_units": 4, "max_units": 20, "limit_units": None}
        self.assertEqual(plan_cut(100, 0, 10.0, config), 10)
        self.assertEqual(plan_cut(100, 95, 10.0, config), 5)
        config["limit_units"] = 12
        self.assertEqual(plan_cut(100, 10, 10.0, config), 2)
        self.assertEqual(plan_cut(100, 12, 10.0, config), 0)


if __name__ == "__main__":
    unittest.main()
