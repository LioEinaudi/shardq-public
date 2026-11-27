from __future__ import annotations

import fcntl
import multiprocessing
import os
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from shardq import ledger as ledger_module
from shardq.backends.fake import FakeBackend
from shardq.backends.script import ScriptBackend
from shardq.ledger import Ledger, atomic_write_json, read_json
from shardq.merge import select_attempts
from shardq.worker import run_worker


def _claim(root, worker, now):
    return Ledger(root).claim("pool", ["job"], {"worker_id": worker}, None, None, now)


def _die_before_owner(root):
    def exit_on_owner(path, payload):
        if path.name == "owner.json":
            os._exit(17)
        atomic_write_json(path, payload)

    with patch.object(ledger_module, "atomic_write_json", exit_on_owner):
        _claim(root, "crashed", 100)


def _compete(root, worker, barrier, results):
    original = Ledger.scan_job
    initial_scan = True

    def scanned(ledger, job_id, now):
        nonlocal initial_scan
        view = original(ledger, job_id, now)
        if initial_scan:
            initial_scan = False
            barrier.wait(timeout=10)
        return view

    with patch.object(Ledger, "scan_job", scanned):
        claim = _claim(root, worker, 100)
    results.put(claim.attempt_id if claim else None)


class LeaseRecoveryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.ledger = Ledger(self.root)
        job = self.ledger.job_dir("job")
        atomic_write_json(job / "job.json", {
            "job_id": "job", "pool": "pool", "backend": "fake", "session_key": "any",
            "pool_snapshot": {"launcher": "local", "backend": "fake"},
            "retry": {"lease_ttl_seconds": 2}, "backend_args": {"unit_seconds": 0},
        })
        partition = job / "partitions" / "p"
        atomic_write_json(partition / "partition.json", {"name": "p", "size": 1})
        self.shard_id = self.ledger.cut("job", "p", 1, 1, 100)
        self.shard = partition / "shards" / self.shard_id
        self.lease = self.shard / "lease"

    def expire(self, now):
        return self.ledger.expire_leases("job", 10, lambda owner: False, now)

    def complete(self, claim, now):
        records = claim.attempt_dir / "records.jsonl"
        records.write_text('{"unit_id":0}\n', encoding="utf-8")
        self.ledger.complete(claim, records, 1, now - 1, 1, {}, now)

    def test_claim_exit_before_owner(self):
        process = multiprocessing.Process(target=_die_before_owner, args=(self.root,))
        process.start()
        process.join(timeout=10)
        self.assertEqual(process.exitcode, 17)
        self.expire(time.time() + 21)
        self.assertIsNotNone(_claim(self.root, "replacement", time.time() + 22))

    def test_owner_and_initial_heartbeat_visible_together(self):
        def before_write(path, contents):
            if path.name == "owner.json":
                self.assertFalse(self.lease.exists())
            atomic_write_json(path, contents)

        with patch.object(ledger_module, "atomic_write_json", before_write):
            claim = _claim(self.root, "a", 100)
        self.assertEqual(read_json(self.lease / "owner.json")["attempt_id"], claim.attempt_id)
        self.assertEqual(float((self.lease / "heartbeat").read_text()), 100)

    def test_script_output_named_heartbeat_is_independent(self):
        claim = _claim(self.root, "w", 100)
        command = """echo 0 > "$SHARDQ_OUT_DIR/heartbeat"
echo '{"unit_id":0}' > "$SHARDQ_OUT_DIR/records.jsonl" """
        session = ScriptBackend({"run_command": command}, {}, {}).open_session()
        session.run_range("p", 0, 1, claim.attempt_dir)
        self.assertEqual(self.expire(101), [])
        self.ledger.heartbeat(claim, 102)
        self.assertEqual((claim.attempt_dir / "heartbeat").read_text().strip(), "0")

    def test_new_worker_lease_is_fresh_after_slow_claim_preparation(self):
        atomic_write_json(self.root / "workers" / "w.json", {"worker_id": "w"})
        original = FakeBackend.open_session
        expired = []
        with patch("time.time", return_value=100) as clock:
            def slow_write(path, payload):
                atomic_write_json(path, payload)
                if path.name == "owner.json":
                    clock.return_value = 130

            def open_session(backend):
                expired.extend(self.expire(clock()))
                return original(backend)

            with patch.object(ledger_module, "atomic_write_json", slow_write), \
                 patch.object(FakeBackend, "open_session", open_session), \
                 patch("shardq.worker._heartbeat_loop", lambda ledger, claim, stop, interval: stop.wait()):
                self.assertEqual(run_worker(self.root, "pool", "w", 60, 0), 0)
        self.assertEqual(expired, [])

    def test_legacy_ownerless_and_corrupt_leases_get_grace(self):
        for bad_owner in (None, b"{", b"[]", b'{"attempt_id":"../unsafe"}'):
            with self.subTest(bad_owner=bad_owner):
                self.lease.mkdir(exist_ok=True)
                if bad_owner is not None:
                    (self.lease / "owner.json").write_bytes(bad_owner)
                created = self.lease.stat().st_mtime
                self.assertEqual(self.expire(created + 11), [])
                self.assertTrue(self.lease.is_dir())
                self.assertEqual(self.expire(created + 21), [self.shard_id])
                self.assertFalse(self.lease.exists())
                self.assertEqual(list(self.shard.glob("attempts/*/failed.json")), [])

    def test_unreadable_legacy_owner_gets_grace_then_releases_lease(self):
        self.lease.mkdir()
        owner_file = self.lease / "owner.json"
        atomic_write_json(owner_file, {"attempt_id": "old-100", "acquired_unix": 100})
        created = owner_file.stat().st_mtime
        original_open = Path.open

        def unreadable(path, *args, **kwargs):
            if path == owner_file:
                raise PermissionError("owner is unreadable")
            return original_open(path, *args, **kwargs)

        with patch.object(Path, "open", unreadable):
            self.assertEqual(self.expire(created + 11), [])
            self.assertEqual(self.expire(created + 31), [self.shard_id])
            self.assertEqual(self.expire(created + 41), [])
        self.assertFalse(self.lease.exists())
        self.assertEqual(list(self.shard.glob("attempts/*/failed.json")), [])
        self.assertIsNotNone(_claim(self.root, "replacement", created + 42))

    def test_expiry_skips_busy_lease_and_continues_other_shards(self):
        first = _claim(self.root, "first", 100)
        self.ledger.update_partition(self.shard.parent.parent / "partition.json", size=2)
        second_id = self.ledger.cut("job", "p", 1, 1, 100)
        second = _claim(self.root, "second", 100)
        with ThreadPoolExecutor(max_workers=1) as executor:
            with (self.shard / "lease.lock").open("a") as handle:
                fcntl.flock(handle, fcntl.LOCK_EX)
                pending = executor.submit(self.expire, 135)
                try:
                    expired = pending.result(timeout=2)
                finally:
                    fcntl.flock(handle, fcntl.LOCK_UN)
        self.assertEqual(expired, [second_id])
        self.assertTrue(self.lease.exists())
        self.assertFalse((first.attempt_dir / "failed.json").exists())
        self.assertEqual(read_json(second.attempt_dir / "failed.json")["reason"], "lease_expired")
        self.assertEqual(self.expire(136), [self.shard_id])

    def competing_claims(self, count):
        barrier = multiprocessing.Barrier(count)
        results = multiprocessing.Queue()
        processes = [multiprocessing.Process(target=_compete, args=(self.root, str(i), barrier, results))
                     for i in range(count)]
        for process in processes:
            process.start()
        winners = [result for result in (results.get(timeout=10) for _ in processes) if result]
        for process in processes:
            process.join(timeout=10)
            self.assertEqual(process.exitcode, 0)
        return winners

    def test_competing_processes_with_same_pending_snapshot(self):
        winners = self.competing_claims(3)
        self.assertEqual(winners, [read_json(self.lease / "owner.json")["attempt_id"]])

    def test_competing_claims_respect_partition_parallel_limit(self):
        spec_path = self.ledger.job_dir("job") / "job.json"
        spec = read_json(spec_path)
        spec["traits"] = {"p": {"max_parallel": 1}}
        atomic_write_json(spec_path, spec)
        self.ledger.update_partition(self.shard.parent.parent / "partition.json", size=2)
        self.ledger.cut("job", "p", 1, 1, 100)
        self.assertEqual(len(self.competing_claims(2)), 1)
        self.assertEqual(self.ledger.scan_job("job", 100).partitions["p"].leased, 1)

    def race_at(self, mod, name, is_boundary, first, contender):
        # Stop the first actor at the write/unlink. Let the contender run until
        # it finishes or the kernel reports it would block on the first actor.
        # This exercises both unguarded and lock-serialized implementations.
        progressed = threading.Event()
        original = getattr(mod, name)
        flock = fcntl.flock
        main = threading.current_thread()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = None

            def start_contender(*args, **kwargs):
                nonlocal future
                if threading.current_thread() is main and future is None and is_boundary(args[0]):
                    future = executor.submit(contender)
                    future.add_done_callback(lambda finished: progressed.set())
                    self.assertTrue(progressed.wait(timeout=5))
                return original(*args, **kwargs)

            def watch_contention(fd, flags):
                if threading.current_thread() is not main and flags == fcntl.LOCK_EX:
                    try:
                        return flock(fd, flags | fcntl.LOCK_NB)
                    except BlockingIOError:
                        progressed.set()
                return flock(fd, flags)

            with patch.object(mod, name, start_contender), patch.object(fcntl, "flock", watch_contention):
                first()
                self.assertIsNotNone(future, "the I/O boundary was not reached")
                return future.result(timeout=5)

    def test_heartbeat_interleaved_with_expiry_cannot_revive_lease(self):
        claim = _claim(self.root, "a", 100)
        expired = self.race_at(
            ledger_module, "_atomic_write_text", lambda path: path.name == "heartbeat",
            lambda: self.ledger.heartbeat(claim, 101), lambda: self.expire(130),
        )
        self.assertEqual(expired, [])
        self.assertEqual(self.expire(130), [claim.shard_id])
        self.assertFalse(self.lease.exists())

    def test_release_interleaved_with_replacement_preserves_new_owner(self):
        claim = _claim(self.root, "a", 100)
        # Exercise release of a pre-upgrade physical lease directory.
        if self.lease.is_symlink():
            files = {name: (self.lease / name).read_bytes() for name in ("owner.json", "heartbeat")}
            self.lease.unlink()
            self.lease.mkdir()
            for name, content in files.items():
                (self.lease / name).write_bytes(content)

        def replace():
            self.expire(130)
            return _claim(self.root, "b", 131)

        replacement = self.race_at(
            shutil, "rmtree", lambda path: Path(path) == self.lease,
            lambda: self.ledger.fail(claim, "preempted", True, "", 0, 102), replace,
        )
        self.assertIsNone(replacement)
        replacement = _claim(self.root, "b", 131)
        self.assertIsNotNone(replacement)
        self.ledger.heartbeat(replacement, 132)
        self.assertEqual(read_json(self.lease / "owner.json")["attempt_id"], replacement.attempt_id)

    def test_expiry_rechecks_heartbeat_after_liveness_query(self):
        claim = _claim(self.root, "a", 100)

        def query(owner):
            self.ledger.heartbeat(claim, 111)
            return False

        self.assertEqual(self.ledger.expire_leases("job", 10, query, 112), [])
        self.ledger.heartbeat(claim, 113)
        self.assertFalse((claim.attempt_dir / "failed.json").exists())

    def test_late_complete_and_replacement_tie_use_existing_winner_order(self):
        old = _claim(self.root, "a", 100)
        self.expire(125)
        new = _claim(self.root, "b", 126)
        self.complete(old, 130)
        self.ledger.heartbeat(new, 131)
        self.complete(new, 130)
        partition = self.ledger.scan_job("job", 132).partitions["p"]
        winner, = select_attempts(partition, self.shard.parent)
        self.assertEqual(winner["attempt_id"], old.attempt_id)

    def test_dead_replacement_expires_after_late_completion(self):
        old = _claim(self.root, "a", 100)
        self.expire(125)
        replacement = _claim(self.root, "b", 126)
        self.complete(old, 130)
        self.assertEqual(self.expire(150), [self.shard_id])
        self.assertFalse(self.lease.exists())
        self.assertEqual(read_json(replacement.attempt_dir / "failed.json")["reason"], "lease_expired")
        self.assertEqual(self.ledger.scan_job("job", 151).partitions["p"].done, 1)

    def test_open_session_is_covered_by_heartbeat_at_short_job_ttl(self):
        atomic_write_json(self.root / "workers" / "w.json", {"worker_id": "w"})
        ready = threading.Event()
        heartbeats = []
        original = FakeBackend.open_session

        def heartbeat_loop(ledger, claim, stop, seconds):
            acquired = read_json(claim.attempt_dir / "owner.json")["acquired_unix"]
            ledger.heartbeat(claim, acquired + 100)
            heartbeats.append((acquired, seconds))
            ready.set()
            stop.wait(timeout=5)

        def open_session(backend):
            self.assertTrue(ready.wait(timeout=1))
            acquired, interval = heartbeats[0]
            self.assertLess(interval, 2)
            self.assertEqual(self.ledger.expire_leases("job", 2, lambda owner: False, acquired + 101), [])
            return original(backend)

        with patch("shardq.worker._heartbeat_loop", heartbeat_loop), patch.object(FakeBackend, "open_session", open_session):
            self.assertEqual(run_worker(self.root, "pool", "w", 60, 0), 0)
        self.assertEqual(self.ledger.scan_job("job", time.time()).partitions["p"].done, 1)
