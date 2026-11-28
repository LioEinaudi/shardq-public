"""Local POSIX ledger microbenchmark. Run with PYTHONPATH set to a ShardQ checkout."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from shardq.controller import tick
from shardq.ledger import Ledger, atomic_write_json
from shardq.merge import merge_partition
from shardq.register import register
from shardq.spec import load_pools


# (historical jobs, shards per job, attempts per shard, units per merge shard, bytes per record)
SCALES = {
    "small": (3, 3, 1, 128, 128),
    "large": (12, 8, 2, 2048, 512),
    "spill": (3, 2, 2, 8192, 1024),
}
NOW = 1700000000
CUSTOM = """printf '{"results":{}}' > "$SHARDQ_OUT_DIR/results.json" """


def install_job(root, pools, job_id, shards, units, attempts, payload_size, mode, active=False, ordered=False):
    ledger = Ledger(root)
    args = {"partitions": {"files": shards * units}, "run_command": "true"}
    if mode == "custom":
        args["merge_command"] = CUSTOM
    path = root / "spec.json"
    atomic_write_json(path, {"job_id": job_id, "pool": "p", "backend": "script",
                             "protocol_id": "synthetic-v1", "backend_args": args})
    register(root, path, pools=pools)
    for index in range(shards):
        sid = ledger.cut(job_id, "files", units, units, NOW)
        if active:
            continue
        attempts_dir = ledger.job_dir(job_id) / "partitions" / "files" / "shards" / sid / "attempts"
        for attempt in range(attempts):
            directory = attempts_dir / f"w-{attempt}"
            directory.mkdir(parents=True)
            records = directory / "records.jsonl"
            digest = hashlib.sha256()
            with records.open("wb") as output:
                offsets = range(units) if ordered else reversed(range(units))
                for offset in offsets:
                    unit = index * units + offset
                    for line_no, label in enumerate(("b", "a", "b")):
                        row = {"unit_id": unit, "filter": label, "line": line_no,
                               "attempt": attempt, "text": "x" * payload_size}
                        line = (json.dumps(row, separators=(",", ":")) + "\n").encode()
                        output.write(line)
                        digest.update(line)
            atomic_write_json(directory / "complete.json", {
                "attempt_id": directory.name, "completed_unix": NOW + attempt,
                "count": units, "elapsed_seconds": units, "records_sha256": digest.hexdigest(),
            })


class Counts:
    """CPython audit events for read-open attempts and directory enumerations inside the fixture."""

    def __init__(self, root):
        self.prefix = str(root) + os.sep
        self.opens = self.scans = 0

    def __call__(self, event, args):
        if event not in ("open", "os.listdir", "os.scandir") or not isinstance(args[0], str):
            return
        if not args[0].startswith(self.prefix):
            return
        if event == "open":
            self.opens += int((args[2] & os.O_ACCMODE) == os.O_RDONLY)
        else:
            self.scans += 1


def measured_merge(root, probe):
    pools = load_pools(root / "pools.json")
    ledger = Ledger(root)
    job = ledger.scan_job("merge", NOW)
    if probe:
        counter = Counts(root)
        sys.addaudithook(counter)
    start = time.perf_counter()
    result = merge_partition(root, job, "files", pools["p"], False, NOW)
    seconds = time.perf_counter() - start
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        rss /= 1024
    measurements = {"seconds": seconds, "peak_rss_mib": rss / 1024}
    if probe:
        measurements.update(read_opens=counter.opens, directory_scans=counter.scans)
    assert not result["missing_units"] and result["effective"] == job.partitions["files"].size
    print(json.dumps(measurements))


def verify_merge(root, units):
    output = root / "jobs/merge/merged/files/records.jsonl"
    digest = hashlib.sha256()
    with output.open("rb") as handle:
        for unit in range(units):
            for line_no, label in enumerate(("b", "a")):
                line = next(handle)
                digest.update(line)
                row = json.loads(line)
                assert (row["unit_id"], row["filter"], row["line"], row["attempt"]) == (unit, label, line_no, 0)
        assert not handle.readline()
    return {"records": units * 2, "output_bytes": output.stat().st_size, "sha256": digest.hexdigest()}


def measure(root, scale, mode, repeats, ordered):
    history_jobs, shards, attempts, units, payload_size = SCALES[scale]
    atomic_write_json(root / "pools.json", {"pools": {"p": {"backend": "script", "launcher": "local"}}})
    pools = load_pools(root / "pools.json")
    ledger = Ledger(root)
    for i in range(history_jobs):
        name = f"history-{i}"
        install_job(root, pools, name, shards, 1, attempts, 8, mode, ordered=ordered)
        merge_partition(root, ledger.scan_job(name, NOW), "files", pools["p"], False, NOW)
    install_job(root, pools, "active", shards, 1, 0, 0, mode, active=True)
    opts = {"max_submits_per_tick": 0}
    def run_tick(step):
        start = time.perf_counter()
        state = tick(root, "local", pools, opts, NOW + 100 + step)
        assert len(state["jobs"]) == history_jobs + 1
        return time.perf_counter() - start
    run_tick(0)
    tick_ms = statistics.median(run_tick(i + 1) for i in range(repeats)) * 1000
    counter = Counts(root)
    sys.addaudithook(counter)
    run_tick(repeats + 1)
    tick_stats = {"ms": round(tick_ms, 2), "read_opens": counter.opens, "directory_scans": counter.scans}
    install_job(root, pools, "merge", shards, units, attempts, payload_size, mode, ordered=ordered)
    # Every timed merge runs in a fresh POSIX process. Probe I/O separately.
    def child(probe=False):
        cmd = [sys.executable, __file__, "--child", str(root)]
        if probe:
            cmd.append("--probe")
        return json.loads(subprocess.check_output(cmd, text=True))
    child()
    values = [child() for _ in range(repeats)]
    io = child(True)
    merge_stats = {
        "ms": round(statistics.median(row["seconds"] for row in values) * 1000, 2),
        "peak_rss_mib": round(statistics.median(row["peak_rss_mib"] for row in values), 2),
        "read_opens": io["read_opens"], "directory_scans": io["directory_scans"],
        **verify_merge(root, shards * units),
    }
    return {"scale": scale, "mode": mode, "ordered": ordered, "repeats": repeats,
            "tick": tick_stats, "merge": merge_stats}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--probe", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--scale", choices=SCALES, default="small")
    parser.add_argument("--mode", choices=("custom", "default"), default="custom")
    parser.add_argument("--ordered", action="store_true", help="write input units ascending (default: descending)")
    parser.add_argument("--repeats", type=int, default=3)
    options = parser.parse_args()
    if options.child:
        measured_merge(options.child, options.probe)
    else:
        with tempfile.TemporaryDirectory(prefix="shardq-bench-") as tmp:
            result = measure(Path(tmp).resolve(), options.scale, options.mode, options.repeats, options.ordered)
        print(json.dumps({**result, "python": platform.python_version(), "platform": platform.platform()}))
