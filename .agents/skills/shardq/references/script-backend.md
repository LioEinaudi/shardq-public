# Adapting any benchmark with the script backend

The `script` backend (`shardq/backends/script.py`) runs shell commands and needs no
Python from the adapter. Use it whenever the benchmark can evaluate a contiguous index
range on request and emit one record per index.

## Contract

`backend_args` fields:

- `partitions`: `{"<name>": <size>, ...}` static sizes, or
- `describe_command`: shell command whose stdout is a JSON array of
  `{"name": str, "size": int, "unit_seconds_hint": float|null, "traits": {}}`. Names
  must match `[A-Za-z0-9._-]+`.
- `run_command`: shell command executed once per shard with the environment
  `SHARDQ_PARTITION`, `SHARDQ_START`, `SHARDQ_COUNT`, `SHARDQ_OUT_DIR`. It must create
  `$SHARDQ_OUT_DIR/records.jsonl`: one JSON object per line, each with an integer
  `unit_id`, and the set of `unit_id`s must equal exactly
  `range(SHARDQ_START, SHARDQ_START + SHARDQ_COUNT)`. Any other keys are the adapter's
  own (prediction, score, timing). Extra files may go under `$SHARDQ_OUT_DIR`.
- `merge_command` (optional): shell command executed once per partition with `SHARDQ_PARTITION`,
  `SHARDQ_OUT_DIR`, `SHARDQ_RECORDS` (the deduplicated, coverage-checked
  `records.jsonl`), `SHARDQ_META` (`meta.json` with the shards used). It must write
  `$SHARDQ_OUT_DIR/results.json` shaped as
  `{"results": {"<partition>": {"<metric>": value, ...}}}`; ShardQ copies the partition's
  metrics into the backend reply. The published results file retains the command output.
- `session_key` is an optional grouping label, defaulting to `any`. The execution
  key fingerprints the Script backend, this label and the exact `run_command`.
  Identical commands and labels can reuse the session wrapper. Every range still
  starts its own subprocess. Different commands always open separate sessions.

Unit ids are absolute partition indices, `0 .. size - 1`. A non-zero exit from
`run_command` is a `backend_error` (the last 4000 characters of stderr are kept in
`failed.json`); wrong coverage is a `validation_error`. Both count toward
`retry.max_errors` and lead to bisection, so a single bad index isolates itself as a
one-unit quarantined shard while the rest of the partition completes.

Model loading happens inside `run_command` every time. Size shards with
`shard.target_seconds` large enough that load time is a small fraction, or expose the
model as a local server the command talks to, or implement a Python `Backend` with
`open_session` when a persistent process is needed.

## Default summary

Without a merge command, the core publishes the deduplicated UTF-8
`records.jsonl`. The adjacent `results.json` contains `schema_version: 1`,
`job_id`, `partition`, `status` (complete or partial), `expected_units`,
`completed_units` (unique indices), `record_count`, sorted `missing_units`,
and `artifacts.records` with the absolute `path` and `sha256`. All-quarantined
partitions produce an empty records file and a partial summary. See
[the manifest batch](../../../../examples/manifest_script/) for runnable commands.

## Custom aggregation example

`adapter.sh` (must be executable and visible to the worker at this absolute path):

```bash
#!/usr/bin/env bash
set -euo pipefail
case "${1:?mode}" in
  run)
    # Evaluate indices [SHARDQ_START, SHARDQ_START+SHARDQ_COUNT) of partition SHARDQ_PARTITION.
    python my_eval.py --split "$SHARDQ_PARTITION" --start "$SHARDQ_START" \
      --count "$SHARDQ_COUNT" --out "$SHARDQ_OUT_DIR/records.jsonl"
    ;;
  merge)
    python - <<'PY'
import json, os
rows = [json.loads(l) for l in open(os.environ["SHARDQ_RECORDS"]) if l.strip()]
acc = sum(r["correct"] for r in rows) / len(rows)
json.dump({"results": {os.environ["SHARDQ_PARTITION"]: {"acc": acc, "n": len(rows)}}},
          open(os.path.join(os.environ["SHARDQ_OUT_DIR"], "results.json"), "w"))
PY
    ;;
esac
```

Job spec:

```json
{
  "job_id": "mybench-v1-full",
  "backend": "script",
  "pool": "local-script",
  "protocol_id": "mybench_v1",
  "priority": 0,
  "backend_args": {
    "partitions": {"val": 1200, "test": 3400},
    "run_command": "/abs/path/adapter.sh run",
    "merge_command": "/abs/path/adapter.sh merge"
  },
  "shard": {"target_seconds": 1200, "min_units": 8, "max_units": 256, "limit_units": null, "lookahead": 4},
  "retry": {"max_errors": 3, "max_preemptions": 20, "backoff_seconds": 120, "bisect": true, "lease_ttl_seconds": 2400}
}
```

Pool entry (pools file):

```json
"local-script": {"backend": "script", "entry": "", "launcher": "local", "volc": {}, "env": {},
                 "local_python": null, "max_workers": 2, "worker_max_wall_seconds": 7200,
                 "worker_idle_exit_seconds": 60, "extra": {"column": "mybench"}}
```

For Volc, switch `launcher` to `volc`, fill `volc` (queue, flavor, image, vepfs_id,
capacity settings), and point `entry` at a copy of `examples/lmms-worker.sh` whose
`SHARDQ_PRE_HOOK` activates the benchmark's environment.

Dry run the whole thing locally first with the fake backend
(`examples/fake-demo.json` plus `examples/pools.example.json`), then with
`shard.limit_units` set to a few dozen units of the real benchmark.
