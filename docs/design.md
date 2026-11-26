# ShardQ design

## Purpose

ShardQ cuts a benchmark's leaf partitions into contiguous ranges sized by
estimated runtime. Short-lived workers claim ranges from a shared filesystem
ledger, write per-unit records, and may disappear without holding authoritative
in-memory state. Repeated failures are bisected until a poison unit is isolated
as an explicit data gap.

ShardQ does not alter protocol identity. Every job declares `protocol_id`.
Backend pins record the execution source needed for reproducibility. The
optional lmms backend expands task groups to leaf tasks at registration and
does not synthesize group-level metrics.

## Components

```text
job spec -> register -> job.json + partition.json
                            |
                       scheduler tick
                  cut / expire / split / merge
                            |
             +--------------+--------------+
             | shared filesystem ledger    |
             +--------------+--------------+
                            |
                    stateless workers
                            |
                  attempt records + raw data
                            |
                 merged results + markers
```

- `spec.py` validates pool/job configuration and resolves ledger and pools
  paths.
- `ledger.py` owns atomic claim, heartbeat, attempt terminals, lease expiry,
  bisection, and quarantine.
- `controller.py` reconstructs state from files on every tick and adds workers
  when demand exists.
- `worker.py` reuses compatible backend sessions and writes one attempt at a
  time.
- `backends/` isolates partition discovery, range execution, validation,
  aggregation, and source pins.
- `launchers/` isolates local or cloud worker lifecycles.
- `merge.py` deterministically chooses completed attempts, deduplicates
  records, checks coverage, and writes results and markers.

## Ledger and state semantics

The root is always supplied by `--root` or `SHARDQ_ROOT`. Its format is:

```text
jobs/<job>/job.json
jobs/<job>/partitions/<partition>/partition.json
jobs/<job>/partitions/<partition>/partition.lock
jobs/<job>/partitions/<partition>/shards/<shard>/
  shard.json
  lease/{owner.json,heartbeat}
  attempts/<attempt>/{records.jsonl,raw/,complete.json|failed.json}
  quarantine.json
  superseded.json
jobs/<job>/merged/<partition>/{results.json,records.jsonl,merge.json}
jobs/<job>/markers/<column>/<partition>.complete.json|partial.json
workers/<worker>.json
scheduler/{lock,state.json,status.md,log/scheduler.log}
history.json
```

A shard's state is derived in this strict order:
`superseded.json -> SUPERSEDED`; any `complete.json -> DONE`;
`quarantine.json -> QUARANTINED`; `lease/ -> LEASED`; otherwise the newest
failure's `retry_after_unix` selects `PENDING_BACKOFF` or `PENDING`.
`lease/` is acquired with atomic `os.mkdir`. Heartbeats contain Unix seconds
and do not depend on filesystem mtime.

Published files use unique temporary names in the destination directory and
clean them up on ordinary errors. Process death can leave an unpublished
temporary file.
The partition lock serializes the full read-modify-write when cutting, updating
the runtime estimate and publishing merge status.

`read_json` returns its default for ledger files deleted during a read or
containing incomplete content; the next tick re-derives state so transient
scan results recover. An exception from an individual scheduler tick is logged
with its traceback to `<root>/scheduler/log/scheduler.log`, then the loop
waits for the poll interval and retries. Only `--once` propagates the exception
to the CLI.

Existing ledgers use this same schema. Old `pool_snapshot` objects with
top-level `column` and `lmms_root` are normalized when a worker must recover
configuration from a snapshot.

## Worker loop

A worker claims from unstopped jobs matching its pool. Candidate ordering keeps
partition and session locality, then considers job priority, remaining shards,
prior failures, and start unit. A backend session is rebuilt only when
`session_key` changes. A completed range must cover exactly its absolute
`unit_id` interval before `complete.json` is written. SIGTERM and SIGINT
write a `preempted` failure record and exit.

## Scheduler tick

Each tick refreshes launcher state, expires leases whose owners are gone,
bisects or quarantines over-limit shards, updates the runtime EMA, maintains
lookahead cuts, merges terminal partitions, and finally submits workers for
remaining demand. A non-blocking `flock` on `scheduler/lock` enforces one
scheduler per ledger.

A lease returns to pending when it is older than its TTL and the launcher is no
longer alive; at twice the TTL it returns unconditionally. Preemption-like
failures count toward `max_preemptions`; other failures count toward
`max_errors`. A terminal partition with missing quarantined units can only
produce a `partial` marker. `--require-complete` refuses that merge.

## Pool configuration

A pools file has a top-level `pools` object. Each pool contains:

- `backend`: built-in name or `module:Class`
- `entry`: absolute container entry script; ignored by the local launcher
- `launcher`: `local` or `volc`
- `volc`: queue, flavor, `gpu_slots`, image, preemptible, priority,
  `vepfs_id`, capacity mode, and capacity fields
- `env`: worker environment additions
- `local_python`: optional interpreter for backend describe/merge operations
- `max_workers`, `worker_max_wall_seconds`,
  `worker_idle_exit_seconds`
- `extra`: backend-specific data; lmms uses `lmms_root`, and `column`
  defaults to the pool name when absent

For Volc, `capacity_mode=probe` returns unknown capacity and gates submission
with `max_queued`. `capacity_mode=audit` subtracts all requested
non-terminal GPU slots for `creator` from `gpu_cap`; unknown flavors are
conservatively counted as eight GPUs per replica.

## Job specification

Job JSON retains `job_id`, `backend`, `pool`, `protocol_id`, `priority`,
`backend_args`, `shard`, `retry`, and optional per-partition `traits`.
Shard configuration contains target seconds, min/max/limit units and lookahead.
Retry configuration contains error/preemption limits, backoff, bisection, and
lease TTL.

Registration stores a pool snapshot, source file checksum, session key, pins,
and the resolved marker `column`. The lmms pin hook records source root,
resolved root, branch, upstream, remote, commit, dirty state, pin mode, and a
resolved-import proof.

## CLI

```bash
python -m shardq register SPEC.json [--pin-mode worktree|verify]
python -m shardq scheduler [--launcher local|volc] [--exit-when-idle]
python -m shardq worker --pool POOL
python -m shardq status [--job JOB] [--json]
python -m shardq merge --job JOB [--partition TASK] [--allow-partial]
python -m shardq split --job JOB --shard SHARD
python -m shardq stop|resume --job JOB
python -m shardq publish --job JOB --out-dir DIR
python -m shardq selftest-fs --root PATH
python -m shardq lmms-merge-check --local-python PYTHON \
  --lmms-root ROOT --samples SAMPLES --results RESULTS
```

All commands accept `--root` and `--pools`. Root resolution is CLI, then
`SHARDQ_ROOT`, then error. Pools resolution is CLI, then `SHARDQ_POOLS`, then
`<root>/pools.json` when it exists, then error.

`publish` writes a self-contained evidence directory: `run.json`,
`artifacts.json`, `acceptance.json`, `summary.md`, and marker copies under
`state/full/<column>/`.

## Adding a backend

Subclass `Backend` and implement `describe`, `session_key`, `open_session`,
`validate_range`, and `merge`. Override the class-level
`pins(pool, root, job_id, pin_mode)` hook only when registration must capture
backend source identity. If pins create resources, optionally override
`preview_pins` to keep `register --dry-run` non-mutating. Configure the class
as `your_module:YourBackend`. Backend imports should stay lazy so importing
ShardQ remains dependency-free.

## Adding a cloud

Subclass `Launcher` and implement `submit`, `status`, and `kill`.
`capacity_slots` may return an integer or `None` when capacity cannot be
audited. Register the launcher name in `launchers.get_launcher`. Launchers
manage workers only; shard truth always remains in the shared ledger.
