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
jobs/<job>/setup_epoch.json                  # created by resume
jobs/<job>/partitions/<partition>/partition.json
jobs/<job>/partitions/<partition>/partition.lock
jobs/<job>/partitions/<partition>/shards/<shard>/
  shard.json
  lease.lock
  lease -> .lease-owner
  .lease-owner/{owner.json,heartbeat}
  attempts/<attempt>/{owner.json,records.jsonl,raw/,complete.json,failed.json}
  quarantine.json
  superseded.json
jobs/<job>/merged/<partition>/{results.json,records.jsonl,merge.json}
jobs/<job>/markers/<column>/<partition>.complete.json|partial.json
workers/<worker>.json
scheduler/{lock,state.json,status.md,log/scheduler.log}
history.json
```

A shard's state follows the first matching condition in this order.

1. A superseded marker gives `SUPERSEDED`.
2. A completed attempt gives `DONE`.
3. A quarantine marker gives `QUARANTINED`.
4. A current lease entry gives `LEASED`.
5. The newest range failure's retry time selects `PENDING_BACKOFF` or `PENDING`.

**Lease publication and release.** Each writer opens the shard's permanent
`lease.lock` file and takes an exclusive `flock` for the mutation. Claim checks
the shard again under the lock and creates a fresh attempt directory with an
audit copy of the owner. While there is no current lease, it prepares the live
owner and initial Unix heartbeat under the private shard directory
`.lease-owner/`. A backend may write its own `heartbeat` file in its attempt
output directory. Successful creation of the relative symlink
`lease -> .lease-owner` is the claim's linearization point. An existing
destination makes creation fail. A crash before publication can leave an unused
attempt directory and staging files. The next claimant initializes the private
files under the same lock before publishing its own generation.

Heartbeats verify the attempt ID under that same lock and atomically replace
the heartbeat file. For release, the identity check and unlink occur within
one lock acquisition. Expiry checks launcher liveness outside the lock and
rechecks the owner and heartbeat under it. Locks have stable inodes across
lease generations and are released by closing the file descriptor or process
exit. Legacy physical lease directories use the same lock for removal. An
unreadable or invalid owner gets a grace period of two TTLs from the latest
lease-entry or owner-file modification. Recovery of an unidentified lease
leaves sample and preemption budgets intact.

Completion publishes the attempt's marker even when its lease has expired.
Only the current generation can release its lease. Merge chooses the minimum
`(completed_unix, attempt_id)` per eligible shard and verifies the records'
SHA-256. A late completion can mark a shard DONE while a replacement still
holds its lease. Expiry and scheduler drain include that physical lease.

**Filesystem contract.** All writers need mutually visible POSIX namespace
operations, cross-client exclusive `flock` on the same inode, atomic symlink
creation/unlink and same-directory file rename, plus clocks suitable for the
configured TTL. Published files use unique temporary names in the destination
directory and clean them up on ordinary errors. Process death can leave an
unpublished temporary file. Unique names prevent collisions. Protecting a
read-modify-write also requires serializing the read and the write, as the
partition lock does for cutting, updating the runtime estimate and publishing
merge status. The recovery contract covers process exit. Power-loss durability
depends on the storage system. `selftest-fs` exercises local-process mkdir
contention on the selected mount. Validate lock visibility, namespace coherence
and client-death recovery between deployment hosts before using a shared mount.

`read_json` returns its default for ledger files deleted during a read or
containing incomplete content; the next tick re-derives state so transient
scan results recover. An exception from an individual scheduler tick is logged
with its traceback to `<root>/scheduler/log/scheduler.log`, then the loop
waits for the poll interval and retries. Only `--once` propagates the exception
to the CLI.

Upgrade an existing ledger in place after draining all old writers, including
queued workers. Mixed-version writers are unsupported because old workers
bypass the shard and partition locks. Old `pool_snapshot` objects with
top-level `column` and `lmms_root` are normalized when a worker must recover
configuration from a snapshot. The attempt and job formats accept absent
setup fields. Old splits, quarantines and completed results retain their state.

## Worker loop

A worker claims from unstopped jobs matching its pool. Candidate ordering keeps
partition and session locality, then considers job priority, remaining shards,
prior failures, and start unit. The registered key still guides that ordering
and keys runtime history. The worker independently derives its reuse identity
from the job's backend and the effective settings captured by the session.
Script hashes the run command and its optional grouping label, Fake includes
the unit duration and poison configuration, and lmms includes model settings,
evaluation settings and pinned source. New registrations persist the new keys.
History for a newly keyed configuration starts from its normal estimate.
Python retains imported modules across session closes. A worker that already
loaded a different lmms source pin records `worker_restart`, releases the claim
and exits. The next worker can load that pin in a fresh interpreter. This
handoff consumes no retry budget. On first load, importing lmms from outside
the requested root produces a setup error. Keep a verify-mode checkout at its
registered revision until the job finishes.
A completed range must cover exactly its absolute
`unit_id` interval before `complete.json` is written. SIGTERM and SIGINT
write a `preempted` failure record and exit.

## Scheduler tick

Each tick refreshes launcher state, expires leases whose owners are gone,
bisects or quarantines over-limit shards, updates the runtime EMA, maintains
lookahead cuts, merges terminal partitions, and finally submits workers for
remaining demand. A non-blocking `flock` on `scheduler/lock` enforces one
scheduler per ledger.

A lease can expire after one TTL when the launcher no longer reports a live
worker, or after two TTLs regardless of launcher state. Heartbeats run through
session setup, execution and validation at most half a TTL apart by default.
Preemption-like failures count toward `max_preemptions`. Backend range and
validation failures count toward `max_errors`. A terminal partition with missing quarantined units can only
produce a `partial` marker. `--require-complete` refuses that merge.

**Session setup budget.** Backend construction or session initialization
failures produce auditable `session_error` attempts. The scheduler and workers
read the current job-wide setup epoch and aggregate matching failures across
all its shards. `session_retry_after_unix` is the greatest retry time among
those failures. The job enters `backoff` until that time, or `paused` once
the cumulative count reaches `retry.max_session_errors` (default 3).
The delay is `retry.backoff_seconds` (default 300 seconds). Blocked jobs
contribute no new worker demand. Workers already dispatched can report more
failures. Other jobs in the pool continue to supply work.

`shardq resume --job JOB` starts a fresh setup epoch and clears a manual STOP.
Each claim captures its epoch so failures from earlier claims stay in their
original budget. Missing epochs denote the initial cycle, including earlier
`session_error` records. The CLI and scheduler expose `setup_status`,
`session_errors` and `session_retry_after_unix`. Restarting the scheduler
reconstructs all three from the ledger. A batch scheduler waits through
temporary backoff and may exit with paused jobs once workers and leases drain.

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
