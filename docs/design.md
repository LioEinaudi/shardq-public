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
jobs/<job>/setup.lock
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
workers/<worker>.starts/<process-receipt>.json
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
`lease.lock` file and takes an exclusive `flock` for the mutation. Claim also
takes the job's `setup.lock`, then rechecks job setup, the partition's
`max_parallel` limit and shard eligibility before creating a fresh attempt
directory with an audit copy of the owner. While there is no current lease, it
prepares the live owner and initial Unix heartbeat under the private shard directory
`.lease-owner/`. A backend may write its own `heartbeat` file in its attempt
output directory. Successful creation of the relative symlink
`lease -> .lease-owner` is the claim's linearization point. An existing
destination makes creation fail. A crash before publication can leave an unused
attempt directory and staging files. The next claimant initializes the private
files under the same lock before publishing its own generation.

Heartbeats verify the attempt ID under that same lock and atomically replace
the heartbeat file. For release, the identity check and unlink occur within
one lock acquisition. Expiry checks launcher liveness outside the lock and
tries to acquire it without waiting. A busy lease is left for a later tick,
so a suspended worker cannot stall expiry of other shards. After acquiring the
lock, expiry rechecks the owner and heartbeat. Locks have stable inodes across
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
bypass the job, shard and partition locks. Old `pool_snapshot` objects with
top-level `column` and `lmms_root` are normalized when a worker must recover
configuration from a snapshot. The attempt and job formats accept absent
setup fields. Old splits, quarantines and completed results retain their state.

## Worker loop

A worker claims from unstopped jobs whose setup is ready and whose pool matches.
Candidate ordering keeps partition and session locality, then considers job
priority, remaining shards, prior failures, and start unit. The registered key still guides that ordering
and keys runtime history. The worker independently derives its reuse identity
from the job's backend and the effective settings captured by the session.
Script hashes the run command and its optional grouping label, Fake includes
the unit duration and poison configuration, and lmms includes model settings,
evaluation settings and pinned source. New registrations persist the new keys.
History for a newly keyed configuration starts from its normal estimate.
Python retains imported modules across session closes. A worker that already
loaded a different lmms source pin records `worker_restart`, releases the claim
and exits. The next worker can load that pin in a fresh interpreter. This
handoff consumes no retry budget. A failure while closing the previous session
also releases the new claim and retires the worker without charging the next
job's setup budget. On first load, importing lmms from outside
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

lmms control operations, including aggregation, run in a fresh subprocess even
when the pool uses the scheduler's Python executable. Each operation therefore
imports its own pinned source. The source pin also selects this isolation for
backends configured through `module:Class`.

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

Claim publication, `session_error` publication and stop/resume share
`setup.lock`. Operations needing both locks take `lease.lock` first, then
`setup.lock`; expiry only needs `lease.lock`. A job cannot become blocked
between a claim's final setup check and its lease publication.

`shardq resume --job JOB` starts a fresh setup epoch and clears a manual STOP.
Each claim captures its epoch so failures from earlier claims stay in their
original budget. Missing epochs denote the initial cycle, including earlier
`session_error` records. The CLI and scheduler expose `setup_status`,
`session_errors` and `session_retry_after_unix`. Restarting the scheduler
reconstructs all three from the ledger. A batch scheduler waits through
temporary backoff and may exit with paused jobs once workers and leases drain.
Setup recovery warnings only name jobs with unfinished partitions; historical
failures in jobs already merged or partial require no resume action.

## Worker submission recovery

The root scheduler lock gives one writer authority over
`workers/<worker_id>.json`. Each parallel submit thread owns one such row.
The scheduler writes **all** intents in a pool's admitted batch before starting
its threads. Each thread publishes `submitting` before entering the launcher,
then publishes its own outcome as soon as the call returns. These are atomic
same-directory file replacements. A replacement that exposes `submitting`
is the point after which a restart must assume the platform could have created
a task, including when the process died immediately before the external call.
The worker rows rebuild reservations even if `scheduler/state.json` predates
the batch. This protocol covers process exits under the filesystem contract above.

| On-disk submission state | Evidence and permitted next action |
| --- | --- |
| `intent` | Capacity reserved, external call not entered. With matching configuration and current demand, dispatch the same ID. Withdraw to `terminal` if the demand or worker limit has fallen. |
| `submitting` | External call may be in flight. A live caller can confirm a handle, record a definite creation rejection, or report uncertainty. After restart, reconcile before any new admission. |
| `rejected` | Explicit pre-creation rejection. Releases capacity. An eligible tick can reserve the same ID again for the same fingerprint. Changed requests get a new ID. |
| `uncertain` | Outcome unknown. Persist the error and `reconcile_after_unix`. After 300 seconds query again. Failure, a partial listing, unsupported lookup and zero matches all retain capacity. |
| `confirmed` | A creation receipt, complete unique lookup, or verified operator decision supplied a nonempty handle. Poll that handle in the original pool. Query errors preserve the last known status and the reservation. |
| `conflict` | Multiple matching handles, or a change in request identity/configuration. Hold the reservation for offline resolution. |
| `terminal` | The known task is terminal, all possible tasks were verified terminal, or an uncalled intent was withdrawn. Releases capacity. Remaining runnable work can now require a new worker. |

New requests use `shardq-<uuid>` for both `worker_id` and `request_id`,
including the worker environment and cloud task name. A SHA-256 fingerprint
covers the worker arguments, explicit environment, execution settings and
provider configuration. Admission limits can change independently. Volc also
hashes the actual template, AIHC hashes an explicitly configured CLI config
file, and local includes host, Python executable and working directory.
Keep the CLI account, region, PID namespace, deployed entry scripts, images
and external configuration stable across submit and recovery. The digest
identifies the configured request. It does not pin remote image contents or
discover changes to a CLI's implicit credentials.

**Capacity.** An intent or unresolved call holds a worker slot and a potential
queued position when `max_queued` applies. The row records its requested GPUs.
After a restart, Volc audit pools with uncalled intents get the first turns,
in pool-name order. Each may dispatch only its existing intents on that turn,
subject to current demand, worker and queue limits, and a fresh GPU audit.
Uncalled intents in these pools can wait for a later turn. A reduced shared
quota therefore lets one eligible request take the available GPUs. Each pool
finishes its batch before the next pool audits capacity. A pending intent in
a probe pool still counts against a matching audit budget because that pool
can dispatch without a GPU audit.

A multiple-match conflict holds one reservation per known distinct match. An
unresolved request stays charged to the original Volc queue/creator during
GPU admission, even across ShardQ pools or a change of configuration. An audit
may already include an unbound task. The extra deduction can temporarily
underuse the queue. Once confirmed, the same worker row is polled and the
provider audit accounts for its resources. A named pool retains outstanding
reservations when its configured launcher changes. A usable queue audit
still requires a complete, current provider view of other consumers.

Each tick reconciles existing submissions before evaluating job demand. A
job with `claims_blocked` contributes no new demand. Setup backoff, paused
epochs, active lease drain and merge drain use the Stage A ledger. An
unresolved row prevents idle exit even after STOP. A failed admission query
backs off its pool. Pools sharing a Volc GPU budget still respect each other's
reservations.

**Worker startup.** With no explicit `--worker-id`, the worker CLI uses
`SHARDQ_REQUEST_ID` when present. A managed worker checks its environment request ID
against the scheduler's row, then atomically publishes its own startup receipt
in `workers/<id>.starts/`. It may claim immediately during `submitting`,
`uncertain` or `confirmed`. It rechecks the state before seeking more work.
In-flight ranges retain their ordinary completion and lease rules. A lease
can carry an empty handle from an early claim. Liveness follows its stable
worker ID within the pool, so a later handle applies to the existing lease
without rewriting its owner. A cloud startup receipt proves that a process
started and cannot by itself identify its remote handle.

| Launcher | Evidence available after a lost create acknowledgement |
| --- | --- |
| local | A Linux worker receipt identifies host, boot ID, PID and process start ticks. A restarted scheduler on the same host and PID namespace can recognize the original process or its termination, even after PID reuse. Pre-receipt exits and unreadable, incomplete or unavailable receipts can remain uncertain. Without procfs an acknowledged local launch can use the legacy PID handle, with the same PID-reuse limitations. |
| volc | A successful create returns a task ID, and get-by-ID can poll it. The capacity listing filters finished tasks. It provides insufficient evidence for complete historical request lookup. An ambiguous create requires operator evidence. |
| aihc | A successful create returns a job ID scoped by pool and queue. Status uses get-by-ID. The adapter has no verified complete lookup by submitted name. An ambiguous create requires operator evidence. |

`lookup_submission(request_id)` may return distinct exact handles only when
the search covers the original request scope and includes terminal tasks.
An incomplete search raises. Even a complete empty result may reflect
visibility delay. None of the built-ins assumes an idempotent create API.
Only explicit pre-creation rejections permit automatic create retries.

**Offline resolution.** Inspect `shardq status --json`, including
`match_handles`, `started_count`, errors and reconciliation time. Stop the
scheduler and verify the provider account, queue and full history, including
any call that could still complete. Run `resolve-submission` with the worker
ID and written evidence. The command takes the same root scheduler lock.

- `--handle H` binds the verified task. For a conflict, supply a
  `--terminal-match H` for each other candidate that the provider confirms
  has finished. A handle already bound or matched by another live request
  in the same pool and launcher cannot be reassigned this way.
- `--not-created` reopens safe creation retry only after verified noncreation.
  A recorded start, handle or match blocks this action.
- `--terminal` records that all possible tasks finished and releases capacity.

The row retains the decision, time, evidence, prior state and original
matches under `resolutions`. Restore the original execution configuration
before tracking a rebound handle. Restart the scheduler after resolution.

Old worker JSON has no `submission_state`, request fingerprint or receipts.
It continues to use its recorded handle and prior terminal status. Missing
receipt directories indicate zero recorded starts. A directly invoked worker
needs no submission record. Upgrade after stopping and draining old writers,
and account separately for earlier unrecorded cloud calls. Mixed-version
writers cannot enforce this protocol.

## Pool configuration

A pools file has a top-level `pools` object. Each pool contains:

- `backend`: built-in name or `module:Class`
- `entry`: absolute container entry script; ignored by the local launcher
- `launcher`: `local`, `volc`, `aihc` or an importable `module:Class`
- `launcher_args`: platform configuration. Legacy named blocks remain readable.
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
python -m shardq scheduler [--launcher NAME] [--exit-when-idle]
python -m shardq worker --pool POOL
python -m shardq status [--job JOB] [--json]
python -m shardq resolve-submission --worker-id ID (--handle H | --not-created | --terminal) --evidence TEXT
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
audited. Select the class with `launcher: "module:Class"` and configure `launcher_args`. Launchers
manage workers only; shard truth always remains in the shared ledger.
