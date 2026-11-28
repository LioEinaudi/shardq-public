# ShardQ

ShardQ runs recoverable offline batches on a shared POSIX filesystem. It cuts
known-size partitions into retryable integer ranges, manages leases and checks
the merged output. A backend supplies the work and a launcher runs workers. Its core has no third-party runtime dependencies.
`lmms-eval` is an optional backend loaded only when that backend runs.

## Install

```bash
git clone https://github.com/LioEinaudi/shardq-public.git
cd shardq-public
python -m pip install -e . --no-deps
shardq --help
```

Python 3.9 or newer is required.

## Run a batch script

The [manifest example](examples/manifest_script/) computes file information for
each manifest entry with the Python standard library. A Script job supplies
partition sizes (or a discovery command) and a range command. Each invocation
writes one JSON object per assigned index to `$SHARDQ_OUT_DIR/records.jsonl`.
The default merge publishes the verified records and a `results.json` summary in
`jobs/<job>/merged/<partition>/`. It reports record count, completed unique
units, missing indices and the records SHA-256. A terminal partition with zero
valid records publishes empty JSONL and reports every index missing.

Set `merge_command` for domain-specific aggregation. For a model that should
stay loaded across ranges, use a Python Backend session. Script starts a new
subprocess for every range.

## Five-minute fake demo

Pick a fresh ledger directory, then run the three commands:

```bash
export SHARDQ_ROOT=/tmp/shardq-fake-demo
shardq register examples/fake-demo.json --pools examples/pools.example.json
shardq scheduler --pools examples/pools.example.json --launcher local --exit-when-idle --poll-seconds 2
shardq status --job fake-demo --json
```

The `p0` partition merges with mean `0.5`. Unit 7 in `p1` is deliberately
poisoned, bisected to a one-unit shard, quarantined, and reported as a partial
merge.

## Configuration

For a batch run under tmux, register the jobs first, set `SHARDQ_ROOT` and
`SHARDQ_POOLS`, then start the scheduler with automatic exit:

```bash
mkdir -p "$SHARDQ_ROOT/scheduler/log"
tmux new -d -s shardq 'exec shardq scheduler --launcher volc --exit-when-idle >>"$SHARDQ_ROOT/scheduler/log/stdout.log" 2>>"$SHARDQ_ROOT/scheduler/log/stderr.log"' \; set-option -t shardq:0 remain-on-exit off
```

Omit `--launcher` to serve all configured pools with one scheduler. An explicit
`--launcher NAME` limits scheduling to pools using that exact alias or
`module:Class`. Older releases defaulted to Volc. Add `--launcher volc` to
existing unattended commands that should serve only Volc pools. The scheduler exits
once runnable jobs are merged or partial, workers and leases have drained, and
background merges have finished. A temporary setup backoff keeps the scheduler
waiting. Stopped jobs and jobs paused after repeated setup failures can be
resumed later. Restart the scheduler if it has already exited. `remain-on-exit off`
closes exited panes; inspect the saved stderr log for failures. Setting it to
`on` retains every exited pane, including successful runs.

The ledger root is resolved in this order:

1. `--root`
2. `SHARDQ_ROOT`
3. an error explaining that one of them is required

The pools file is resolved in this order:

1. `--pools`
2. `SHARDQ_POOLS`
3. `<root>/pools.json`, when present
4. an error explaining the available choices

Every subcommand accepts `--root` and `--pools`; commands that do not read
pools still accept the option so wrapper scripts can use one uniform argument
set.

A pool chooses a backend and launcher. Backend-specific values belong in
`extra`; for example, the lmms backend reads `extra.lmms_root` and
`extra.column`. See [examples/pools.example.json](examples/pools.example.json)
for local and placeholder Volc pools.

## Recovery and upgrades

If model loading or another session setup step fails, `shardq status --job JOB`
shows the job's setup state, failure count and next retry time. ShardQ waits
`retry.backoff_seconds` (default 300) and pauses that job after
`retry.max_session_errors` failures (default 3). Other jobs in its pool can
continue. Fix the environment and run `shardq resume --job JOB` to start a new
setup budget with the original partition ranges and attempt history.

**Interrupted worker submissions.** Before contacting a launcher, the scheduler
records a worker ID and reserves capacity in `workers/<worker_id>.json`.
Restart the scheduler with the same ledger, pools file and provider account.
Keep the deployed package path, interpreter, entry scripts and worker environment
stable when recovering recorded requests. A changed execution fingerprint
requires inspection of the original request.
It resumes unused intents and reconciles calls that may have reached the
platform. An unresolved request keeps its pool slot and keeps
`--exit-when-idle` waiting. A worker may start and renew leases while its
submit acknowledgement is still in flight.

`shardq status --json` shows the request ID, submission state, observed
worker starts, errors, and the next reconciliation time. Linux local workers
can be identified from their startup receipts. Volc and AIHC currently need
operator evidence when the create acknowledgement is lost. Inspect the
original queue/account and task history, including finished tasks. Stop the
scheduler before recording a verified outcome, for example:

```bash
shardq resolve-submission --worker-id "$WORKER_ID" --handle "$HANDLE" --evidence "$PROVIDER_EVIDENCE"
```

Use `--not-created` only with positive evidence that creation was rejected
and no call can still create the task. Use `--terminal` when every possible
matching task has finished. For multiple matches, binding one requires
`--terminal-match HANDLE` for each other verified terminal task and supporting
evidence. Restart the scheduler after resolution. A temporarily empty listing
or missing worker heartbeat leaves the reservation in place.
[Submission states, resource accounting and operator recovery](docs/design.md#worker-submission-recovery)

For an existing ledger, stop the old scheduler and drain old workers, including
queued submissions, before starting the upgraded writers. Existing pools,
snapshots, jobs and attempts can be used in place. Workers recompute session
compatibility, including the actual Script `run_command`, on each claim.
Legacy worker rows with a handle continue to be polled. Before cutover,
reconcile any old cloud calls that may have created a task without recording
a handle. The new protocol starts with requests created by the upgraded scheduler.

Use a POSIX shared filesystem with coherent cross-host `flock`, symbolic links
and atomic same-directory file rename. A worker publishes a fully initialized
lease with one link creation. The scheduler can also recover incomplete leases
left by earlier versions after a two-TTL grace period. The deployment assumptions
and recovery rules are in [the design](docs/design.md#ledger-and-state-semantics).

Finished batches can keep their ledger at its original absolute path and use a
fresh root for the next batch. The retained ledger stays available for status
and publication. [Draining and retaining a batch](docs/archiving.md)

## Volc

The package includes its Volc CLI client and a packaged YAML template. A Volc
pool supplies `queue`, `flavor`, `image`, `vepfs_id`, preemption and
priority settings under `volc`. In `probe` mode, `max_queued` limits queued
workers. In `audit` mode, also set `creator` and `gpu_cap`; ShardQ sums
non-terminal requested GPU slots before submitting.

The [Volc deployment template](examples/deployments/volc-lmms/) includes
a configurable pool, six example job specs and a batch scheduler script.

For AIHC GPU workers, set `aihc.gpu_resource` to the resource name advertised
by your queue. CPU-only pools can set `aihc.gpu_slots` to zero.

## Optional lmms backend

Registration records the lmms Git revision and proves that `lmms_eval`
resolves from the configured tree. `--pin-mode verify` uses the tree in place;
`--pin-mode worktree` creates a detached tree under `<root>/src`.

The merge fixture can be checked without importing lmms in the control
interpreter:

```bash
shardq lmms-merge-check \
  --local-python /path/to/lmms-python \
  --lmms-root /path/to/lmms-eval \
  --samples /path/to/samples.jsonl \
  --results /path/to/results.json
```

## Extension points

[Adapter contract and compatibility](docs/extensions.md) ·
[Python Backend with a reusable session](examples/python_backend/) ·
[External Launcher](examples/external_launcher/)

New registrations freeze backend reporting metadata alongside source pins.
New jobs write version 2 markers and `run.json` with common fields for every
backend and optional backend reporting fields. lmms retains its original
top-level marker parameters, revision and import proof. Jobs registered before
this change still export version 1 markers and runs. The ledger, worker,
attempt and pool formats accept the previous files in place.

A backend subclasses `shardq.backends.base.Backend`, implements the existing
describe/session/validate/merge contract, and may implement the class-level
`pins` hook. Select it with `module:Class` in a pool. The built-in `script`
backend is convenient for command-line adapters.

A cloud launcher subclasses `shardq.launchers.base.Launcher` and implements
`submit`, `status`, and `kill`; implement `capacity_slots` when the cloud
has auditable capacity. A launcher with complete request history can implement
`lookup_submission` for automatic recovery. Set `launcher` to the importable `module:Class` and put its options in
`launcher_args`. Legacy `volc` and `aihc` blocks are also accepted. When both
forms specify options for the selected launcher they must agree.
[Runnable external launcher](examples/external_launcher/)

[Local performance measurements and reproduction](docs/performance.md)

## Agent skill

The repository ships an [Agent Skill](https://agentskills.io) at
`.agents/skills/shardq/` (`.claude/skills/shardq` is a symlink to it). Codex and
Claude Code discover it automatically when started inside this checkout, and it teaches
either agent how to register jobs, run the scheduler, read the ledger, triage failures,
and adapt a new benchmark. To use it from other directories, link it into the user-level
skill roots:

```bash
ln -sfn "$PWD/.agents/skills/shardq" ~/.codex/skills/shardq
ln -sfn "$PWD/.agents/skills/shardq" ~/.claude/skills/shardq
```

## License

Copyright 2026 LioEinaudi. Licensed under the [Apache License, Version 2.0](LICENSE).
