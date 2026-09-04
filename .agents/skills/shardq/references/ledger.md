# Ledger layout and triage

## Layout under the root

```text
history.json                                   runtime EMA per backend:session_key:partition
pools.json                                     optional default pools file
scheduler/lock                                 flock; one scheduler per root
scheduler/state.json                           last tick: jobs, workers, actions, warnings, pool_backoff
scheduler/status.md                            human view of state.json
scheduler/log/scheduler.log                    tick log with tracebacks
workers/<worker_id>.json                       handle, last_status, dead, submitted_unix
workers/<worker_id>.log                        local launcher stdout/stderr
workers/<worker_id>.yaml                       Volc task YAML as submitted
jobs/<job>/job.json                            spec + pins + pool_snapshot + session_key + column
jobs/<job>/STOP                                present while the job is stopped
jobs/<job>/partitions/<p>/partition.json       size, source_size, next_cut, unit_seconds_est, status
jobs/<job>/partitions/<p>/shards/<shard>/shard.json        start, count, depth, est_seconds
jobs/<job>/partitions/<p>/shards/<shard>/lease/owner.json  worker_id, handle, attempt
jobs/<job>/partitions/<p>/shards/<shard>/lease/heartbeat   unix seconds, rewritten every 60 s
jobs/<job>/partitions/<p>/shards/<shard>/attempts/<a>/records.jsonl
jobs/<job>/partitions/<p>/shards/<shard>/attempts/<a>/raw/            backend raw output
jobs/<job>/partitions/<p>/shards/<shard>/attempts/<a>/complete.json | failed.json
jobs/<job>/partitions/<p>/shards/<shard>/quarantine.json
jobs/<job>/partitions/<p>/shards/<shard>/superseded.json   children after a split
jobs/<job>/merged/<p>/{results.json,records.jsonl,merge.json}
jobs/<job>/markers/<column>/<p>.complete.json | <p>.partial.json
src/                                            detached source worktrees from --pin-mode worktree
```

Shard ids are `<partition>-<start:06d>-<end:06d>`, so the unit range is visible in the
directory name.

## State derivation (strict order)

1. `superseded.json` present: SUPERSEDED (children carry the work).
2. any `attempts/*/complete.json`: DONE.
3. `quarantine.json` present: QUARANTINED (terminal data gap).
4. `lease/` directory present: LEASED.
5. otherwise, the newest `failed.json` `retry_after_unix` decides PENDING_BACKOFF or PENDING.

A claim is `os.mkdir(lease)`; the loser gets `FileExistsError`. Heartbeat content is the
unix time, not the file mtime. Records in an attempt are only trusted after
`complete.json` exists; merge picks, per unit, the earliest completed attempt.

## Failure reasons

| reason | written by | counts toward |
|---|---|---|
| `preempted` | worker on SIGTERM/SIGINT | max_preemptions |
| `lease_expired` | scheduler when a lease outlives its TTL and the launcher reports the owner gone (or at twice the TTL) | max_preemptions |
| `launcher_terminal` | reserved in `FAILURE_REASONS`; no code path writes it yet | max_preemptions |
| `backend_error` | worker when `run_range` raises or the command fails | max_errors |
| `validation_error` | worker when records do not cover the range | max_errors |
| `session_error` | worker when `open_session` fails (model load, import) | max_errors |

`failed.json` holds `attempt_id`, `worker_id`, `handle`, `reason`, `retryable`, `error`
(last 4000 characters of the traceback or stderr), `failed_unix`, `retry_after_unix`.

## Triage recipes

Nothing is being submitted
: `state.json` `warnings` names the reason (submit failure, merge failure, unknown pool).
`pool_backoff` shows a pool parked for 300 s after a failed submit. Demand is zero when
every partition is cut and no shard is pending; check `partition.json.next_cut` against
`size` and the per-partition `pending` count in `shardq status`.

Workers sit in `queued`
: Volc idle queue is waiting for free GPUs. Probe mode stops submitting once
`max_queued` workers are queued. Waits from under a minute to about 40 minutes are
normal; do nothing.

A shard keeps failing
: `find <shard_dir>/attempts -name failed.json | xargs -I{} sh -c 'echo {}; python -c "import json,sys;d=json.load(open(sys.argv[1]));print(d[\"reason\"]);print(d[\"error\"][-1500:])" {}'`.
Preemption reasons are expected on idle queues and only bisect after `max_preemptions`.
A repeating `backend_error` or `validation_error` on the same units is a poison unit or
an adapter bug; check `attempts/<a>/raw/` and the worker log. A `session_error` on
every attempt means the environment inside the worker is broken (imports, weights,
GPU); fix the entry script or hook, the retry budget will otherwise quarantine everything.

Partition finished as `partial`
: `merged/<p>/merge.json` lists `missing_units` and `quarantined_shards`. Quarantine is
terminal; after fixing the cause, register a new job id (optionally with a spec limited
to the missing partition). Never present the partial metric as the headline number.

Lease looks stale
: leased shard, old heartbeat, worker `terminal:*` in the workers table. The next tick
writes `lease_expired` and releases it once the TTL has passed; at twice the TTL it is
released even if the launcher still claims the worker is alive. Do not `rm -r lease`.

Scheduler will not start (exit 3)
: `fuser <root>/scheduler/lock` or `tmux ls` to find the live scheduler. Kill the old
one first; never run two against one root.

Merge failed
: the scheduler retries on every tick and logs the traceback in `scheduler.log`. For the
lmms backend the usual cause is the merge interpreter (`pool.local_python`) missing the
canonical tree; for the script backend it is `merge_command` not producing
`results.json`.

Worker exits immediately on Volc
: read `<root>/workers/<worker_id>.yaml` for the exact entrypoint, then fetch the task log
by handle with the `volc` CLI. The entry script fails fast on missing `SHARDQ_*`
variables and on a pre-hook assertion (transformers version, GPU count, GPU name).

## Safe and unsafe manual actions

Safe: `shardq stop`/`resume`, `shardq split`, `shardq merge --allow-partial`, reading
any file, deleting `workers/*.log`, registering a new job, killing a worker process or
Volc task (it becomes a preemption).

Unsafe: removing or editing anything under `shards/`, touching `history.json` while a
scheduler runs, running two schedulers, editing `job.json` after registration (pins and
pool snapshot are provenance), reusing a job id after deleting its directory.
