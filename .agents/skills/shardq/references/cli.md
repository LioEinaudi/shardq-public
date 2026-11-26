# ShardQ CLI reference

Invoke as `shardq ...` (console script installed by `pip install -e .`) or
`python -m shardq ...`. Every subcommand accepts `--root PATH` and `--pools PATH`.
Root resolves as `--root`, then `$SHARDQ_ROOT`, else error. Pools resolve as `--pools`,
then `$SHARDQ_POOLS`, then `<root>/pools.json`, else error. Any `ValueError`,
`RuntimeError`, `KeyError`, or `OSError` prints `error: <message>` to stderr and exits 2.

## register SPEC.json [--pin-mode worktree|verify] [--sizes NAME=N ...] [--dry-run]

- Loads and validates the job spec (`references/specs.md`), checks that `pool` exists in
  the pools file and that the job `backend` equals the pool backend, and refuses when
  `<root>/jobs/<job_id>` already exists.
- Runs the backend `describe` operation to enumerate partitions and sizes. It executes
  through `pool.local_python` when that is set, so a control host without the backend's
  dependencies still works. `--sizes p0=40 p1=24` skips describe and uses those sizes.
- Pins recorded in `job.json`: `image` and `flavor` from `pool.volc`, `control_root` and
  `control_commit` (git HEAD of the current directory, if any), `shardq_version`, plus
  backend pins. The lmms backend records source root, resolved root, branch, upstream,
  remote, commit, dirty flag, pin mode, and an import proof. `--pin-mode worktree`
  (default) creates a detached worktree under `<root>/src`; `verify` uses the tree in place.
- `shard.limit_units` caps every partition at that many units (smoke runs).
- Writes `jobs/<job>/job.json` and `jobs/<job>/partitions/<p>/partition.json` with
  `status: cutting`, `next_cut: 0`, and `unit_seconds_est` from `history.json` or the
  backend hint. `--dry-run` prints the payload and writes nothing.
- Prints the job payload as JSON on success.

## scheduler [--launcher volc|local] [--poll-seconds 60] [--once] [--exit-when-idle] [--max-submits-per-tick 4] [--max-cuts-per-tick 64] [--require-complete]

- Holds a non-blocking `flock` on `<root>/scheduler/lock`; exits 3 if another scheduler
  holds it.
- Handles only jobs whose pool uses the selected launcher. Default is `volc`.
- One tick, in order: refresh worker statuses through the launcher (workers already in a
  `terminal:*` state are not polled again; live ones are polled with 8 threads); expire leases;
  bisect or quarantine shards over `max_errors` or `max_preemptions`; update the runtime
  EMA in `history.json`; cut shards until pending plus backoff reaches `shard.lookahead`
  (at most `--max-cuts-per-tick` per partition per tick); start a background merge
  (2 threads) for partitions whose shards
  are all done, superseded, or quarantined, and collect finished ones; submit workers.
  A merge never blocks the tick; a failed merge is retried after 600 s and listed in
  `warnings`. `state.json` lists `merges_in_flight`.
- Submission count per pool is `min(demand - live, pool.max_workers - live,
  --max-submits-per-tick)`, where demand = pending + backoff + leased shards of
  jobs eligible for claims, and live
  counts queued plus running workers. Uncut units are never counted. Submissions run
  concurrently with up to 4 threads, with successful workers recorded in submission order.
  Volc `capacity_mode: probe` additionally caps queued workers at `volc.max_queued`;
  `audit` divides `capacity_slots()` by `volc.gpu_slots`.
- A failed submit puts the pool on a 300 s backoff, persisted in `state.json`; other
  successful submissions in the same batch are still recorded.
  Volc flow-control rejections are retried up to 5 times inside the launcher with a
  1 to 3 s jittered wait per attempt; if they still fail they are logged as
  `submit throttled` and do not trigger the backoff. There are 5 attempts total,
  and the jittered wait is multiplied by the attempt number.
- If a tick raises an exception, the scheduler writes
  `ERROR tick failed; retrying in <poll>s` and the full traceback to
  `<root>/scheduler/log/scheduler.log`, sleeps for one poll interval, and continues
  with the next tick without exiting. Only `--once` propagates the tick exception
  to the CLI, which prints `error: ...` and exits 2 under the error rules above.
  Logs go to both stderr and `scheduler.log`, but fatal errors outside this handler
  (including startup failures and `KeyboardInterrupt`) appear only on stderr, so
  preserve stderr when starting the scheduler.
- `--once` runs one tick and exits 0. `--exit-when-idle` exits 0 once every partition of
  jobs outside manual STOP or setup pause is merged or partial, no non-terminal
  worker or physical lease remains, and background merges have finished. Temporary
  setup backoff keeps the scheduler waiting. Paused jobs require a
  scheduler restart when resumed after exit. Without this flag the scheduler keeps polling.
  Under tmux, persist stdout/stderr to files and use `remain-on-exit off` so
  exits close the window; `remain-on-exit on` retains even successfully completed panes.
- `--require-complete` refuses to merge a partition with quarantined units and logs a
  warning instead of writing a partial marker.
- Writes `scheduler/state.json`, `scheduler/status.md`, and
  `scheduler/log/scheduler.log`. Tick log fields are `actions`, `warnings`,
  `workers_polled`, `cut`, `submitted`, `merges`, and `seconds`; `state.json` also
  records this tick's `shards_cut` and `workers_submitted` counts.

## worker --pool POOL [--worker-id ID] [--max-wall-seconds N] [--idle-exit-seconds N] [--job JOB ...]

- Normally started by the scheduler: the local launcher spawns
  `python -m shardq worker ...` on the control host; the Volc launcher submits a task
  whose entrypoint runs `pool.entry` with `SHARDQ_ROOT`, `SHARDQ_POOL`, `SHARDQ_POOLS`,
  `SHARDQ_WORKER_ID`, `SHARDQ_MAX_WALL`, `SHARDQ_IDLE_EXIT`, `SHARDQ_PKG`, and
  `pool.env` exported. `examples/lmms-worker.sh` shows the expected entry script.
- Pool configuration comes from the pools file, or from the job's `pool_snapshot` when
  no pools file is reachable.
- After claim, heartbeats cover setup and execution every 60 seconds or half the
  job's TTL, whichever is shorter. The worker checks backend identity and the
  runtime session key before reuse, runs the range, validates exact coverage of
  `[start, start + count)` and writes `complete.json`.
- Exits when the remaining wall clock is under 1.5 times the next shard estimate, or
  after `idle-exit-seconds` without a claim.
- `SIGTERM` or `SIGINT` writes a `preempted` failure for the active attempt and exits 143.
- `--job` (repeatable) restricts claims to the listed job ids.

## status [--job JOB] [--json]

- Text: one table per job with columns partition, size, done_units, done, leased,
  pending, backoff, quarantined, est_s/unit, status; then a workers table with
  worker_id, pool, launcher, handle, last_status.
- `--json`: full detail including each shard's state, attempts, errors, preemptions,
  lease owner, and heartbeat.

## merge --job JOB [--partition P] [--allow-partial]

- Manual merge of terminal partitions. Without `--allow-partial`, a partition with
  missing units raises an error.
- Writes `jobs/<job>/merged/<p>/{results.json,records.jsonl,merge.json}` and
  `jobs/<job>/markers/<column>/<p>.complete.json` or `.partial.json`, then prints the
  merge reports (`effective`, `missing_units`, `shards_used`, `attempts_total`,
  `preemptions_total`, `selfcheck`).

## split --job JOB --shard SHARD

- Bisects a shard into two children and writes `superseded.json` on the parent. Fails on
  a one-unit shard. This is the operation the scheduler applies to over-limit shards.

## stop --job JOB and resume --job JOB

- `stop` writes `jobs/<job>/STOP`. `resume` starts a fresh setup budget and clears
  STOP. It also releases setup backoff or pause. Existing failures stay available
  for audit and range/preemption budgets retain their counts. Running attempts
  finish normally and workers with nothing to claim idle out.

## publish --job JOB --out-dir DIR

- Requires every partition to be merged or partial; DIR must not exist yet.
- Writes `run.json`, `artifacts.json` (paths and sha256 of results and records),
  `acceptance.json` (`accepted`, or `data_gap` with `missing_units` per partition),
  `summary.md`, and copies the markers to `state/full/<column>/`.

## selftest-fs --root PATH [--procs 16] [--rounds 200]

- Races `procs` processes on `os.mkdir` per round. Expect
  `single_winner_rounds == rounds` and `conflicts == 0`. This covers processes
  on one host. Verify cross-host lock exclusion and coherent symlink and rename
  behavior on the deployment mount separately. Leaves a `run-*` directory under PATH.

## lmms-merge-check --samples S.jsonl --results R.json (--lmms-root ROOT --local-python PY | --pool POOL)

- Re-aggregates an lmms-eval samples file through ShardQ's merge path and compares the
  public scalar metrics with `results.json`. Exit 0 on match, 1 with a `diffs` object
  otherwise. `--pool` takes `lmms_root` and `local_python` from the pools file.
