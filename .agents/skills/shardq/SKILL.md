---
name: shardq
description: Use when asked to run, monitor, debug, or extend ShardQ, the filesystem-ledger scheduler that cuts a long benchmark into short preemptible ranges for idle-queue GPUs (Volcengine idle queue or local processes). Covers registering job specs, running the scheduler in tmux, reading status and ledger files, handling preemptions, poison units and partial merges, publishing results, adapting any benchmark through the script backend, and adding a backend or launcher. Also triggers on 分片调度, 闲时资源跑 benchmark, or ledger paths under jobs/JOB/partitions.
---

# ShardQ

## What it is

- Three processes: `shardq register` turns a job spec into partitions; `shardq scheduler`
  ticks every N seconds (cut, expire leases, bisect, merge, submit workers); stateless
  `shardq worker`s claim contiguous unit ranges from a shared-filesystem ledger, run them
  through a backend, and write one `records.jsonl` per attempt.
- Truth is the ledger directory. Every tick re-derives state from files; nothing in memory
  is authoritative, so a dead scheduler or worker loses nothing. Restarting is always safe.
- Workers are expected to die (preemption, wall clock, idle exit). Failures with reason
  `preempted`, `lease_expired`, `launcher_terminal` count toward `retry.max_preemptions`;
  `backend_error` and `validation_error` count toward `retry.max_errors`.
  `session_error` backs off the job and pauses it after `retry.max_session_errors`
  (default 3). Fix setup and run `shardq resume --job J` for a new setup budget.
  An over-limit shard is bisected until it holds one unit, then quarantined. A partition
  with quarantined units merges as `partial`, never as complete.
- Every job carries a `protocol_id`. ShardQ never changes protocol identity; it only
  splits execution and re-aggregates records.

## How concurrency is capped

- Caps are per pool and shared by every job registered on that pool. Nothing limits how
  many jobs may be registered at once.
- Each tick submits `min(demand - live, pool.max_workers - live, --max-submits-per-tick)`
  workers, where live counts queued plus running workers and demand is pending,
  backoff and leased shards of jobs eligible for claims; uncut units are never counted, so a worker is only
  submitted when a shard already exists for it. Volc probe mode additionally keeps at most
  `volc.max_queued` workers waiting in the queue; running workers no longer count. So
  `max_workers` is the GPU ceiling and `max_queued` only bounds unstarted tasks.
- Each tick cuts at most `min(lookahead - pending - backoff, --max-cuts-per-tick)`
  shards per partition while below lookahead, subject to remaining units and
  `shard.limit_units`. `--max-cuts-per-tick` defaults to 64, so `lookahead` also
  determines how fast the pool can scale up. Jobs with one-unit shards
  (`min_units = max_units = 1`) should set `lookahead` at least as high as the desired
  worker count.
- Which job a worker serves is decided at claim time, in this order: same partition as
  its last shard, same registered `session_key`, higher job `priority`, fewer
  remaining shards, shards without prior failures, lower start unit. A worker with a
  loaded model keeps draining that model's jobs even when a higher-priority job with a
  different `session_key` appears; only a fresh worker follows priority first. Equal
  priorities interleave. To serialize benchmarks, `shardq stop` the ones that must wait
  or give them distinct priorities.
- At execution time the worker verifies backend identity and the effective session
  settings again. A legacy saved key can affect ordering. Script jobs with different
  commands open different sessions even if their explicit labels match.

## Preflight, every session

1. The checkout is the directory containing this skill (`shardq/`, `examples/`,
   `docs/design.md`, `tests/`). Read `docs/design.md` for architecture questions.
2. Resolve config before any command: root is `--root`, else `$SHARDQ_ROOT`; pools is
   `--pools`, else `$SHARDQ_POOLS`, else `<root>/pools.json`. There is no repo default.
   If neither is known, look for a running scheduler (`tmux ls`, `ps -ef | grep shardq`)
   or ask. Do not invent a root.
3. `shardq --help` must work. If not: `python -m pip install -e . --no-deps` in the
   checkout (Python >= 3.9, no third-party dependencies), or use `python -m shardq`.
4. Inspect before mutating: `shardq status`, `cat <root>/scheduler/status.md`,
   `tail -50 <root>/scheduler/log/scheduler.log`.

## Workflows

### A. Run a job end to end

```bash
export SHARDQ_ROOT=/path/to/ledger            # shared filesystem visible to all workers
export SHARDQ_POOLS=/path/to/pools.json       # see references/specs.md
shardq selftest-fs --root "$SHARDQ_ROOT/_selftest"   # one-host mkdir probe; expect conflicts 0
shardq register job.json --dry-run            # validates spec, prints pins, writes nothing
shardq register job.json                      # lmms backend: add --pin-mode verify to use the tree in place
mkdir -p "$SHARDQ_ROOT/scheduler/log"
tmux new -d -s shardq 'exec shardq scheduler --launcher volc --exit-when-idle >>"$SHARDQ_ROOT/scheduler/log/stdout.log" 2>>"$SHARDQ_ROOT/scheduler/log/stderr.log"' \; set-option -t shardq:0 remain-on-exit off
# One scheduler per root; local: --launcher local. Exits close the window; inspect
# stdout.log and stderr.log for failures. Do not use remain-on-exit on for batch runs.
shardq status                                 # repeat; partitions end as merged or partial
shardq publish --job <job_id> --out-dir /path/to/new/dir  # optional evidence bundle
```

- Smoke first: set `shard.limit_units` (for example 64) in a copy of the spec with a new
  `job_id`; the same spec with the limit removed is the full run.
- Merged outputs: `<root>/jobs/<job>/merged/<partition>/{results.json,records.jsonl,merge.json}`
  and markers under `<root>/jobs/<job>/markers/<column>/`.
- `job_id` is permanent. `register` refuses an existing id; to rerun, pick a new id.
- `--exit-when-idle` waits through temporary setup backoff. It exits after all jobs
  outside manual STOP or setup pause merge (complete or partial),
  workers and leases drain, and background merges finish. Stopped jobs remain stopped
  and unfinished; resuming or registering more work after exit requires restarting
  the scheduler. Omit this flag only for an intentionally persistent scheduler.

### B. Monitor and triage

Order of checks when something looks stuck:

1. Scheduler alive? `tmux ls`; `updated_unix` in `<root>/scheduler/state.json` must move
   every poll. Exit code 3 means another scheduler holds `scheduler/lock`.
   If `updated_unix` stops moving but `scheduler.log` keeps updating, run
   `grep "tick failed" <root>/scheduler/log/scheduler.log`; tick exceptions are
   logged there with tracebacks and the loop keeps retrying.
   A visible tmux session may contain only a dead pane: check `tmux list-panes -a -F
   '#{session_name} #{pane_id} #{pane_dead} #{pane_dead_status} #{remain-on-exit}'`.
   Before removing a dead pane, capture its output and verify it is still dead;
   use its pane ID to avoid closing other windows. Check custom `tmux -S` sockets too.
2. `warnings` and `pool_backoff` in `state.json`: a failed submit backs a pool off 300 s.
3. Workers table in `shardq status`: `queued` on Volc means waiting for idle GPUs. `terminal:*` rows are normal
   after preemption or wall clock; the scheduler resubmits while demand exists.
4. Per-shard detail: `shardq status --job J --json`, then read
   `attempts/<attempt>/failed.json` (`reason`, `error`) for the shard. Recipes in
   `references/ledger.md`.
5. Worker logs: local launcher writes `<root>/workers/<worker_id>.log`; Volc writes the
   submitted YAML to `<root>/workers/<worker_id>.yaml` and the handle `t-...` is in
   `workers/<worker_id>.json`; fetch container logs with the `volc` CLI (volc skill).

### C. Adapt a new benchmark

Any benchmark that can run a contiguous index range from a command line works through the
built-in `script` backend without writing Python: `run_command` gets
`SHARDQ_PARTITION`, `SHARDQ_START`, `SHARDQ_COUNT`, `SHARDQ_OUT_DIR` and must write
`records.jsonl` with integer `unit_id`s covering exactly that range; `merge_command`
turns the deduplicated records into `results.json`. Full contract and a worked example:
`references/script-backend.md`. When model loading dominates, write a Python `Backend`
whose `open_session` loads once per worker (`docs/design.md`, "Adding a backend").

### D. Operate on a live job

- Pause claims and cutting: `shardq stop --job J`; undo with `shardq resume --job J`.
  Running attempts finish; idle workers exit on their own.
- Force a bisection: `shardq split --job J --shard S` (fails on one-unit shards).
- Manual merge: `shardq merge --job J [--partition P] --allow-partial`. The scheduler
  merges automatically when every shard of a partition is done, superseded, or quarantined.
- Refuse partial results for a headline run: start the scheduler with `--require-complete`.
- Retry quarantined units: not possible in place. Fix the cause, register a new job id.

### E. Extend

New backend: subclass `shardq.backends.base.Backend` (`describe`, `session_key`,
`open_session`, `validate_range`, `merge`, optional class-level `pins`), reference it as
`module:Class` in the pool. New cloud: subclass `shardq.launchers.base.Launcher`
(`submit`, `status`, `kill`, optional `capacity_slots`) and add it to
`shardq.launchers.get_launcher`. Keep imports lazy so `import shardq` stays dependency-free.
Run `python -m unittest discover -s tests -v` afterwards.

## Hard rules

- One scheduler per root. Run it under tmux; if it fails, inspect and restart it.
  Normal `--exit-when-idle` completion does not need a restart until more work arrives.
  Preserve stdout/stderr to files and use `remain-on-exit off` for batch runs.
  Never run two.
- Upgrade the scheduler and workers together after draining old writers, including
  queued workers. Keep existing job, pool snapshot and attempt files. Leases now
  require cross-host `flock` and coherent atomic symlink creation, unlink and rename.
  Verify these on the actual mount across participating hosts. Keep lock files in place.
- Never hand-edit or delete `lease/`, `attempts/`, `complete.json`, `failed.json`,
  `quarantine.json`, or `superseded.json`. Use `stop`, `split`, `merge`, or wait for lease
  expiry (`retry.lease_ttl_seconds`; twice that forces expiry regardless of the launcher).
- Never delete a job directory to retry it, and never reuse a `job_id`.
- Never report a `partial` partition as complete. `publish` records it as `data_gap`
  with the missing unit ids; carry that forward.
- The Volc launcher shells out to the already configured `volc` CLI. Do not print, copy,
  or commit credentials, and do not put them in pools files.
- `--launcher local` runs workers as processes on the control host. Only use it where the
  backend can actually run there (the fake and script backends, or a GPU host).

## Deployment examples

- Run the local demo with `examples/fake-demo.json` and `examples/pools.example.json`.
- For Volc pools, adapt `examples/deployments/volc-lmms/` to the shared filesystem,
  runtime and resource IDs available to the deployment.
- Run the tests with `python -m unittest discover -s tests -v`. The optional MMStar
  merge test reads `SHARDQ_TEST_LMMS_PYTHON`, `SHARDQ_TEST_LMMS_ROOT` and
  `SHARDQ_TEST_LMMS_FIXTURE_DIR`. Supply a fixture directory containing one
  `<run>_samples_mmstar.jsonl` and its matching `<run>_results.json`.

## References

- `references/cli.md`: every subcommand, flag, output, exit code.
- `references/specs.md`: job spec and pool spec fields, defaults, validation, backend args.
- `references/ledger.md`: directory layout, state derivation, failure reasons, triage recipes.
- `references/script-backend.md`: adapting any benchmark, env contract, file formats, example.
- `docs/design.md` in the checkout: architecture and extension points.
