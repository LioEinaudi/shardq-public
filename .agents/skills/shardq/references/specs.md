# Job spec and pool spec

Both files are JSON objects. Validation lives in `shardq/spec.py`; the examples under
`examples/` are known-good.

## Job spec (`shardq register SPEC.json`)

| field | type | notes |
|---|---|---|
| `job_id` | string | `[A-Za-z0-9._-]+`; permanent, must be new |
| `backend` | string | must equal the pool's backend |
| `pool` | string | key in the pools file |
| `protocol_id` | string | required, carried into markers and publish output |
| `priority` | int | higher claims first; default 0 |
| `backend_args` | object | backend specific, see below |
| `shard` | object | cutting parameters |
| `retry` | object | failure handling |
| `traits` | object | per-partition overrides, e.g. `{"p1": {"max_parallel": 2}}` |

`shard` defaults: `target_seconds` 1800, `min_units` 4, `max_units` 512,
`limit_units` null, `lookahead` 4. The cutter sizes each shard to about
`target_seconds / unit_seconds_est`, clamped to `[min_units, max_units]`.
`limit_units` truncates every partition (smoke runs). `lookahead` is how many pending
plus backoff shards may exist per partition before cutting pauses.

`retry` defaults: `max_errors` 3, `max_preemptions` 20, `backoff_seconds` 300,
`bisect` true, `lease_ttl_seconds` 900. Exceeding either limit bisects the shard while
`bisect` is true, it has more than one unit, and depth is below `log2(max_units)`;
otherwise it is quarantined. `backoff_seconds` delays re-claiming after a failure.
`lease_ttl_seconds` should exceed the longest expected single-range runtime plus queue
jitter; a lease older than the TTL is released once the launcher reports the worker gone,
and unconditionally at twice the TTL.

`traits.<partition>.max_parallel` caps concurrently leased shards of that partition.

### backend_args by backend

- `fake` (tests and demos): `partitions` `{name: size}`, `unit_seconds` (default 0.05),
  `poison_units` `{partition: [unit_id, ...]}`.
- `script` (any command line, see `script-backend.md`): `partitions` or
  `describe_command`, `run_command`, `merge_command`, optional `session_key`.
- `lmms_eval`: `tasks` (list; groups expand to leaf tasks, one partition per leaf),
  `model`, `model_args`, `gen_kwargs`, `batch_size` (default 1), `force_simple`
  (default false), `bootstrap_iters` (default 100000). Ranges run through
  `lmms_eval.simple_evaluate` with an absolute offset; merge re-aggregates records with
  the task's own aggregation, so scores match an unsharded run.

## Pools file (`--pools`, `$SHARDQ_POOLS`, or `<root>/pools.json`)

```json
{"schema_version": 1, "pools": {"<name>": { ...pool... }}}
```

| field | type | default | notes |
|---|---|---|---|
| `backend` | string | required | built-in name or `module:Class` |
| `entry` | string | "" | absolute path of the container entry script; Volc only |
| `launcher` | string | required | `local` or `volc` |
| `volc` | object | {} | see below |
| `env` | object | {} | exported into the worker environment |
| `local_python` | string | null | interpreter for describe and merge on the control host |
| `max_workers` | int | 1 | live workers per pool |
| `worker_max_wall_seconds` | int | 14400 | worker wall clock; Volc deadline is this plus 1800 |
| `worker_idle_exit_seconds` | int | 300 | exit after this long with nothing to claim |
| `extra` | object | {} | backend data; `column` (marker directory, defaults to pool name), lmms `lmms_root` |

`volc` keys: `queue`, `flavor` (`ml.pni3ln.4xlarge` is one GPU), `gpu_slots` (default 1),
`image`, `preemptible` (default true), `priority` (default 4), `vepfs_id`,
`capacity_mode` (`probe` default, or `audit`), `max_queued` (probe mode cap on queued
workers), `creator` and `gpu_cap` (audit mode), optional `template` path overriding the
packaged `shardq/templates/volc_worker.yaml.tmpl`. The template mounts the vePFS at
`/mlp_vepfs/share` and sets `RetryOptions.EnableRetry: false` so ShardQ, not Volc,
owns retries.

A Volc worker entry script must be reachable inside the container at the same absolute
path, export or source what the backend needs, put `$SHARDQ_PKG` on `PYTHONPATH`, and
`exec` `python -m shardq worker` with the `SHARDQ_*` values; copy
`examples/lmms-worker.sh` and add a `SHARDQ_PRE_HOOK` for project-specific setup rather
than editing the package.
