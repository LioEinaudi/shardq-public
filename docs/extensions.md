# Extending ShardQ

Choose a Backend for the **work** and a Launcher for the **execution platform**.
A pool names each by its built-in alias or Python `module:Class`. The job
names the same Backend as its pool. Install the plugin on the control host,
in the configured `local_python` environment and on workers that use it.
Keep module import lightweight and defer optional framework imports to the
operation that needs them. An absolute `PYTHONPATH` can expose a standalone
module. [Backend example](../examples/python_backend/) · [Launcher example](../examples/external_launcher/)

## Backend and Session

Subclass `shardq.backends.base.Backend`. The constructor receives
`backend_args`, the pool settings and registered `pins`.

| Operation | Contract |
| --- | --- |
| `describe()` | Return positive-sized `Partition` objects with unique filesystem-safe names. A partition's integers from zero through `size - 1` must keep the same meaning across retries. A fixed manifest can map them to filenames or business IDs. |
| `session_key()` | Fingerprint every effective setting captured by the Session, including input identity, arguments and relevant source revisions. `session_fingerprint(*settings)` hashes JSON-compatible values. |
| `open_session()` | Return an object with `session_key`, `run_range(partition, start, count, out_dir)` and `close()`. Load reusable models, indexes or tables here. |
| `validate_range(..., result)` | Check the attempt's domain constraints and unit coverage. `validate_unit_ids(result.records_file, start, count)` handles one record per unit. Raise `RangeValidationError` for invalid output. |
| `merge(partition, records, out_dir, meta)` | Aggregate the ordered list of validated records and publish `out_dir/results.json`. Return a dict, which may be empty. To read the canonical JSONL directly, also override `merge_file(partition, records_file, out_dir, meta)`. |

The worker compares the configured Backend identity and the current
`session_key()` before using a loaded Session. The saved key also helps
schedule compatible jobs. On a key change the worker closes the previous
Session and opens another. If earlier imports or native runtime state make a
process incompatible, raise `WorkerRestartRequired` during setup. A worker
with a previous Session releases the range for a fresh process. Failure to
open the initial Session consumes the separate setup failure budget.

Each range covers `[start, start + count)` and may run more than once. Write
`records.jsonl` to the supplied private attempt directory and return
`RangeResult(count, elapsed_seconds, records_file)`. Each JSON object has an
integer `unit_id`. Keep external writes idempotent, for example by keying an
upsert on job, partition and unit ID. ShardQ handles lease fencing, completion
and attempt selection. Use `RangeResult.extra` for JSON attempt diagnostics.

Merge verifies selected attempts and their checksums. For duplicate
`(unit_id, filter)` pairs it keeps the first selected record, then orders
records by unit ID, preserving encounter order among filters for a unit.
`meta` supplies `record_count`, `completed_units` (distinct units),
`expected_units`, `missing_units`, and the records artifact path and SHA-256.
Missing units produce a partial marker and a published `data_gap`. Domain
reports can aggregate the available records, including an empty input.

Optional registration hooks capture sources in `pins()`. Implement
`preview_pins()` and/or `preview_describe()` when real setup would create
resources. `register --dry-run` must be read-only. Return a tuple of import
roots from `control_pythonpath(pool, pins)` to request a fresh process for
control operations. An empty tuple requests isolation with ordinary imports.
The default allows in-process execution with the control interpreter.
`report_metadata(job_spec)` returns an optional dict of `marker`, `run` and
`source_revisions` fields. Registration freezes these values for offline
publication. Common identity, status and checksum fields take precedence.
New registrations emit schema 2 markers and `run.json`. The v1 export path
for earlier jobs preserves their original lmms fields.

## Launcher and recovery

Subclass `shardq.launchers.base.Launcher`. Implement
`submit(worker_id, worker_argv, env)`, `status(handle)` and `kill(handle)`.
Use the request identity and forward the supplied worker arguments and
environment, including `SHARDQ_REQUEST_ID`. Return a nonempty platform
handle from an acknowledged create. Polling reports `running`, `queued`,
`terminal:<reason>` or `unknown`. Set `initial_status` if a successful
receipt establishes the initial state. Use `pool.launcher_args` for platform
configuration and `submission_inputs()` for identity-relevant external
configuration, for example a template digest. Persistable inputs and
execution provenance must exclude secrets.

ShardQ writes an intent before calling the platform. A timeout leaves
creation uncertain and holds its worker and applicable queue reservation.
`lookup_submission(request_id)` returns distinct exact matching handles from
a complete search, including terminal tasks. One match can confirm the
submission. Several matches create a conflict. Zero matches retain the
uncertain state and never authorize a create retry. Unsupported or incomplete
queries must raise. The base implementation reports unsupported lookup.
Raise `TransientSubmitError` only for a verified rejection before creation.
A worker's independent startup receipt lets it begin by request ID before
handle acknowledgement. [Recovery and operator resolution](design.md)

`capacity_slots()` returns free platform resource units. `None` means the
platform is unmetered by this check. Raise on a failed quota query.
`admission()` can specify units per worker in those same units, the shared
budget scope and a queue limit. Defaults use one unit per worker and
pool-local capacity. `pool.max_workers` is measured in workers. Unresolved
submissions continue to occupy capacity across scheduler restarts.

## Compatibility and deployment

The existing list-based Backend merge and the three required Launcher
operations remain supported. The new hooks have default implementations.
Old pool sections, schema-1 job snapshots, attempts and submission records
remain readable. Use `launcher_args` for new configurations. When a pool
supplies both its nonempty legacy platform block and `launcher_args`, their
values must agree. An explicit `scheduler --launcher NAME` filters pools
by configured launcher identity. Omitting the filter manages all configured
pools under one scheduler lock.

All participants need the same shared POSIX ledger with coherent
cross-host locking, atomic rename and symlink operations. To upgrade, stop
the scheduler, drain queued and running workers and active leases, and
deploy the same writer version everywhere. Preserve the ledger, including
outstanding submission evidence. Use ShardQ commands to manage it.
