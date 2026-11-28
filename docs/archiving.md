# Retain a finished ledger and start the next batch

For batches with a clean end, keep the entire finished ledger at its original
absolute path and register the next batch under a new `SHARDQ_ROOT`. This
keeps all jobs, attempts, startup receipts and submission decisions available
for audit. Existing marker and publication paths continue to identify the
same files. The new scheduler scans only the new ledger.

## Close the old batch

1. Finish registration for the old root. Let its existing scheduler finish
   background merges and drain workers. A scheduler run with
   `--exit-when-idle` can exit with stopped or setup-paused jobs, and a
   `--launcher` filter checks only selected pools. Verify **every job and
   every pool** independently. Each partition must be `merged` or `partial`,
   fully cut (`next_cut == size`), with zero pending, backoff and leased
   shards. A manual partial merge can be published before work drains.
   Keep an unfinished job on this ledger and wait to close the batch.
2. Stop the old scheduler and scheduled restarts. Inspect the ledger and
   platform in the original account, queue and host/PID namespace. Confirm
   that queued and running workers are terminal, all leases have drained,
   and every submission has a verified outcome. For an unresolved request,
   use its identity, matches and startup receipts to check the platform's
   full history. With the scheduler stopped, `resolve-submission` records
   an operator decision and its evidence. A timeout or an empty lookup leaves
   creation uncertain. [Submission recovery procedure](design.md)
3. Account for workers launched directly, old remote entry scripts and
   processes whose leases expired during suspension. A direct worker can
   lack a submission row. A late process can still publish its own attempt.
   Confirm these processes have exited and disable any producer that could
   start another writer against the old root. Wait for active manual merges
   and registration processes to finish as well.

Use these checks against the old root. The `find` command also lists dangling
lease links. A drained ledger produces no lease paths.

```sh
OLD_ROOT=/shared/shardq/batch-001
python3 -m shardq status --root "$OLD_ROOT" --json
find "$OLD_ROOT/jobs" -path '*/shards/*/lease' -print
```

## Start the next ledger

Leave the old directory at the same mount path on all hosts. Give the next
batch a fresh root and point to the intended pools configuration. Start its
single scheduler after registering the new jobs.

```sh
export SHARDQ_ROOT=/shared/shardq/batch-002
export SHARDQ_POOLS=/shared/shardq/config/pools.json
python3 -m shardq register /path/to/new-job.json
python3 -m shardq scheduler --poll-seconds 5 --exit-when-idle
```

Scheduling history belongs to each root. The new ledger starts with the
registered Backend's unit-time hints or defaults and learns its own estimates.
Its history EMA excludes attempts from the retained batch. Keep the old
`history.json` together with its source attempts for auditing.

Inspect or export the old job explicitly. Choose a fresh evidence directory
for each publication.

```sh
python3 -m shardq status --root "$OLD_ROOT" --job finished-job
python3 -m shardq publish --root "$OLD_ROOT" --job finished-job --out-dir /shared/exports/finished-job
```

`publish` records artifact paths and SHA-256 values for the retained files.
This procedure operates at batch boundaries. Jobs that share a live ledger
keep their original directories and attempt histories.
