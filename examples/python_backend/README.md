# Route support tickets with a reusable Session

The Backend describes an indexed set of tickets and compiles the ordered
routing rules in `open_session()`. One local worker uses the compiled rules
for both two-ticket ranges. A match selects a tag, and unmatched tickets
receive `other`. Merging counts the selected tags. Add or change rules in the
job file. Their patterns and order are part of the session fingerprint.

From the repository root, run the following with ShardQ installed. For a
wheel install, copy this example directory anywhere and set `EXAMPLE` to
its absolute path.

```sh
EXAMPLE="$PWD/examples/python_backend"
export PYTHONPATH="$EXAMPLE${PYTHONPATH:+:$PYTHONPATH}"
export SHARDQ_ROOT="$(mktemp -d)"
export SHARDQ_POOLS="$EXAMPLE/pools.json"
python3 -m shardq register "$EXAMPLE/job.json"
python3 -m shardq scheduler --poll-seconds 1 --exit-when-idle
python3 -m shardq status --job ticket-tags
cat "$SHARDQ_ROOT/jobs/ticket-tags/merged/tickets/results.json"
```

The output contains two billing tickets, one access ticket and one other.
Each successful attempt records its ordinal within the Session as
`backend_extra.session_range` in `complete.json`.
