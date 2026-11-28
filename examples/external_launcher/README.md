# Run a launcher from a separate module

`threaded_local.py` starts Linux processes through the built-in LocalLauncher
and sets `OMP_NUM_THREADS` for each worker. The pool selects the class by
its Python import path and supplies `threads_per_worker` in `launcher_args`.
Local process handles, status, termination and startup receipt lookup come
from LocalLauncher.

Run from the repository root with ShardQ installed. Make the module importable
in both the scheduler and worker environments.

```sh
export PYTHONPATH="$PWD/examples/external_launcher${PYTHONPATH:+:$PYTHONPATH}"
export SHARDQ_ROOT="$(mktemp -d)"
export SHARDQ_POOLS="$PWD/examples/external_launcher/pools.json"
python3 -m shardq register examples/external_launcher/job.json
python3 -m shardq scheduler --poll-seconds 1 --exit-when-idle
python3 -m shardq status --job local-threads-demo
cat "$SHARDQ_ROOT/jobs/local-threads-demo/merged/sample/records.jsonl"
```

The scheduler uses the launcher selected by each pool. Pass
`--launcher threaded_local:ThreadedLocal` to operate only on pools
configured with this launcher identity.
