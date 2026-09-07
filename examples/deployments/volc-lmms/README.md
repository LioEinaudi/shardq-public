# Volc and lmms-eval example

This template shows how to run image and video benchmarks on preemptible workers.
Start with a copy of this directory on a shared filesystem visible at the same
path on the control host and the workers. Run the commands below from that copy.

## Configure the pool

Edit `pools.json` with your queue, image, GPU flavor and vePFS ID. Point `entry`
at the shared copy of ShardQ's `examples/lmms-worker.sh`. Set `local_python`
to the control host interpreter and `env.SHARDQ_PYTHON` to the worker interpreter.
Set `extra.lmms_root` to a Git checkout of lmms-eval compatible with the ShardQ
range backend. Both hosts need access to that checkout. For additional worker
setup, set `env.SHARDQ_PRE_HOOK` to a shared shell script.

The six job specs illustrate fixed-size shards. DynaMath runs four items per
shard. Video-MME, MLVU and LVBench run one item per shard. Replace the model
path and check the model adapter and task names against your lmms-eval checkout.
When changing a task name, also update its key in `traits`. `protocols.json`
documents the example settings. Choose a new protocol ID when the evaluation
settings change.

## Run a smoke job

```bash
export SHARDQ_ROOT=/absolute/path/to/shared/ledger
export SHARDQ_POOLS="$PWD/pools.json"
shardq register jobs/videomme-smoke.json --pin-mode verify --dry-run
shardq register jobs/videomme-smoke.json --pin-mode verify
./start-scheduler.sh &
shardq status --job example-videomme-smoke --json
```

The smoke spec limits coverage to two videos. To run the full partition,
register `jobs/videomme-full.json` while the scheduler is running. The example
pool allows up to two live workers. Scheduler logs are under
`$SHARDQ_ROOT/scheduler/log/`.
