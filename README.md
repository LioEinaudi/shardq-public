# ShardQ

ShardQ is a filesystem-backed scheduler for cutting benchmark partitions into
short, retryable ranges. Its core has no third-party runtime dependencies.
`lmms-eval` is an optional backend loaded only when that backend runs.

## Install

```bash
git clone https://github.com/LioEinaudi/shardq-public.git
cd shardq-public
python -m pip install -e . --no-deps
shardq --help
```

Python 3.9 or newer is required.

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

## Volc

The package includes its Volc CLI client and a packaged YAML template. A Volc
pool supplies `queue`, `flavor`, `image`, `vepfs_id`, preemption and
priority settings under `volc`. In `probe` mode, `max_queued` limits queued
workers. In `audit` mode, also set `creator` and `gpu_cap`; ShardQ sums
non-terminal requested GPU slots before submitting.

The [Volc deployment template](examples/deployments/volc-lmms/) includes
a configurable pool, six example job specs and a batch scheduler script.

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

A backend subclasses `shardq.backends.base.Backend`, implements the existing
describe/session/validate/merge contract, and may implement the class-level
`pins` hook. Select it with `module:Class` in a pool. The built-in `script`
backend is convenient for command-line adapters.

A cloud launcher subclasses `shardq.launchers.base.Launcher` and implements
`submit`, `status`, and `kill`; implement `capacity_slots` when the cloud
has auditable capacity. Add its name to `shardq.launchers.get_launcher`.

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
