#!/usr/bin/env bash
set -euo pipefail

: "${SHARDQ_ROOT:?missing SHARDQ_ROOT}"
: "${SHARDQ_POOL:?missing SHARDQ_POOL}"
: "${SHARDQ_POOLS:?missing SHARDQ_POOLS}"
: "${SHARDQ_WORKER_ID:?missing SHARDQ_WORKER_ID}"
: "${SHARDQ_MAX_WALL:?missing SHARDQ_MAX_WALL}"
: "${SHARDQ_IDLE_EXIT:?missing SHARDQ_IDLE_EXIT}"
: "${SHARDQ_PKG:?missing SHARDQ_PKG}"

[[ -n "${SHARDQ_ENV_FILE:-}" ]] && source "$SHARDQ_ENV_FILE"
[[ -n "${SHARDQ_PRE_HOOK:-}" ]] && source "$SHARDQ_PRE_HOOK"

export PYTHONPATH="$SHARDQ_PKG${SHARDQ_EXTRA_PYTHONPATH:+:$SHARDQ_EXTRA_PYTHONPATH}${PYTHONPATH:+:$PYTHONPATH}"

exec "${SHARDQ_PYTHON:-python}" -m shardq worker \
  --root "$SHARDQ_ROOT" \
  --pools "$SHARDQ_POOLS" \
  --pool "$SHARDQ_POOL" \
  --worker-id "$SHARDQ_WORKER_ID" \
  --max-wall-seconds "$SHARDQ_MAX_WALL" \
  --idle-exit-seconds "$SHARDQ_IDLE_EXIT"
