#!/usr/bin/env bash
set -euo pipefail

: "${SHARDQ_ROOT:?set SHARDQ_ROOT to the shared ledger directory}"
: "${SHARDQ_POOLS:?set SHARDQ_POOLS to the configured pools.json}"

mkdir -p "$SHARDQ_ROOT/scheduler/log"
exec shardq scheduler --launcher volc --poll-seconds 15 \
  >>"$SHARDQ_ROOT/scheduler/log/stdout.log" 2>>"$SHARDQ_ROOT/scheduler/log/stderr.log"
