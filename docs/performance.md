# Local ledger measurements

Measured on Linux 7.0.0-44, x86-64 Intel Core i9-14900HX, CPython 3.12.3,
with fixtures in `/tmp` on ext4. These measurements cover the filesystem
ledger and local Script merge. A cloud queue, distributed mount, inference
runtime and network service were outside this run.

## Fixtures and method

Each fixture has one active job with pending shards plus completed historical
jobs, all in the same pool. The tick runs the real scheduler with worker
submissions disabled. The merge runs separately against actual attempt files,
complete markers and their SHA-256 digests. Successful replays have a later
completion timestamp. Each unit has filters in order b, a, b, and the last
record deliberately duplicates the first key with a different value. Input
units descend within each shard by default. The benchmark checks each output
row's unit, filter, source attempt and winner, and hashes the merged bytes.

| Fixture | Historical jobs | Shards per job | Successful attempts per historical/merge shard | Merge units per shard | Output rows | Output bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 3 | 3 | 1 | 128 | 768 | 144,164 |
| large | 12 | 8 | 2 | 2,048 | 32,768 | 18,786,612 |
| spill | 3 | 2 | 2 | 8,192 | 32,768 | 35,563,828 |

Times are medians of three runs after one warm-up. Each timed merge uses a
fresh interpreter. Peak RSS is the merge process's `ru_maxrss`, including
the interpreter and excluding child processes. Separate probes count CPython
audit events inside the fixture root for read-only open attempts and directory
enumerations. They exclude `stat` calls, sort files in the system temporary
directory and I/O performed by shell children. Filesystem caches are warm.

## Results

The comparison with the integrated baseline uses the same custom command
that writes a minimal `results.json` on both releases. Stage 3 already uses
the canonical records file for Script. It isolates the additional effect of
stage 4's scanning and bounded merge. Opens and enumerations are per operation.

| Fixture and code (custom command) | Tick ms | Tick opens / enumerations | Merge ms | Merge peak MiB | Merge opens / enumerations |
| --- | ---: | ---: | ---: | ---: | ---: |
| Small, integrated baseline `02c1326` | 10.54 | 270 / 515 | 7.87 | 22.50 | 12 / 9 |
| Small, stage 3 `6d55447` | 10.48 | 270 / 515 | 5.42 | 22.98 | 12 / 9 |
| Small, 0.2.0 | 3.13 | 70 / 66 | 5.48 | 21.19 | 9 / 9 |
| Large, integrated baseline | 88.01 | 2,078 / 6,641 | 331.21 | 159.08 | 35 / 32 |
| Large, stage 3 | 87.04 | 2,078 / 6,641 | 196.84 | 74.70 | 35 / 32 |
| Large, 0.2.0 | 18.12 | 324 / 311 | 184.30 | 27.49 | 27 / 32 |

| Default Script summary | Stage 3 tick / merge ms | 0.2.0 tick / merge ms | Peak RSS MiB, stage 3 → 0.2.0 |
| --- | ---: | ---: | ---: |
| Small | 10.41 / 4.26 | 3.19 / 4.45 | 21.78 → 21.18 |
| Large | 88.11 / 194.00 | 17.50 / 180.74 | 74.69 → 27.48 |
| Large with ascending input | 87.68 / 189.83 | 18.06 / 184.49 | 74.68 → 27.55 |
| Spill with descending input | 8.48 / 270.26 | 2.84 / 503.00 | 100.11 → 27.42 |

All compared releases produced identical records bytes within each fixture.
The small fixture shows no stage-4 merge time improvement. In the spill case,
external sorting roughly doubles merge time and cuts peak RSS by about 73%.
Temporary disk space and I/O grow with the unsorted input.

## Implementation and invalidation

Each tick scans all relevant jobs at the start and again before publishing
final state. Within a tick it reuses a view of a fully cut, published job
with no pending/backoff work or physical leases. Active jobs are refreshed
between mutation and admission. The scheduler uses completion membership
from the scan and keeps an in-memory cache of history samples by path, inode,
size and nanosecond mtime. A membership or file change triggers a deterministic
EMA rebuild for the affected history key. Restart discards the cache. Claim
and expiry still recheck the live owner, generation and state under their locks.

The merge hashes each selected input while reading it and publishes the output
with atomic rename after the ordering and coverage checks succeed. Small
attempts sort locally with a 4 MiB input budget. Large sorted input streams
through. Overlapping ranges or oversized unordered input use stable temporary
sort runs and at most 16 input runs per merge pass. A single record, distinct
filters for the current unit, selected attempt metadata and the reported
missing IDs still need memory. Backends using the original list method also
materialize the canonical records in the aggregation process. Script uses the
file directly. The export step hashes each artifact and compares any checksum
recorded at merge time.

In the measured mixed ledger the scan reductions keep existing histories
online. For a deployment that closes whole batches, the next ledger can start
in a fresh root after all writers drain. [Batch retention](archiving.md)

## Reproduce

Run from a checkout containing this benchmark. Python and the standard
library are sufficient. Each invocation creates and cleans up its own ledger.

```sh
PYTHONPATH="$PWD" python3 benchmarks/ledger_bench.py --scale small --mode custom
PYTHONPATH="$PWD" python3 benchmarks/ledger_bench.py --scale large --mode custom
PYTHONPATH="$PWD" python3 benchmarks/ledger_bench.py --scale large --mode default
PYTHONPATH="$PWD" python3 benchmarks/ledger_bench.py --scale large --mode default --ordered
PYTHONPATH="$PWD" python3 benchmarks/ledger_bench.py --scale spill --mode default

baseline=$(mktemp -d)
git archive 02c1326 | tar -x -C "$baseline"
PYTHONPATH="$baseline" python3 benchmarks/ledger_bench.py --scale large --mode custom
```

Use `6d55447` in the archive command for the stage-3 comparison. Each run
prints JSON with environment, operation counts, timings, peak memory and the
output digest. The integrated baseline supports the custom command only.

## Distribution

Built offline with pip 26.1.1 and setuptools 81.0.0. Sizes refer to the actual
pure-Python wheels. Physical lines count all `shardq/**/*.py`, including blank
lines and comments. Examples, tests and benchmarks live outside the package.

| Tree | Required runtime distributions | Wheel bytes | Package Python lines |
| --- | ---: | ---: | ---: |
| main `27fa914` | 0 | 60,268 | 4,535 |
| Stage B `dc78bba` | 0 | 63,722 | 4,943 |
| Integrated `02c1326` | 0 | 68,720 | 4,978 |
| 0.2.0 | 0 | 76,293 | 5,437 |

## Validation

PR #1 had merged Stage A into main. Stage B had no merged PR. The integration
commit has main and Stage B as parents and retains both test suites. The full
runs on CPython 3.12.3 were:

| Tree | Tests run | Passed | Skipped |
| --- | ---: | ---: | ---: |
| main | 94 | 93 | 1 |
| Stage B | 144 | 143 | 1 |
| Integrated | 152 | 151 | 1 |
| 0.2.0 | 172 | 171 | 1 |

The shared skip requires the real MMStar reference samples, results, source
tree and pool interpreter. Synthetic lmms source trees exercised subprocess
isolation and aggregation. Provider tests exercised simulated Volc/AIHC
responses and interrupted requests. Real cloud submissions, distributed
mount behavior and GPU/model evaluation were outside this verification.

An offline installation of the 0.2.0 wheel into an empty virtual environment
contained one installed distribution. The console entry point, packaged Volc
template, LICENSE and NOTICE were checked. The manifest, external Launcher
and Python Backend examples each ran from a copied example directory through
preview, registration, scheduling, status and publication. Checks included
the two exported artifact digests for each job. Run the full regression suite
with `python3 -m unittest discover -s tests -v`.
