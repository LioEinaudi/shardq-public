# Compute a checksum for every file in a manifest

Each line of `manifest.txt` names a file relative to the manifest. Its
zero-based line number is the stable integer `unit_id`. The script reads
the assigned range and writes one record per file with its path, byte count
and SHA-256. The example hashes three files included in this directory.

Run on a Linux host with Python 3 and ShardQ. From the repository root,
use the commands below. With a wheel installation, copy this example folder
to the control host and set `FILE_TASK_DIR` to its absolute path.

```sh
export FILE_TASK_DIR="$PWD/examples/manifest_script"
export SHARDQ_ROOT="$(mktemp -d)"
export SHARDQ_POOLS="$FILE_TASK_DIR/pools.json"
python3 -m shardq register "$FILE_TASK_DIR/job.json"
python3 -m shardq scheduler --poll-seconds 1 --exit-when-idle
python3 -m shardq status --job file-hashes
cat "$SHARDQ_ROOT/jobs/file-hashes/merged/files/records.jsonl"
cat "$SHARDQ_ROOT/jobs/file-hashes/merged/files/results.json"
python3 -m shardq publish --job file-hashes --out-dir "$SHARDQ_ROOT/export"
cat "$SHARDQ_ROOT/export/artifacts.json"
cp "$SHARDQ_ROOT/jobs/file-hashes/merged/files/records.jsonl" "$SHARDQ_ROOT/export/"
cp "$SHARDQ_ROOT/jobs/file-hashes/merged/files/results.json" "$SHARDQ_ROOT/export/"
```

The local launcher runs the worker on this host. `FILE_TASK_DIR` is inherited
by the worker, so the script can be invoked from any working directory.
`records.jsonl` contains the merged records. `results.json` reports coverage,
missing unit IDs and the records checksum. `publish` exports an evidence
bundle. Its `artifacts.json` records the original ledger paths and SHA-256
digests for both files. The two `cp` commands put the data alongside the
evidence so you can take the export directory to another machine. There,
read the local copies and compare their SHA-256 against the manifest.

To process another list, update the manifest and its partition size in
`job.json`, and choose a fresh job ID. Keep input files and manifest order
stable throughout execution and recovery. ShardQ can replay a range, and
each attempt writes in its own `SHARDQ_OUT_DIR`.

Script starts one process per assigned range. Use a Python Backend with
`open_session()` when expensive state such as a loaded model needs to persist
between ranges.
