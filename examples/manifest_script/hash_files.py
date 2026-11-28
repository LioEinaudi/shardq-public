"""Hash the input files assigned to this range. Each manifest line is a unit."""

import hashlib
import json
import os
from itertools import islice
from pathlib import Path


def main():
    manifest = Path(__file__).with_name("manifest.txt")
    start = int(os.environ["SHARDQ_START"])
    count = int(os.environ["SHARDQ_COUNT"])
    records = Path(os.environ["SHARDQ_OUT_DIR"]) / "records.jsonl"
    with manifest.open(encoding="utf-8") as paths, records.open("w", encoding="utf-8") as out:
        for unit_id, line in enumerate(islice(paths, start, start + count), start):
            name = line.rstrip("\n")
            digest = hashlib.sha256()
            size = 0
            with (manifest.parent / name).open("rb") as source:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(block)
                    size += len(block)
            record = {"unit_id": unit_id, "file": name, "bytes": size, "sha256": digest.hexdigest()}
            out.write(json.dumps(record, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
