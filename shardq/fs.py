"""Publication and locking primitives for a cooperating POSIX ledger."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator
from uuid import uuid4


@contextmanager
def file_lock(path: Path, *, blocking: bool = True) -> Iterator[bool]:
    """Yield acquisition status; contenders must share a permanent lock inode."""
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            if blocking:
                raise
            yield False
            return
        yield True


@contextmanager
def atomic_output(path: Path) -> Iterator[Path]:
    """Publish one finished file with rename. A compound update needs its own lock."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid4().hex}")
    # Use normal file permissions subject to umask, including on group-shared ledgers.
    temporary.touch(mode=0o666, exist_ok=False)
    try:
        yield temporary
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_text(path: Path, content: str) -> None:
    with atomic_output(path) as temporary:
        temporary.write_text(content, encoding="utf-8")


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

def write_records(path: Path, rows: Iterable[dict]) -> str:
    """Publish UTF-8 JSONL and hash exactly the bytes written."""
    digest = hashlib.sha256()
    with atomic_output(path) as temporary, temporary.open("wb") as output:
        for row in rows:
            line = (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
            output.write(line)
            digest.update(line)
    return digest.hexdigest()
