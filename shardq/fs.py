"""Publication primitives for a cooperating POSIX ledger."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4


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
