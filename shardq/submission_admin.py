"""Operator decisions about unresolved submissions, under the scheduler lock."""

from __future__ import annotations

import fcntl
import time
from pathlib import Path
from typing import List, Optional

from .ledger import read_json
from .submission import UNRESOLVED, occupies, save, worker_records


def resolve_submission(
    root: Path, worker_id: str, action: str, evidence: str, handle: Optional[str] = None,
    terminal_matches: Optional[List[str]] = None,
) -> dict:
    if not worker_id or worker_id in {".", ".."} or Path(worker_id).name != worker_id:
        raise ValueError("worker-id must be a single filename component")
    if not evidence.strip():
        raise ValueError("record the provider evidence for this decision")
    if action not in {"bind", "not-created", "terminal"} or (action == "bind" and not (handle or "").strip()):
        raise ValueError("choose a nonempty handle or a terminal resolution")
    if terminal_matches and action != "bind":
        raise ValueError("--terminal-match can only accompany --handle")
    lock_path = root / "scheduler" / "lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("stop the scheduler before resolving a submission") from exc

        row = read_json(root / "workers" / (worker_id + ".json"), {})
        if (not isinstance(row, dict) or row.get("worker_id") != worker_id or row.get("request_id") != worker_id
                or row.get("submission_state") not in UNRESOLVED):
            raise ValueError("expected an unresolved submission with this worker and request ID")
        matches = row.get("match_handles", [])
        if action == "not-created" and (
            matches or row.get("handle") or any((root / "workers" / f"{worker_id}.starts").glob("*.json"))
        ):
            raise ValueError("matching tasks or worker starts are recorded; verify their terminal states")
        decision = {
            "resolved_unix": time.time(), "action": action, "evidence": evidence.strip(),
            "previous_state": row["submission_state"], "match_handles": matches,
            "previous_error": row.get("submission_error"),
        }
        if action == "bind":
            if matches and handle not in matches:
                raise ValueError("handle is outside the recorded conflict matches")
            certified = set(terminal_matches or [])
            if certified != set(matches) - {handle}:
                raise ValueError("supply --terminal-match for every other recorded match verified terminal")
            for _, other in worker_records(root):
                if (other["worker_id"] != worker_id and other.get("pool") == row["pool"]
                        and other.get("launcher") == row["launcher"] and occupies(other)
                        and (other.get("handle") == handle or handle in other.get("match_handles", []))):
                    raise ValueError("handle is already owned or matched by another live submission in this pool")
            decision["handle"] = handle
            decision["terminal_matches"] = sorted(certified)
            row.update(submission_state="confirmed", handle=handle, last_status="unknown", dead=False)
        else:
            rejected = action == "not-created"
            row.update(submission_state="rejected" if rejected else "terminal", dead=True,
                       last_status="terminal:rejected" if rejected else "terminal:verified")
        row.setdefault("resolutions", []).append(decision)
        for key in ("match_handles", "submission_error", "status_error", "reconcile_after_unix"):
            row.pop(key, None)
        save(root, row)
        return row
