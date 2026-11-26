from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

from ..spec import PoolSpec


class TransientSubmitError(RuntimeError):
    """Submission was rejected before any task was created; safe to retry."""


class CommandError(RuntimeError):
    """Keep the provider response separate from command paths and arguments."""

    def __init__(self, command: str, code: int, output: str) -> None:
        super().__init__(f"{command} failed ({code}): {output[-2000:]}")
        self.output = output


class Launcher(ABC):
    name = ""

    def __init__(self, root: Path, pool: PoolSpec, repo_root: Path) -> None:
        self.root = root
        self.pool = pool
        self.repo_root = repo_root

    @abstractmethod
    def submit(self, worker_id: str, worker_argv: List[str], env: Dict[str, str]) -> str:
        raise NotImplementedError

    @abstractmethod
    def status(self, handle: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def kill(self, handle: str) -> None:
        raise NotImplementedError

    def capacity_slots(self) -> Optional[int]:
        return None

    def submission_inputs(self) -> dict:
        """Inputs outside PoolSpec that change the submitted request."""
        return {}

    def lookup_submission(self, worker_id: str) -> List[str]:
        """Return exact matches, including finished tasks, from a complete lookup.

        Zero observed matches cannot establish that creation did not occur.
        Incomplete, unavailable or unsupported lookups must raise.
        """
        raise NotImplementedError(f"{self.name} cannot recover a handle by request identity")
