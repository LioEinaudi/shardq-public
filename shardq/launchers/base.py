from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..spec import PoolSpec


class TransientSubmitError(RuntimeError):
    """Submission was rejected before any task was created; safe to retry."""


class SubmissionChangedError(RuntimeError):
    """Local request material differs from the intent, before calling the provider."""


class CommandError(RuntimeError):
    """Keep the provider response separate from command paths and arguments."""

    def __init__(self, command: str, code: int, output: str) -> None:
        super().__init__(f"{command} failed ({code}): {output[-2000:]}")
        self.output = output


@dataclass(frozen=True)
class Admission:
    """Resource accounting for one worker. Each scope starts with a namespace.

    A shorter recorded scope covers its descendants, for example an unknown
    account within a queue. Empty scopes share capacity only within the pool.
    """

    units_per_worker: int = 1
    scope: Tuple[str, ...] = ()
    max_queued: Optional[int] = None
    recheck_shared_capacity: bool = False

    def __post_init__(self):
        if type(self.units_per_worker) is not int or self.units_per_worker < 0:
            raise ValueError("units_per_worker must be a nonnegative integer")
        if self.max_queued is not None and (type(self.max_queued) is not int or self.max_queued < 0):
            raise ValueError("max_queued must be a nonnegative integer or None")
        if not isinstance(self.scope, tuple) or any(not isinstance(key, str) for key in self.scope):
            raise ValueError("capacity scope must be a tuple of strings")


class Launcher(ABC):
    name = ""
    initial_status = "unknown"
    admission_only_args: Tuple[str, ...] = ()

    @classmethod
    def admission(cls, pool: PoolSpec, available_slots: Optional[int]) -> Admission:
        """Describe request cost, shared budget and queue limit. No external calls."""
        return Admission()

    @classmethod
    def execution_source(cls, pool: PoolSpec) -> dict:
        """Public execution provenance for registration. No resource creation."""
        return {}

    @classmethod
    def startup_receipt(cls) -> dict:
        """Optional process identity evidence collected on the worker host."""
        return {}

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
        """Free resource units, or None if unmetered. Raise on a failed quota query."""
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
