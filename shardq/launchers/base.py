from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

from ..spec import PoolSpec


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
