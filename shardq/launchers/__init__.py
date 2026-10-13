from __future__ import annotations

from typing import Type

from .base import Launcher


def get_launcher(name: str) -> Type[Launcher]:
    if name == "local":
        from .local import LocalLauncher

        return LocalLauncher
    if name == "volc":
        from .volc import VolcLauncher

        return VolcLauncher
    if name == "aihc":
        from .aihc import AihcLauncher

        return AihcLauncher
    raise ValueError(f"unknown shardq launcher: {name}")
