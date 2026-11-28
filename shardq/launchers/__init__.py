from __future__ import annotations

import importlib
import inspect
from typing import Type

from .base import Launcher


_ALIASES = {
    "local": "shardq.launchers.local:LocalLauncher",
    "volc": "shardq.launchers.volc:VolcLauncher",
    "aihc": "shardq.launchers.aihc:AihcLauncher",
}


def validate_launcher_name(name: str) -> None:
    module, _, cls = _ALIASES.get(name, name).partition(":")
    if not all(part.isidentifier() for part in module.split(".")) or not cls.isidentifier():
        raise ValueError(f"invalid launcher {name!r}: expected local, volc, aihc or module:Class")


def get_launcher(name: str) -> Type[Launcher]:
    validate_launcher_name(name)
    module, cls = _ALIASES.get(name, name).split(":")
    try:
        candidate = getattr(importlib.import_module(module), cls)
    except Exception as exc:
        raise ValueError(f"cannot load launcher {name!r}: {exc}") from exc
    if not isinstance(candidate, type) or not issubclass(candidate, Launcher):
        raise ValueError(f"launcher {name!r} must be a Launcher subclass")
    if inspect.isabstract(candidate):
        missing = ", ".join(sorted(candidate.__abstractmethods__))
        raise ValueError(f"launcher {name!r} must implement {missing}")
    return candidate
