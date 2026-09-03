from __future__ import annotations

import importlib
from typing import Type

from .base import Backend


def get_backend(name: str) -> Type[Backend]:
    if ":" in name:
        module_name, class_name = name.split(":", 1)
        candidate = getattr(importlib.import_module(module_name), class_name)
    elif name == "fake":
        from .fake import FakeBackend

        candidate = FakeBackend
    elif name == "script":
        from .script import ScriptBackend

        candidate = ScriptBackend
    elif name == "lmms_eval":
        from .lmms_eval import LmmsEvalBackend

        candidate = LmmsEvalBackend
    else:
        raise ValueError(f"unknown shardq backend: {name}")
    if not isinstance(candidate, type) or not issubclass(candidate, Backend):
        raise TypeError(f"backend {name!r} is not a Backend subclass")
    return candidate
