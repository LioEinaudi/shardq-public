from __future__ import annotations

from typing import Optional


DEFAULT_UNIT_SECONDS = 20.0


def estimate_unit_seconds(history: dict, key: str, hint: Optional[float]) -> float:
    row = history.get(key)
    if isinstance(row, dict):
        value = row.get("unit_seconds")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
            return float(value)
    if isinstance(hint, (int, float)) and not isinstance(hint, bool) and hint > 0:
        return float(hint)
    return DEFAULT_UNIT_SECONDS


def update_history(
    history: dict,
    key: str,
    elapsed_seconds: float,
    count: int,
    now: float,
    alpha: float = 0.5,
) -> None:
    if count <= 0 or elapsed_seconds < 0:
        raise ValueError("history samples require count > 0 and elapsed_seconds >= 0")
    observed = float(elapsed_seconds) / count
    previous = history.get(key, {})
    old = previous.get("unit_seconds") if isinstance(previous, dict) else None
    value = observed if not isinstance(old, (int, float)) else alpha * observed + (1.0 - alpha) * float(old)
    history[key] = {
        "unit_seconds": value,
        "samples": int(previous.get("samples", 0)) + count if isinstance(previous, dict) else count,
        "updated_unix": now,
    }


def plan_cut(size: int, next_cut: int, unit_seconds: float, shard_cfg: dict) -> int:
    if size < 0 or next_cut < 0 or next_cut > size:
        raise ValueError("invalid partition size/next_cut")
    if unit_seconds <= 0:
        unit_seconds = DEFAULT_UNIT_SECONDS
    remaining = size - next_cut
    limit = shard_cfg.get("limit_units")
    if limit is not None:
        remaining = min(remaining, max(0, int(limit) - next_cut))
    if remaining <= 0:
        return 0
    target = max(1, int(float(shard_cfg["target_seconds"]) / unit_seconds))
    target = max(int(shard_cfg["min_units"]), target)
    target = min(int(shard_cfg["max_units"]), target)
    return min(remaining, target)
