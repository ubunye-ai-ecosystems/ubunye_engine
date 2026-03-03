"""Monitoring hooks for optional logging backends (e.g., MLflow)."""

from __future__ import annotations

import importlib.metadata as md
from typing import Any, Dict, List, Protocol


class Monitor(Protocol):
    """Runtime monitor hooks for logging and drift observability."""

    def task_start(self, *, context: Any, config: dict) -> None:
        """Called at task start."""

    def task_end(
        self,
        *,
        context: Any,
        config: dict,
        outputs: Dict[str, Any] | None,
        status: str,
        duration_sec: float,
    ) -> None:
        """Called at task end."""


def load_monitors(cfg: dict) -> List[Monitor]:
    monitors_cfg = cfg.get("CONFIG", {}).get("monitors", []) or []
    if isinstance(monitors_cfg, dict):
        monitors_cfg = [monitors_cfg]
    if not monitors_cfg:
        return []

    registry = {ep.name: ep.load() for ep in md.entry_points(group="ubunye.monitors")}
    instances: List[Monitor] = []
    for entry in monitors_cfg:
        mtype = entry.get("type")
        if not mtype:
            raise ValueError("Monitor config missing 'type'.")
        if mtype not in registry:
            if entry.get("optional"):
                continue
            raise KeyError(f"Monitor plugin '{mtype}' not found. Installed: {sorted(registry)}")
        params = entry.get("params", {}) or {}
        instances.append(registry[mtype](**params))
    return instances


def safe_call(monitor: Monitor, method: str, **kwargs: Any) -> None:
    try:
        getattr(monitor, method)(**kwargs)
    except Exception:
        # Avoid failing task runs due to monitor errors.
        return
