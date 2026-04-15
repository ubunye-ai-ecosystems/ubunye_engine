"""Bridge hooks for the legacy ``Monitor`` protocol.

``LegacyMonitorsHook`` loads monitors declared under ``CONFIG.monitors``.
``MonitorHook`` wraps a single already-constructed ``Monitor`` instance so
callers (the Python API, the CLI's lineage recorder) can adapt it to the
Hook protocol without going through config.

Both read outputs from the shared ``state`` dict on task exit.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator

from ubunye.core.hooks import Hook
from ubunye.telemetry.monitors import load_monitors, safe_call


def _wrap_monitor_task(monitor, ctx, cfg, state) -> Iterator[None]:
    """Shared task-lifecycle contextmanager body for a single Monitor."""
    t0 = time.perf_counter()
    safe_call(monitor, "task_start", context=ctx, config=cfg)
    try:
        yield
    except Exception:
        safe_call(
            monitor,
            "task_end",
            context=ctx,
            config=cfg,
            outputs=None,
            status="error",
            duration_sec=time.perf_counter() - t0,
        )
        raise
    else:
        safe_call(
            monitor,
            "task_end",
            context=ctx,
            config=cfg,
            outputs=state.get("outputs"),
            status="success",
            duration_sec=time.perf_counter() - t0,
        )


class MonitorHook(Hook):
    """Adapt a single ``Monitor`` instance to the Hook protocol.

    Useful when a caller already holds a Monitor (e.g. a LineageRecorder
    constructed from CLI flags) and wants to plug it into the engine's hook
    chain without going through ``CONFIG.monitors``.
    """

    def __init__(self, monitor: Any) -> None:
        self.monitor = monitor

    @contextmanager
    def task(self, ctx, cfg: Dict[str, Any], state: Dict[str, Any]) -> Iterator[None]:
        yield from _wrap_monitor_task(self.monitor, ctx, cfg, state)


class LegacyMonitorsHook(Hook):
    """Invoke legacy monitors (e.g. MLflow) declared in the task config."""

    def __init__(self, cfg: Dict[str, Any]) -> None:
        try:
            self.monitors = load_monitors(cfg)
        except Exception:
            self.monitors = []

    @contextmanager
    def task(self, ctx, cfg: Dict[str, Any], state: Dict[str, Any]) -> Iterator[None]:
        if not self.monitors:
            yield
            return

        t0 = time.perf_counter()
        for m in self.monitors:
            safe_call(m, "task_start", context=ctx, config=cfg)

        try:
            yield
        except Exception:
            dur = time.perf_counter() - t0
            for m in self.monitors:
                safe_call(
                    m,
                    "task_end",
                    context=ctx,
                    config=cfg,
                    outputs=None,
                    status="error",
                    duration_sec=dur,
                )
            raise
        else:
            dur = time.perf_counter() - t0
            outputs = state.get("outputs")
            for m in self.monitors:
                safe_call(
                    m,
                    "task_end",
                    context=ctx,
                    config=cfg,
                    outputs=outputs,
                    status="success",
                    duration_sec=dur,
                )
