"""Bridge hooks for the legacy ``Monitor`` protocol.

``LegacyMonitorsHook`` loads monitors declared under ``CONFIG.monitors``.
``MonitorHook`` wraps a single already-constructed ``Monitor`` instance so
callers (the Python API, the CLI's lineage recorder) can adapt it to the
Hook protocol without going through config.

Both read outputs from the shared ``state`` dict on task exit.
"""

from __future__ import annotations

import inspect
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator

from ubunye.core.hooks import Hook
from ubunye.telemetry.monitors import load_monitors, safe_call

#: What the engine knows beyond the outputs; given only to a monitor whose
#: ``task_end`` accepts it, so older monitors keep working unchanged.
EVIDENCE = (
    "inputs",
    "expectations",
    "timings",
    "llm_calls",
    "llm_budget",
    "hash_basis",
    "source_versions",
)


def _evidence(monitor: Any, state: Dict[str, Any], keys: tuple = EVIDENCE) -> Dict[str, Any]:
    method = getattr(monitor, "task_end", None)
    if method is None:
        return {}
    try:
        params = inspect.signature(method).parameters
    except (TypeError, ValueError):
        return {}
    takes_any = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
    return {k: state.get(k) for k in keys if takes_any or k in params}


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
            **_evidence(monitor, state),
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
            **_evidence(monitor, state),
        )


class MonitorHook(Hook):
    """Adapt a single ``Monitor`` instance to the Hook protocol.

    Useful when a caller already holds a Monitor (e.g. a LineageRecorder
    constructed from CLI flags) and wants to plug it into the engine's hook
    chain without going through ``CONFIG.monitors``.
    """

    def __init__(self, monitor: Any) -> None:
        self.monitor = monitor

    @property
    def reads_outputs(self) -> bool:  # type: ignore[override]
        """Whether the monitor says it acts on the output frames (the lineage recorder)."""
        return bool(getattr(self.monitor, "reads_outputs", False))

    @property
    def reads_inputs(self) -> bool:  # type: ignore[override]
        """Whether the monitor hashes the input frames (the lineage recorder, by default)."""
        return bool(getattr(self.monitor, "reads_inputs", False))

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

    @property
    def reads_outputs(self) -> bool:  # type: ignore[override]
        """Whether a monitor from ``CONFIG.monitors`` acts on the output frames."""
        return any(getattr(m, "reads_outputs", False) for m in self.monitors)

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
                    # Only what a record needs to be honest about its hashes.
                    **_evidence(m, state, ("hash_basis",)),
                )
