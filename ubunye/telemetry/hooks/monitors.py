"""Bridge hook for user-defined monitors declared under ``CONFIG.monitors``.

Keeps backwards compatibility with the existing ``Monitor`` protocol
(``task_start``/``task_end`` with keyword args). Reads outputs from the
shared ``state`` dict on task exit.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator

from ubunye.core.hooks import Hook
from ubunye.telemetry.monitors import load_monitors, safe_call


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
