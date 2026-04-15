"""Hook wrapping the JSON event logger."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from ubunye.core.hooks import Hook
from ubunye.telemetry.events import EventLogger


class EventLoggerHook(Hook):
    """Emit structured JSON events for task and step lifecycle."""

    def __init__(self, logger: Optional[EventLogger] = None) -> None:
        self._logger = logger

    def _resolve(self, ctx) -> EventLogger:
        if self._logger is None:
            self._logger = EventLogger(
                task=ctx.task_name or "unknown_task",
                profile=ctx.profile or "default",
                run_id=ctx.run_id,
            )
        return self._logger

    @contextmanager
    def task(self, ctx, cfg: Dict[str, Any], state: Dict[str, Any]) -> Iterator[None]:
        logger = self._resolve(ctx)
        t0 = time.perf_counter()
        try:
            logger.task_start()
        except Exception:
            pass
        try:
            yield
        except Exception:
            try:
                logger.task_end(status="error", duration_sec=time.perf_counter() - t0)
            except Exception:
                pass
            raise
        else:
            try:
                logger.task_end(status="success", duration_sec=time.perf_counter() - t0)
            except Exception:
                pass

    @contextmanager
    def step(self, ctx, name: str, meta: Optional[Dict[str, Any]] = None) -> Iterator[None]:
        logger = self._resolve(ctx)
        t0 = time.perf_counter()
        try:
            logger.step_start(name, extra=dict(meta) if meta else None)
        except Exception:
            pass
        try:
            yield
        except Exception as e:
            try:
                logger.step_end(
                    name,
                    status="error",
                    duration_sec=time.perf_counter() - t0,
                    extra={"error": repr(e)},
                )
            except Exception:
                pass
            raise
        else:
            try:
                logger.step_end(name, status="success", duration_sec=time.perf_counter() - t0)
            except Exception:
                pass
