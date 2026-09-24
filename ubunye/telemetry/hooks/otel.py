"""OpenTelemetry hook: a span per task and step, and the run's metrics.

Enabled with the other telemetry hooks (``UBUNYE_TELEMETRY=1``) or passed to the
engine directly (``Engine(extra_hooks=[OTelHook()])``). See
:mod:`ubunye.telemetry.otel` for configuration and the metrics it records.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from ubunye.core.hooks import Hook
from ubunye.telemetry import otel

TRUE = ("1", "true", "yes", "on")


def _cheap_to_count(backend: Optional[str]) -> bool:
    """Rows are counted where that is free: a single-machine frame is already in memory.

    On a distributed backend a count is a job over the data, so it is done only
    when ``UBUNYE_OTEL_COUNT_ROWS`` asks for it.
    """
    if os.environ.get("UBUNYE_OTEL_COUNT_ROWS", "").lower() in TRUE:
        return True
    if not backend:
        return False
    try:
        from ubunye.core import backends

        return not backends.load_class(backend).CAPABILITIES.distributed
    except Exception:
        return False


def _rows(frame: Any) -> Optional[int]:
    try:
        return int(frame.count())
    except Exception:
        return None


class OTelHook(Hook):
    """Wrap each task and step in a span, and record the run's metrics."""

    def __init__(self, service_name: str = "ubunye") -> None:
        self._on = otel.setup(service_name)

    @contextmanager
    def task(self, ctx, cfg: Dict[str, Any], state: Dict[str, Any]) -> Iterator[None]:
        task = ctx.task_name or "unknown"
        attrs = {
            "ubunye.task": task,
            "ubunye.run_id": ctx.run_id,
            "ubunye.profile": ctx.profile or "default",
            "ubunye.backend": ctx.backend,
            "ubunye.config_hash": getattr(ctx, "config_hash", None),
        }
        metric_attrs = {"task": task, "backend": ctx.backend or ""}
        t0 = time.perf_counter()
        status = "error"
        try:
            with otel.span(f"ubunye.task {task}", attrs):
                yield
            status = "success"
        finally:
            seconds = time.perf_counter() - t0
            otel.record("runs", 1, {**metric_attrs, "status": status})
            otel.record("task_s", seconds, {**metric_attrs, "status": status})
            if status == "success" and _cheap_to_count(ctx.backend):
                for key, instrument in (("inputs", "read"), ("outputs", "written")):
                    for name, frame in (state.get(key) or {}).items():
                        rows = _rows(frame)
                        if rows is not None:
                            otel.record(instrument, rows, {"task": task, "dataset": name})
            otel.flush()

    @contextmanager
    def step(self, ctx, name: str, meta: Optional[Dict[str, Any]] = None) -> Iterator[None]:
        task = ctx.task_name or "unknown"
        attrs: Dict[str, Any] = {"ubunye.task": task, "ubunye.step": name}
        for key, value in (meta or {}).items():
            attrs[f"ubunye.{key}"] = value
        t0 = time.perf_counter()
        status = "error"
        try:
            with otel.span(name, attrs):
                yield
            status = "success"
        finally:
            otel.record(
                "step_s",
                time.perf_counter() - t0,
                {"task": task, "step": name, "status": status},
            )
