"""Hook wrapping OpenTelemetry spans."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from ubunye.core.hooks import Hook
from ubunye.telemetry.otel import init_tracer, span


class OTelHook(Hook):
    """Wrap each task and step in an OpenTelemetry span."""

    def __init__(self, service_name: str = "ubunye") -> None:
        try:
            init_tracer(service_name=service_name)
        except Exception:
            pass

    @contextmanager
    def task(self, ctx, cfg: Dict[str, Any], state: Dict[str, Any]) -> Iterator[None]:
        attrs = {
            "task": ctx.task_name or "unknown",
            "profile": ctx.profile or "default",
            "run_id": ctx.run_id,
        }
        with span(f"Task:{ctx.task_name or 'unknown'}", attrs):
            yield

    @contextmanager
    def step(self, ctx, name: str, meta: Optional[Dict[str, Any]] = None) -> Iterator[None]:
        attrs: Dict[str, Any] = {
            "task": ctx.task_name or "unknown",
            "profile": ctx.profile or "default",
        }
        if meta:
            attrs.update(meta)
        with span(name, attrs):
            yield
