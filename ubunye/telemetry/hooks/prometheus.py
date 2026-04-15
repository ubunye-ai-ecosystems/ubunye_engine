"""Hook wrapping Prometheus counters and histograms."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from ubunye.core.hooks import Hook
from ubunye.telemetry.prometheus import (
    observe_step,
    observe_task,
    start_prometheus_http_server,
)


class PrometheusHook(Hook):
    """Record task and step counts/durations to Prometheus.

    If ``port`` (or ``UBUNYE_PROM_PORT``) is set, also starts the HTTP
    metrics endpoint once per process.
    """

    def __init__(self, port: Optional[int] = None) -> None:
        resolved = port if port is not None else os.getenv("UBUNYE_PROM_PORT")
        if resolved:
            try:
                start_prometheus_http_server(int(resolved))
            except Exception:
                pass

    @contextmanager
    def task(self, ctx, cfg: Dict[str, Any], state: Dict[str, Any]) -> Iterator[None]:
        task = ctx.task_name or "unknown"
        profile = ctx.profile or "default"
        try:
            yield
        except Exception:
            try:
                observe_task(task=task, profile=profile, status="error")
            except Exception:
                pass
            raise
        else:
            try:
                observe_task(task=task, profile=profile, status="success")
            except Exception:
                pass

    @contextmanager
    def step(self, ctx, name: str, meta: Optional[Dict[str, Any]] = None) -> Iterator[None]:
        task = ctx.task_name or "unknown"
        profile = ctx.profile or "default"
        t0 = time.perf_counter()
        try:
            yield
        except Exception:
            try:
                observe_step(
                    task=task,
                    profile=profile,
                    step=name,
                    status="error",
                    duration_sec=time.perf_counter() - t0,
                )
            except Exception:
                pass
            raise
        else:
            try:
                observe_step(
                    task=task,
                    profile=profile,
                    step=name,
                    status="success",
                    duration_sec=time.perf_counter() - t0,
                )
            except Exception:
                pass
