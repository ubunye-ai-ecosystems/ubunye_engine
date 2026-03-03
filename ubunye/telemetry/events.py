"""
Structured JSON event logging for Ubunye.

- Writes JSON Lines (one dict per line) to a file or stdout.
- Captures run/task/step metadata, timings, and optional counters.
- Safe to use in clusters; file writes are append-only.

Usage:
    from ubunye.telemetry.events import EventLogger

    logger = EventLogger(task="fraud/claims/claim_etl", profile="prod", run_id="uuid-1234")
    logger.task_start()
    logger.step_start("Reader:hive", extra={"input": "fraud_db.raw_claims"})
    ...
    logger.step_end("Reader:hive", status="success", rows=250_000, duration_sec=1.2)
    ...
    logger.task_end(status="success", duration_sec=45.7)
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TextIO


def _ts() -> str:
    """UTC ISO-8601 timestamp with milliseconds (e.g., '2025-09-10T12:34:56.789Z')."""
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class EventLogger:
    """
    Minimal JSON lines event logger for Ubunye task runs.

    Parameters
    ----------
    task : str
        Task coordinates (e.g., 'fraud_detection/claims/claim_etl').
    profile : str
        Active profile (e.g., 'dev', 'prod').
    run_id : str, optional
        Unique run identifier. If not provided, a uuid4 is generated.
    sink : TextIO, optional
        File-like object to write JSONL events to. Defaults to stdout.
    """

    def __init__(
        self,
        task: str,
        profile: str = "default",
        run_id: Optional[str] = None,
        sink: Optional[TextIO] = None,
    ) -> None:
        self.task = task
        self.profile = profile
        self.run_id = run_id or str(uuid.uuid4())
        self.sink: TextIO = sink or sys.stdout
        self._task_start_monotonic: Optional[float] = None
        self._step_monotonic: Dict[str, float] = {}

    # -------- generic emitter --------
    def emit(self, event: Dict[str, Any]) -> None:
        """Write a JSON event line; never raise in production paths."""
        base = {
            "ts": _ts(),
            "task": self.task,
            "profile": self.profile,
            "run_id": self.run_id,
        }
        try:
            line = json.dumps({**base, **event}, default=str)
            self.sink.write(line + os.linesep)
            self.sink.flush()
        except Exception:
            # Avoid crashing ETL for logging issues
            pass

    # -------- task-level --------
    def task_start(self, extra: Optional[Dict[str, Any]] = None) -> None:
        self._task_start_monotonic = time.monotonic()
        self.emit({"event": "task_start", **(extra or {})})

    def task_end(
        self,
        status: str = "success",
        duration_sec: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if duration_sec is None and self._task_start_monotonic is not None:
            duration_sec = max(0.0, time.monotonic() - self._task_start_monotonic)
        self.emit(
            {"event": "task_end", "status": status, "duration_sec": duration_sec, **(extra or {})}
        )

    # -------- step-level --------
    def step_start(self, step: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._step_monotonic[step] = time.monotonic()
        self.emit({"event": "step_start", "step": step, **(extra or {})})

    def step_end(
        self,
        step: str,
        status: str = "success",
        duration_sec: Optional[float] = None,
        rows: Optional[int] = None,
        bytes_: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if duration_sec is None and step in self._step_monotonic:
            duration_sec = max(0.0, time.monotonic() - self._step_monotonic[step])
        payload = {
            "event": "step_end",
            "step": step,
            "status": status,
            "duration_sec": duration_sec,
        }
        if rows is not None:
            payload["rows"] = rows
        if bytes_ is not None:
            payload["bytes"] = bytes_
        if extra:
            payload.update(extra)
        self.emit(payload)
        # Clear timer to avoid unbounded dict growth
        self._step_monotonic.pop(step, None)
