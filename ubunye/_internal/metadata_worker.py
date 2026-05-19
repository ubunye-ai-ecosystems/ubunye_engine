"""Background worker for non-blocking metadata writes (Decision 1).

Metadata operations (lineage records, registry index updates) are
dispatched to a background daemon thread via a bounded queue.  This
ensures that a slow or failing metadata backend does not block
pipeline execution.

**Queue overflow** (Decision 1): When the queue is full, the oldest
pending write is dropped and a warning is logged.

**Backend failure** (Decision 2): When a write raises an exception,
the record is appended to a local fallback manifest at
``~/.ubunye/fallback/{run_id}/{kind}.jsonl`` and pipeline execution
continues.  ``ubunye sync`` replays the manifest later.

**Shutdown**: :meth:`MetadataWorker.flush` drains the queue with a
configurable timeout (default 30s).  The daemon thread exits when the
sentinel ``None`` is enqueued.

Uses a worker-thread pattern, not async/await — async fights with
Spark's threading model in Python.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from queue import Full, Queue
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

_DEFAULT_QUEUE_SIZE = 1000
_DEFAULT_FLUSH_TIMEOUT = 30.0
_FALLBACK_ROOT = Path.home() / ".ubunye" / "fallback"

_SENTINEL = None


def _get_queue_size() -> int:
    try:
        return int(os.environ.get("UBUNYE_METADATA_QUEUE_SIZE", _DEFAULT_QUEUE_SIZE))
    except (TypeError, ValueError):
        return _DEFAULT_QUEUE_SIZE


def _get_flush_timeout() -> float:
    try:
        return float(os.environ.get("UBUNYE_METADATA_FLUSH_TIMEOUT", _DEFAULT_FLUSH_TIMEOUT))
    except (TypeError, ValueError):
        return _DEFAULT_FLUSH_TIMEOUT


class MetadataWorker:
    """Manages a bounded queue and a daemon thread for background writes.

    Parameters
    ----------
    write_fn:
        The synchronous write function that the worker calls for each
        record.  Signature: ``write_fn(record) -> None``.
    kind:
        Category label for fallback manifests (``"lineage"`` or
        ``"registry"``).
    queue_size:
        Maximum pending writes.  ``None`` reads from
        ``UBUNYE_METADATA_QUEUE_SIZE`` (default 1000).
    """

    def __init__(
        self,
        write_fn: Callable[[Any], None],
        kind: str = "metadata",
        queue_size: Optional[int] = None,
    ) -> None:
        self._write_fn = write_fn
        self._kind = kind
        self._queue: Queue = Queue(maxsize=queue_size or _get_queue_size())
        self._thread: Optional[threading.Thread] = None
        self._started = False
        self._lock = threading.Lock()
        self._fallback_count = 0
        self._drop_count = 0

    @property
    def fallback_count(self) -> int:
        return self._fallback_count

    @property
    def drop_count(self) -> int:
        return self._drop_count

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(self, record: Any, run_id: str = "unknown") -> None:
        """Enqueue a record for background writing.

        If the queue is full, drops the oldest pending item and logs a
        warning (Decision 1 overflow policy).
        """
        self._ensure_started()
        item = _WorkItem(record=record, run_id=run_id)
        try:
            self._queue.put_nowait(item)
        except Full:
            try:
                dropped = self._queue.get_nowait()
                self._drop_count += 1
                logger.warning(
                    "Metadata queue full (%s) — dropped oldest record (run_id=%s)",
                    self._kind,
                    dropped.run_id if isinstance(dropped, _WorkItem) else "?",
                )
            except Exception:
                pass
            try:
                self._queue.put_nowait(item)
            except Full:
                self._drop_count += 1
                logger.warning(
                    "Metadata queue still full after drop (%s) — record lost",
                    self._kind,
                )

    def flush(self, timeout: Optional[float] = None) -> int:
        """Drain the queue and stop the worker thread.

        Returns the number of records that went to fallback manifests.
        """
        if not self._started:
            return 0
        timeout = timeout if timeout is not None else _get_flush_timeout()
        self._queue.put(_SENTINEL)
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._started = False
        return self._fallback_count

    def summary(self) -> Dict[str, int]:
        """Return counts for end-of-run reporting."""
        return {
            "pending": self._queue.qsize(),
            "fallback": self._fallback_count,
            "dropped": self._drop_count,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_started(self) -> None:
        if self._started:
            return
        with self._lock:
            if self._started:
                return
            self._thread = threading.Thread(
                target=self._worker_loop,
                name=f"ubunye-metadata-{self._kind}",
                daemon=True,
            )
            self._thread.start()
            self._started = True

    def _worker_loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is _SENTINEL:
                self._queue.task_done()
                break
            if not isinstance(item, _WorkItem):
                self._queue.task_done()
                continue
            try:
                self._write_fn(item.record)
            except Exception as exc:
                logger.warning(
                    "Metadata write failed (%s, run_id=%s): %s",
                    self._kind,
                    item.run_id,
                    exc,
                )
                self._write_fallback(item)
                self._fallback_count += 1
            finally:
                self._queue.task_done()

    def _write_fallback(self, item: _WorkItem) -> None:
        """Append the failed record to a fallback JSONL manifest (Decision 2)."""
        try:
            fallback_dir = _FALLBACK_ROOT / item.run_id
            fallback_dir.mkdir(parents=True, exist_ok=True)
            manifest = fallback_dir / f"{self._kind}.jsonl"

            payload = _serialize_record(item.record)
            payload["_fallback_at"] = datetime.now(timezone.utc).isoformat()

            with manifest.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, default=str) + "\n")
        except Exception as exc:
            logger.error(
                "Failed to write fallback manifest (%s, run_id=%s): %s",
                self._kind,
                item.run_id,
                exc,
            )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


class _WorkItem:
    __slots__ = ("record", "run_id")

    def __init__(self, record: Any, run_id: str) -> None:
        self.record = record
        self.run_id = run_id


def _serialize_record(record: Any) -> Dict[str, Any]:
    """Best-effort serialization of a record to a JSON-safe dict."""
    if hasattr(record, "to_dict"):
        return record.to_dict()
    if hasattr(record, "__dataclass_fields__"):
        from dataclasses import asdict

        return asdict(record)
    if isinstance(record, dict):
        return record
    return {"_raw": str(record)}
