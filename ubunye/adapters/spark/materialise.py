"""Compute a Spark output once, for every consumer of it (ADR 009).

The engine checks an output, writes it and hashes it for the run record. On a
lazy DataFrame each of those is a new Spark job that computes the plan again,
so a column that differs per computation (``current_timestamp()``, a UDF that
calls a service, a source that changed) gave the record a digest of rows that
were never written (F-040), and every check and write paid for the transform
again (F-039, F-043).

``localCheckpoint(eager=True)`` computes the rows once and cuts the plan: later
actions read the held rows and nothing else. If a held block is lost (its
executor died), the next action fails with ``CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND``
instead of recomputing. ``persist()`` was measured against it and rejected: after
a lost block it recomputes quietly, so the checks passed rows the writer never
wrote, and an overwrite of the task's own input refreshed the cache from the new
files, so the record hashed rows that were not written
(``tasks/hardening/experiments/e06/e06_materialise.py``).
"""

from __future__ import annotations

import logging
from typing import Any, Optional

log = logging.getLogger(__name__)


def materialise(df: Any) -> Optional[Any]:
    """``df`` computed once and held in executor memory and disk, or ``None``.

    ``None`` when it cannot be done here: a streaming frame, a frame without
    ``localCheckpoint`` (a mock), or a session that refuses it. The caller then
    goes on as before, and the run record says the hash was recomputed.
    """
    if getattr(df, "isStreaming", False) or not hasattr(df, "localCheckpoint"):
        return None
    try:
        from pyspark import StorageLevel

        level = StorageLevel.MEMORY_AND_DISK
    except Exception:  # noqa: BLE001 (no pyspark: not a Spark frame)
        return None
    try:
        try:
            return df.localCheckpoint(eager=True, storageLevel=level)
        except TypeError:  # pyspark before 4.0 takes no storage level
            return df.localCheckpoint(eager=True)
    except Exception as exc:  # noqa: BLE001 (never fail a run for this)
        log.warning(
            "Could not compute the output once and hold it (%s); every check, the "
            "write and the run record's hash will each compute it.",
            _first_line(exc),
        )
        return None


def release(df: Any) -> None:
    """Free the rows :func:`materialise` held. Never raises.

    On classic Spark the held rows are the RDD behind the frame's plan; they
    would otherwise stay on the executors until the JVM's garbage collector
    notices. On Spark Connect the server frees them when the frame object is
    freed, which happens once the engine drops its last reference.
    """
    try:
        jdf = getattr(df, "_jdf", None)
        if jdf is None:
            return
        jdf.logicalPlan().rdd().unpersist(False)
    except Exception as exc:  # noqa: BLE001
        log.debug("release of a held output failed: %s", _first_line(exc))


def _first_line(exc: Exception) -> str:
    lines = [ln.strip() for ln in str(exc).splitlines() if ln.strip()]
    return (lines[0] if lines else type(exc).__name__)[:200]
