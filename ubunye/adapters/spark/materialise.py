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
    ``localCheckpoint`` (a mock), a platform that refuses it (serverless, an old
    Connect server), or a result that is not actually held. The caller then goes
    on as before, and the run record says the hash was recomputed.

    When the computation itself fails (the transform raised, a UDF's service
    returned an error), that error is raised here. Falling back would run the
    same failing plan a second time, doubling every side effect of the transform
    (paid API calls, rows posted) before failing with the same error.
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
            held = df.localCheckpoint(eager=True, storageLevel=level)
        except TypeError:  # pyspark before 4.0 takes no storage level
            held = df.localCheckpoint(eager=True)
    except Exception as exc:  # noqa: BLE001 (sorted below)
        if not _refused(exc):
            raise
        log.warning(
            "This platform cannot hold the output once (%s); every check, the "
            "write and the run record's hash will each compute it.",
            _first_line(exc),
        )
        return None
    if not _is_held(held):
        log.warning(
            "localCheckpoint returned a frame that still recomputes its plan; "
            "every check, the write and the run record's hash will each compute it."
        )
        return None
    return held


# What a platform says when it will not hold a frame, as opposed to a job that
# failed while computing it. Serverless compute and older Spark Connect servers
# refuse localCheckpoint with one of these; a failing transform never does.
_REFUSALS = ("NOT_SUPPORTED", "NOT_IMPLEMENTED", "UNSUPPORTED", "not supported")


def _refused(exc: BaseException) -> bool:
    """True when the platform refused to hold the frame, False when the job failed."""
    if _job_failed(exc):
        return False
    if isinstance(exc, NotImplementedError) or type(exc).__name__ == "PySparkNotImplementedError":
        return True
    return _says_refused(exc)


def _says_refused(exc: BaseException) -> bool:
    first = _first_line(exc)
    return any(word in first for word in _REFUSALS)


def _job_failed(exc: BaseException) -> bool:
    """A Spark job ran and failed: a Python worker error or a SparkException."""
    if type(exc).__name__ in ("PythonException", "SparkException"):
        return True
    java = getattr(exc, "java_exception", None)  # classic Py4JJavaError
    try:
        return java is not None and "SparkException" in str(java.getClass().getName())
    except Exception:  # noqa: BLE001
        return False


def _is_held(held: Any) -> bool:
    """True unless the frame's plan is visibly not the held rows.

    A checkpointed frame's plan is a ``LogicalRDD`` over the held blocks. Where
    the plan cannot be seen (Spark Connect has no ``_jdf``), the result is trusted.
    """
    jdf = getattr(held, "_jdf", None)
    if jdf is None:
        return held is not None
    try:
        plan = jdf.queryExecution().logical()
        return str(plan.getClass().getSimpleName()) == "LogicalRDD"
    except Exception:  # noqa: BLE001 (cannot inspect: trust the checkpoint)
        return True


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


def _first_line(exc: BaseException) -> str:
    lines = [ln.strip() for ln in str(exc).splitlines() if ln.strip()]
    return (lines[0] if lines else type(exc).__name__)[:200]
