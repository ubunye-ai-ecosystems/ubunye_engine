"""LineageBackend protocol — contract for lineage storage.

Schema evolution (Decision 3)
-----------------------------
``LineageRecord`` separates strict core columns from a flexible
``metadata: Dict[str, str]`` payload:

* Core columns (``run_id``, ``recorded_at``, ``engine_version``,
  ``task``, ``usecase``, ``pipeline``, ``target``) never change
  without a major version bump.
* New data goes into ``metadata`` — old clients ignore unknown keys,
  new clients write without touching the schema.
* Structured step data (sources, sinks) is in ``metadata`` for
  cross-backend portability; backends that support richer types
  (e.g. Delta arrays) may promote them internally.
* Reads explicitly project columns; never ``SELECT *``.

Non-blocking writes (Decision 1)
---------------------------------
``record()`` returns immediately.  The actual write is dispatched to a
background worker thread via a bounded queue (default size 1000,
configurable via ``UBUNYE_METADATA_QUEUE_SIZE``).

* On queue overflow: drop oldest pending write, log warning.
* On engine shutdown: flush with configurable timeout (default 30s).
* Conformance test: a backend that takes 5 seconds per write must NOT
  slow a pipeline logging 10 records.

Uses a worker-thread pattern, not async/await — async fights with
Spark's threading model in Python.

Graceful degradation (Decision 2)
----------------------------------
When a write fails, the record is appended to a local fallback manifest
at ``~/.ubunye/fallback/{run_id}/lineage.jsonl``.  Pipeline execution
continues.  At end-of-run, a summary message reports how many records
went to fallback.  ``ubunye sync lineage`` replays the manifest with
idempotent deduplication (key: ``run_id + task + recorded_at``).

Implementations are discovered via the ``ubunye.lineage_backends``
entry-point group.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol, runtime_checkable


@dataclass
class LineageRecord:
    """Cross-boundary lineage record (Decision 3 compliant).

    Core columns are strict; everything else goes into ``metadata``.
    Every record stamps ``engine_version`` so analysts can filter by
    writer version.
    """

    run_id: str
    recorded_at: str
    engine_version: str
    task: str
    usecase: str
    pipeline: str
    target: str
    status: str = "running"
    duration_sec: Optional[float] = None
    error: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)


@runtime_checkable
class LineageBackend(Protocol):
    """Structural interface for lineage storage backends.

    Concrete implementations: ``FilesystemLineageBackend`` (built-in),
    ``DeltaLineageBackend`` (Phase 2).
    """

    def record(self, record: LineageRecord) -> None:
        """Persist a lineage record (non-blocking per Decision 1).

        Creates or overwrites the record for the given ``run_id``.
        Typically called twice per task: once at start
        (``status="running"``) and once at end (``status="success"``
        or ``"error"``).

        The actual write is dispatched to a background worker thread.
        On failure, the record is written to a fallback manifest
        (Decision 2).

        Parameters
        ----------
        record:
            The lineage record to persist.  Callers construct this
            with the current engine version and task metadata.
        """
        ...

    def get_run(self, run_id: str) -> LineageRecord:
        """Load a single run record by ID.

        Raises
        ------
        ubunye.core.errors.LineageRecordNotFoundError
            When no record exists for ``run_id``.
        """
        ...

    def search(
        self,
        *,
        task: Optional[str] = None,
        usecase: Optional[str] = None,
        pipeline: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 100,
    ) -> List[LineageRecord]:
        """Search across recorded runs with optional filters.

        Parameters
        ----------
        task:
            Filter by task name.
        usecase:
            Filter by use case.
        pipeline:
            Filter by pipeline.
        status:
            Filter by status (``"success"``, ``"error"``, ``"running"``).
        since:
            ISO-8601 datetime; only return runs recorded at or after.
        limit:
            Maximum number of records to return.
        """
        ...

    def compact(self) -> None:
        """Optimize the underlying storage.

        For filesystem backends this is a no-op.  For Delta backends
        this triggers ``OPTIMIZE`` on the lineage table.
        """
        ...
