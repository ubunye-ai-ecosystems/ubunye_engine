"""Lineage record persistence backends.

``LineageStore`` is the legacy abstract base class.
``FileSystemLineageStore`` persists each run as a ``.json`` file under a
configurable directory tree and additionally satisfies the
:class:`~ubunye.interfaces.LineageBackend` protocol.

    {base_dir}/{usecase}/{package}/{task_name}/{run_id}.json

``S3LineageStore`` is a stub that raises ``NotImplementedError`` with a
helpful message — it is included so the entry-point registry can be
extended later.

Usage
-----
    from ubunye.lineage.storage import FileSystemLineageStore

    store = FileSystemLineageStore(".ubunye/lineage")
    store.save(ctx)                         # legacy API
    store.record(lineage_record)            # protocol API
    ctx2 = store.load("fraud/ingest/etl", run_id)
    runs  = store.list_runs("fraud/ingest/etl", n=10)
    errs  = store.search(status="error", since="2025-01-01")
"""

from __future__ import annotations

import importlib.metadata as _meta
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from ubunye.core.errors import LineageRecordNotFoundError
from ubunye.interfaces.lineage import LineageRecord
from ubunye.lineage.context import RunContext

if TYPE_CHECKING:
    from ubunye._internal.metadata_worker import MetadataWorker as _MetadataWorker


def _engine_version() -> str:
    try:
        return _meta.version("ubunye-engine")
    except _meta.PackageNotFoundError:
        return "unknown"


_KNOWN_META_KEYS = {
    "profile",
    "model",
    "config_version",
    "config_hash",
    "ended_at",
    "inputs",
    "outputs",
}


def _record_to_ctx(record: LineageRecord) -> RunContext:
    """Convert a protocol LineageRecord to a legacy RunContext for storage.

    Extra metadata keys (beyond the known set) are round-tripped via a
    ``_extra_metadata`` field in the serialised JSON so they survive the
    RunContext→JSON→RunContext→LineageRecord conversion.
    """
    ctx = RunContext(
        run_id=record.run_id,
        task_path=f"{record.usecase}/{record.pipeline}/{record.task}",
        usecase=record.usecase,
        package=record.pipeline,
        task_name=record.task,
        profile=record.metadata.get("profile", ""),
        model=record.metadata.get("model", ""),
        version=record.metadata.get("config_version", ""),
        config_hash=record.metadata.get("config_hash", ""),
        started_at=record.recorded_at,
        ended_at=record.metadata.get("ended_at"),
        duration_sec=record.duration_sec,
        status=record.status,
        error=record.error,
    )
    extras = {k: v for k, v in record.metadata.items() if k not in _KNOWN_META_KEYS}
    if extras:
        ctx._extra_metadata = extras  # type: ignore[attr-defined]
    return ctx


def _ctx_to_record(ctx: RunContext) -> LineageRecord:
    """Convert a legacy RunContext to a protocol LineageRecord."""
    metadata = {}
    if ctx.profile:
        metadata["profile"] = ctx.profile
    if ctx.model:
        metadata["model"] = ctx.model
    if ctx.version:
        metadata["config_version"] = ctx.version
    if ctx.config_hash:
        metadata["config_hash"] = ctx.config_hash
    if ctx.ended_at:
        metadata["ended_at"] = ctx.ended_at
    if ctx.inputs:
        metadata["inputs"] = json.dumps([s.to_dict() for s in ctx.inputs])
    if ctx.outputs:
        metadata["outputs"] = json.dumps([s.to_dict() for s in ctx.outputs])
    extras = getattr(ctx, "_extra_metadata", None)
    if extras:
        metadata.update(extras)

    return LineageRecord(
        run_id=ctx.run_id,
        recorded_at=ctx.started_at,
        engine_version=_engine_version(),
        task=ctx.task_name,
        usecase=ctx.usecase,
        pipeline=ctx.package,
        target=ctx.profile or "",
        status=ctx.status,
        duration_sec=ctx.duration_sec,
        error=ctx.error,
        metadata=metadata,
    )


class LineageStore(ABC):
    """Abstract interface for lineage record storage (legacy API)."""

    @abstractmethod
    def save(self, ctx: RunContext) -> None:
        """Persist (create or overwrite) a lineage record."""

    @abstractmethod
    def load(self, task_path: str, run_id: str) -> RunContext:
        """Load a specific run record by task path and run ID."""

    @abstractmethod
    def list_runs(self, task_path: str, n: int = 20) -> List[RunContext]:
        """Return the *n* most recent run records for a task, newest first."""

    @abstractmethod
    def search(
        self,
        task_path: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
    ) -> List[RunContext]:
        """Search across all recorded runs with optional filters."""


# ---------------------------------------------------------------------------
# FileSystem implementation
# ---------------------------------------------------------------------------


class FileSystemLineageStore(LineageStore):
    """Stores lineage records as JSON files in a local directory tree.

    Satisfies both the legacy :class:`LineageStore` ABC and the
    :class:`~ubunye.interfaces.LineageBackend` protocol.

    Layout::

        {base_dir}/{usecase}/{package}/{task_name}/{run_id}.json
    """

    def __init__(self, base_dir: str = ".ubunye/lineage") -> None:
        self.base_dir = Path(base_dir)
        self._worker: Optional[_MetadataWorker] = None
        self._run_index: Optional[dict] = None
        self._ctx_cache: dict = {}

    def _get_worker(self) -> _MetadataWorker:
        if self._worker is None:
            from ubunye._internal.metadata_worker import MetadataWorker as _MW

            self._worker = _MW(
                write_fn=self._record_sync,
                kind="lineage",
            )
        return self._worker

    def flush(self, timeout: Optional[float] = None) -> int:
        """Drain the background worker queue.

        Returns the number of records that went to fallback manifests.
        """
        if self._worker is None:
            return 0
        return self._worker.flush(timeout=timeout)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _task_dir(self, ctx: RunContext) -> Path:
        return self.base_dir / ctx.usecase / ctx.package / ctx.task_name

    def _task_dir_from_path(self, task_path: str) -> Path:
        """task_path is ``usecase/package/task_name``."""
        parts = task_path.strip("/").split("/")
        return self.base_dir.joinpath(*parts)

    def _record_path(self, ctx: RunContext) -> Path:
        return self._task_dir(ctx) / f"{ctx.run_id}.json"

    def _build_run_index(self) -> dict:
        """Build a run_id→Path mapping from the directory tree."""
        index: dict = {}
        if self.base_dir.exists():
            for p in self.base_dir.rglob("*.json"):
                index[p.stem] = p
        self._run_index = index
        return index

    def _get_run_index(self) -> dict:
        if self._run_index is None:
            return self._build_run_index()
        return self._run_index

    def _index_record(self, run_id: str, path: Path) -> None:
        """Add or update a single entry in the run index."""
        if self._run_index is not None:
            self._run_index[run_id] = path

    def _load_file(self, p: Path) -> RunContext:
        run_id = p.stem
        cached = self._ctx_cache.get(run_id)
        if cached is not None:
            return cached
        with p.open(encoding="utf-8") as fh:
            data = json.load(fh)
        ctx = RunContext.from_dict(data)
        extras = data.get("_extra_metadata")
        if extras:
            ctx._extra_metadata = extras  # type: ignore[attr-defined]
        self._ctx_cache[run_id] = ctx
        return ctx

    # ------------------------------------------------------------------
    # LineageStore (legacy) interface
    # ------------------------------------------------------------------

    def save(self, ctx: RunContext) -> None:
        record_path = self._record_path(ctx)
        record_path.parent.mkdir(parents=True, exist_ok=True)
        record_path.write_text(
            json.dumps(ctx.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        self._ctx_cache[ctx.run_id] = ctx

    def load(self, task_path: str, run_id: str) -> RunContext:
        task_dir = self._task_dir_from_path(task_path)
        record_path = task_dir / f"{run_id}.json"
        if not record_path.exists():
            raise LineageRecordNotFoundError(
                f"No lineage record found for task '{task_path}' run '{run_id}'.",
                context={"Task": task_path, "Run ID": run_id, "Expected": str(record_path)},
                hint="Check the task path and run ID, or list runs with 'ubunye lineage list'.",
            )
        return self._load_file(record_path)

    def list_runs(self, task_path: str, n: int = 20) -> List[RunContext]:
        task_dir = self._task_dir_from_path(task_path)
        if not task_dir.exists():
            return []
        files = sorted(task_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        records: List[RunContext] = []
        for f in files[:n]:
            try:
                records.append(self._load_file(f))
            except Exception:
                continue
        return records

    def search(
        self,
        task_path: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
    ) -> List[RunContext]:
        root = self._task_dir_from_path(task_path) if task_path else self.base_dir
        if not root.exists():
            return []

        results: List[RunContext] = []
        for json_file in sorted(
            root.rglob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        ):
            try:
                ctx = self._load_file(json_file)
            except Exception:
                continue

            if status and ctx.status != status:
                continue
            if since and ctx.started_at < since:
                continue

            results.append(ctx)

        return results

    # ------------------------------------------------------------------
    # LineageBackend (protocol) interface
    # ------------------------------------------------------------------

    def _record_sync(self, record: LineageRecord) -> None:
        """Synchronous write — called by the background worker thread."""
        ctx = _record_to_ctx(record)
        record_path = self._record_path(ctx)
        record_path.parent.mkdir(parents=True, exist_ok=True)
        data = ctx.to_dict()
        extras = {k: v for k, v in record.metadata.items() if k not in _KNOWN_META_KEYS}
        if extras:
            data["_extra_metadata"] = extras
        record_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        self._ctx_cache[ctx.run_id] = ctx
        self._index_record(ctx.run_id, record_path)

    def record(self, record: LineageRecord) -> None:
        """Persist a lineage record (non-blocking per Decision 1).

        Dispatches the write to a background worker thread.  On failure,
        the record is written to a fallback manifest (Decision 2).
        """
        self._get_worker().submit(record, run_id=record.run_id)

    def get_run(self, run_id: str) -> LineageRecord:
        """Load a single run record by run ID.

        Uses a run_id→path index for O(1) lookup instead of rglob.
        """
        if not self.base_dir.exists():
            raise LineageRecordNotFoundError(
                f"No lineage record found for run '{run_id}'.",
                context={"Run ID": run_id},
                hint="Check the run ID, or list runs with 'ubunye lineage list'.",
            )
        index = self._get_run_index()
        json_file = index.get(run_id)
        if json_file is not None and json_file.exists():
            try:
                ctx = self._load_file(json_file)
                return _ctx_to_record(ctx)
            except Exception:
                pass
        self._build_run_index()
        json_file = self._run_index.get(run_id)  # type: ignore[union-attr]
        if json_file is not None and json_file.exists():
            try:
                ctx = self._load_file(json_file)
                return _ctx_to_record(ctx)
            except Exception:
                pass
        raise LineageRecordNotFoundError(
            f"No lineage record found for run '{run_id}'.",
            context={"Run ID": run_id},
            hint="Check the run ID, or list runs with 'ubunye lineage list'.",
        )

    def search_records(
        self,
        *,
        task: Optional[str] = None,
        usecase: Optional[str] = None,
        pipeline: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 100,
    ) -> List[LineageRecord]:
        """Protocol-compatible search returning LineageRecord objects."""
        task_path = None
        if usecase and pipeline and task:
            task_path = f"{usecase}/{pipeline}/{task}"
        elif usecase and pipeline:
            task_path = f"{usecase}/{pipeline}"
        elif usecase:
            task_path = usecase

        contexts = self.search(task_path=task_path, status=status, since=since)
        return [_ctx_to_record(ctx) for ctx in contexts[:limit]]

    def compact(self) -> None:
        """No-op for filesystem backend."""


# ---------------------------------------------------------------------------
# S3 stub
# ---------------------------------------------------------------------------


class S3LineageStore(LineageStore):
    """Stub S3 lineage store — raises ``NotImplementedError`` on all operations.

    To implement, install ``boto3`` and override each method to read/write from
    an S3 prefix such as ``s3://{bucket}/.ubunye/lineage/{task_path}/{run_id}.json``.
    """

    def __init__(self, base_path: str) -> None:
        self.base_path = base_path

    def _error(self) -> NotImplementedError:
        return NotImplementedError(
            "S3LineageStore is not yet implemented. "
            "Use FileSystemLineageStore or implement S3 storage using boto3."
        )

    def _not_impl(self) -> None:
        raise self._error()

    def save(self, ctx: RunContext) -> None:
        self._not_impl()

    def load(self, task_path: str, run_id: str) -> RunContext:
        raise self._error()

    def list_runs(self, task_path: str, n: int = 20) -> List[RunContext]:
        raise self._error()

    def search(
        self,
        task_path: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
    ) -> List[RunContext]:
        raise self._error()
