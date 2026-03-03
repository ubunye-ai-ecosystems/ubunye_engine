"""Lineage record persistence backends.

``LineageStore`` is the abstract base class. ``FileSystemLineageStore`` persists
each run as a ``.json`` file under a configurable directory tree:

    {base_dir}/{usecase}/{package}/{task_name}/{run_id}.json

``S3LineageStore`` is a stub that raises ``NotImplementedError`` with a helpful
message — it is included so the entry-point registry can be extended later.

Usage
-----
    from ubunye.lineage.storage import FileSystemLineageStore

    store = FileSystemLineageStore(".ubunye/lineage")
    store.save(ctx)                         # write/overwrite JSON
    ctx2 = store.load("fraud/ingest/etl", run_id)
    runs  = store.list_runs("fraud/ingest/etl", n=10)
    errs  = store.search(status="error", since="2025-01-01")
"""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

from ubunye.lineage.context import RunContext


class LineageStore(ABC):
    """Abstract interface for lineage record storage."""

    @abstractmethod
    def save(self, ctx: RunContext) -> None:
        """Persist (create or overwrite) a lineage record."""

    @abstractmethod
    def load(self, task_path: str, run_id: str) -> RunContext:
        """Load a specific run record by task path and run ID.

        Raises
        ------
        FileNotFoundError
            If the record does not exist.
        """

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
        """Search across all recorded runs with optional filters.

        Parameters
        ----------
        task_path:
            If given, restrict results to this ``usecase/package/task`` path.
        status:
            If given, only return runs with this status (``"success"``,
            ``"error"``, ``"running"``).
        since:
            ISO-8601 date or datetime string. Only return runs where
            ``started_at >= since``.
        """


# ---------------------------------------------------------------------------
# FileSystem implementation
# ---------------------------------------------------------------------------


class FileSystemLineageStore(LineageStore):
    """Stores lineage records as JSON files in a local directory tree.

    Layout::

        {base_dir}/{usecase}/{package}/{task_name}/{run_id}.json
    """

    def __init__(self, base_dir: str = ".ubunye/lineage") -> None:
        self.base_dir = Path(base_dir)

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

    def _load_file(self, p: Path) -> RunContext:
        with p.open(encoding="utf-8") as fh:
            return RunContext.from_dict(json.load(fh))

    # ------------------------------------------------------------------
    # LineageStore interface
    # ------------------------------------------------------------------

    def save(self, ctx: RunContext) -> None:
        record_path = self._record_path(ctx)
        record_path.parent.mkdir(parents=True, exist_ok=True)
        record_path.write_text(
            json.dumps(ctx.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8",
        )

    def load(self, task_path: str, run_id: str) -> RunContext:
        task_dir = self._task_dir_from_path(task_path)
        record_path = task_dir / f"{run_id}.json"
        if not record_path.exists():
            raise FileNotFoundError(
                f"No lineage record found for task '{task_path}' run '{run_id}'. "
                f"Expected: {record_path}"
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

    def _not_impl(self) -> None:
        raise NotImplementedError(
            "S3LineageStore is not yet implemented. "
            "Use FileSystemLineageStore or implement S3 storage using boto3."
        )

    def save(self, ctx: RunContext) -> None:
        self._not_impl()

    def load(self, task_path: str, run_id: str) -> RunContext:
        self._not_impl()

    def list_runs(self, task_path: str, n: int = 20) -> List[RunContext]:
        self._not_impl()

    def search(
        self,
        task_path: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
    ) -> List[RunContext]:
        self._not_impl()
