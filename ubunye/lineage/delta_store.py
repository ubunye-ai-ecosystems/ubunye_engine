"""Delta-compatible lineage backend.

On Databricks with an active SparkSession, this backend writes lineage
records to a Delta table via Spark SQL.  In local/CI environments
(no SparkSession), it falls back to a JSONL-based store that mirrors
Delta's append semantics — one JSON object per line, indexed by a
manifest file for search.

The fallback ensures that:

1. The ``LineageBackend`` protocol is always satisfied.
2. Unit tests run without Spark or Java.
3. Records written locally can be bulk-loaded into Delta later via
   ``ubunye sync lineage``.

Discovered via the ``ubunye.lineage_backends`` entry-point group as
``"delta"``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

from ubunye.core.errors import LineageRecordNotFoundError
from ubunye.interfaces.lineage import LineageRecord

if TYPE_CHECKING:
    from ubunye._internal.metadata_worker import MetadataWorker as _MetadataWorker

logger = logging.getLogger(__name__)


def _record_to_dict(record: LineageRecord) -> Dict:
    """Serialise a LineageRecord to a flat dict for storage."""
    d = {
        "run_id": record.run_id,
        "recorded_at": record.recorded_at,
        "engine_version": record.engine_version,
        "task": record.task,
        "usecase": record.usecase,
        "pipeline": record.pipeline,
        "target": record.target,
        "status": record.status,
        "duration_sec": record.duration_sec,
        "error": record.error,
    }
    if record.metadata:
        d["metadata"] = dict(record.metadata)  # type: ignore[assignment]  # mixed-type record dict
    return d


def _dict_to_record(d: Dict) -> LineageRecord:
    """Deserialise a flat dict back to a LineageRecord."""
    return LineageRecord(
        run_id=d["run_id"],
        recorded_at=d.get("recorded_at", ""),
        engine_version=d.get("engine_version", "unknown"),
        task=d.get("task", ""),
        usecase=d.get("usecase", ""),
        pipeline=d.get("pipeline", ""),
        target=d.get("target", ""),
        status=d.get("status", "unknown"),
        duration_sec=d.get("duration_sec"),
        error=d.get("error"),
        metadata=d.get("metadata", {}),
    )


class DeltaLineageBackend:
    """Lineage backend targeting Delta tables, with JSONL fallback.

    Parameters
    ----------
    table_or_path:
        On Databricks: a Delta table name (e.g. ``"ubunye.metadata.lineage"``).
        Locally: a directory path for JSONL fallback storage.
    """

    def __init__(self, table_or_path: str = ".ubunye/lineage_delta") -> None:
        self._table_or_path = table_or_path
        self._use_spark = self._detect_spark()
        self._local_path = Path(table_or_path) if not self._use_spark else None
        self._worker: Optional[_MetadataWorker] = None

    @staticmethod
    def _detect_spark() -> bool:
        """Check if an active SparkSession with Delta support is available."""
        try:
            from pyspark.sql import SparkSession

            session = SparkSession.getActiveSession()
            return session is not None
        except ImportError:
            return False

    def _get_worker(self) -> _MetadataWorker:
        if self._worker is None:
            from ubunye._internal.metadata_worker import MetadataWorker as _MW

            self._worker = _MW(
                write_fn=self._record_sync,
                kind="lineage",
            )
        return self._worker

    def flush(self, timeout: Optional[float] = None) -> int:
        if self._worker is None:
            return 0
        return self._worker.flush(timeout=timeout)

    # ------------------------------------------------------------------
    # LineageBackend protocol
    # ------------------------------------------------------------------

    def record(self, record: LineageRecord) -> None:
        """Persist a lineage record (non-blocking per Decision 1)."""
        self._get_worker().submit(record, run_id=record.run_id)

    def get_run(self, run_id: str) -> LineageRecord:
        """Load a single run record by ID."""
        if self._use_spark:
            return self._get_run_spark(run_id)
        return self._get_run_local(run_id)

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
        """Search across recorded runs with optional filters."""
        if self._use_spark:
            return self._search_spark(
                task=task,
                usecase=usecase,
                pipeline=pipeline,
                status=status,
                since=since,
                limit=limit,
            )
        return self._search_local(
            task=task,
            usecase=usecase,
            pipeline=pipeline,
            status=status,
            since=since,
            limit=limit,
        )

    def compact(self) -> None:
        """Optimize the underlying storage."""
        if self._use_spark:
            self._compact_spark()

    # ------------------------------------------------------------------
    # Synchronous write — called by worker thread
    # ------------------------------------------------------------------

    def _record_sync(self, record: LineageRecord) -> None:
        """Synchronous write — called by the background worker thread."""
        if self._use_spark:
            self._write_spark(record)
        else:
            self._write_local(record)

    # ------------------------------------------------------------------
    # Local JSONL fallback
    # ------------------------------------------------------------------

    def _ensure_local_dir(self) -> Path:
        assert self._local_path is not None
        self._local_path.mkdir(parents=True, exist_ok=True)
        return self._local_path

    def _write_local(self, record: LineageRecord) -> None:
        base = self._ensure_local_dir()
        record_file = base / f"{record.run_id}.json"
        record_file.write_text(
            json.dumps(_record_to_dict(record), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _get_run_local(self, run_id: str) -> LineageRecord:
        if self._local_path is None or not self._local_path.exists():
            raise LineageRecordNotFoundError(
                f"No lineage record found for run '{run_id}'.",
                context={"Run ID": run_id},
                hint="Check the run ID, or list runs with 'ubunye lineage list'.",
            )
        record_file = self._local_path / f"{run_id}.json"
        if record_file.exists():
            data = json.loads(record_file.read_text(encoding="utf-8"))
            return _dict_to_record(data)
        for f in self._local_path.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if data.get("run_id") == run_id:
                    return _dict_to_record(data)
            except Exception:
                continue
        raise LineageRecordNotFoundError(
            f"No lineage record found for run '{run_id}'.",
            context={"Run ID": run_id},
            hint="Check the run ID, or list runs with 'ubunye lineage list'.",
        )

    def _search_local(
        self,
        *,
        task: Optional[str] = None,
        usecase: Optional[str] = None,
        pipeline: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 100,
    ) -> List[LineageRecord]:
        if self._local_path is None or not self._local_path.exists():
            return []
        results: List[LineageRecord] = []
        for f in sorted(self._local_path.glob("*.json"), reverse=True):
            if len(results) >= limit:
                break
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                rec = _dict_to_record(data)
            except Exception:
                continue
            if task and rec.task != task:
                continue
            if usecase and rec.usecase != usecase:
                continue
            if pipeline and rec.pipeline != pipeline:
                continue
            if status and rec.status != status:
                continue
            if since and rec.recorded_at < since:
                continue
            results.append(rec)
        return results

    # ------------------------------------------------------------------
    # Spark/Delta implementation (used on Databricks)
    # ------------------------------------------------------------------

    def _write_spark(self, record: LineageRecord) -> None:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            self._write_local(record)
            return
        d = _record_to_dict(record)
        if "metadata" in d and isinstance(d["metadata"], dict):
            d["metadata"] = json.dumps(d["metadata"])
        row_df = spark.createDataFrame([d])
        row_df.write.mode("append").saveAsTable(self._table_or_path)

    def _get_run_spark(self, run_id: str) -> LineageRecord:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return self._get_run_local(run_id)
        df = spark.sql(f"SELECT * FROM {self._table_or_path} WHERE run_id = '{run_id}' LIMIT 1")
        rows = df.collect()
        if not rows:
            raise LineageRecordNotFoundError(
                f"No lineage record found for run '{run_id}'.",
                context={"Run ID": run_id, "Table": self._table_or_path},
                hint="Check the run ID, or list runs with 'ubunye lineage list'.",
            )
        row = rows[0].asDict()
        meta = row.get("metadata", "{}")
        if isinstance(meta, str):
            row["metadata"] = json.loads(meta)
        return _dict_to_record(row)

    def _search_spark(
        self,
        *,
        task: Optional[str] = None,
        usecase: Optional[str] = None,
        pipeline: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 100,
    ) -> List[LineageRecord]:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return self._search_local(
                task=task,
                usecase=usecase,
                pipeline=pipeline,
                status=status,
                since=since,
                limit=limit,
            )
        conditions = []
        if task:
            conditions.append(f"task = '{task}'")
        if usecase:
            conditions.append(f"usecase = '{usecase}'")
        if pipeline:
            conditions.append(f"pipeline = '{pipeline}'")
        if status:
            conditions.append(f"status = '{status}'")
        if since:
            conditions.append(f"recorded_at >= '{since}'")
        where = " AND ".join(conditions) if conditions else "1=1"
        query = (
            f"SELECT * FROM {self._table_or_path} "
            f"WHERE {where} "
            f"ORDER BY recorded_at DESC LIMIT {limit}"
        )
        rows = spark.sql(query).collect()
        results = []
        for row in rows:
            d = row.asDict()
            meta = d.get("metadata", "{}")
            if isinstance(meta, str):
                d["metadata"] = json.loads(meta)
            results.append(_dict_to_record(d))
        return results

    def _compact_spark(self) -> None:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is not None:
                spark.sql(f"OPTIMIZE {self._table_or_path}")
        except Exception as exc:
            logger.warning("Delta OPTIMIZE failed (non-fatal): %s", exc)
