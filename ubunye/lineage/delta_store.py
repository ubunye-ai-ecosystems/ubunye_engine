"""Run records in a Delta table (ADR 011).

Registered as ``delta`` in the ``ubunye.lineage_backends`` group. Built with one
argument, its location:

* a table name (``main.ops.ubunye_runs``), written with ``saveAsTable``;
* or a path (anything with ``/``, ``\\`` or ``:``, such as ``s3://bucket/runs``
  or ``/dbfs/ops/runs``), written with ``save``.

Each save appends one row: the whole run record as JSON (``record``), so no field
is lost, plus a few columns to find it by: ``run_id``, ``task_path``,
``usecase``, ``package``, ``task``, ``status``, ``started_at``, ``ended_at``,
``variables`` (the batch: ``dt``, ``mode``, ...), ``record_version`` and
``written_at``. A run is saved twice (running, then finished); a read takes the
row written last for each run. Appends never conflict, so jobs on different
clusters can write to one table at once.

It needs a SparkSession with Delta: the active one, or a local one it starts
(``pip install 'ubunye-engine[delta]'``). When a record cannot be written, it is
kept in a fallback manifest (``~/.ubunye/fallback/<run_id>/lineage.jsonl``,
Decision 2) and a warning says so; ``ubunye sync lineage --lineage-backend
delta`` replays it. It never goes quietly to a local file other jobs cannot see.

The older ``LineageRecord`` methods (``record``, ``get_run``, ``search_records``,
``compact``) are kept. As before, without an active SparkSession when the
backend is built they use JSON files under the location; that is now logged.
"""

from __future__ import annotations

import json
import logging
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ubunye.core.errors import LineageRecordNotFoundError
from ubunye.interfaces.lineage import LineageRecord
from ubunye.lineage.context import RunContext

if TYPE_CHECKING:
    from ubunye._internal.metadata_worker import MetadataWorker as _MetadataWorker

logger = logging.getLogger(__name__)

_DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


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


def is_path(location: str) -> bool:
    """A path (``s3://...``, ``/dbfs/...``, ``./runs``), not a table name."""
    return any(c in location for c in "/\\:")


def _schema() -> Any:
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    s = StringType()
    return StructType(
        [
            StructField("run_id", s, False),
            StructField("task_path", s, False),
            StructField("usecase", s, True),
            StructField("package", s, True),
            StructField("task", s, True),
            StructField("status", s, True),
            StructField("started_at", s, True),
            StructField("ended_at", s, True),
            StructField("variables", MapType(s, s), True),
            StructField("record_version", IntegerType(), True),
            StructField("written_at", LongType(), False),
            StructField("record", s, False),
        ]
    )


def _row(ctx: RunContext) -> tuple:
    doc = ctx.to_dict()
    extras = getattr(ctx, "_extra_metadata", None)
    if extras:
        doc["_extra_metadata"] = extras
    variables = {
        str(k): v if isinstance(v, str) else json.dumps(v, default=str)
        for k, v in (ctx.variables or {}).items()
    }
    return (
        ctx.run_id,
        ctx.task_path,
        ctx.usecase,
        ctx.package,
        ctx.task_name,
        ctx.status,
        ctx.started_at,
        ctx.ended_at,
        variables,
        int(ctx.record_version),
        time.time_ns(),
        json.dumps(doc, ensure_ascii=False, default=str),
    )


def _from_json(text: str) -> RunContext:
    doc = json.loads(text)
    ctx = RunContext.from_dict(doc)
    extras = doc.get("_extra_metadata")
    if extras:
        ctx._extra_metadata = extras  # type: ignore[attr-defined]
    return ctx


class DeltaLineageBackend:
    """Run records in a Delta table (a :class:`~ubunye.interfaces.lineage.RunRecordStore`).

    Parameters
    ----------
    table_or_path:
        A Delta table name (``"main.ops.ubunye_runs"``) or a path
        (``"s3://bucket/ubunye/runs"``), used unchanged.
    """

    def __init__(self, table_or_path: str = ".ubunye/lineage_delta") -> None:
        self._table_or_path = str(table_or_path)
        # For the older LineageRecord methods only (see the module docstring).
        self._use_spark = self._detect_spark()
        self._local_path = Path(table_or_path) if not self._use_spark else None
        self._warned_local = False
        self._worker: Optional[_MetadataWorker] = None

    @staticmethod
    def _detect_spark() -> bool:
        """Check if an active SparkSession is available."""
        try:
            from pyspark.sql import SparkSession

            session = SparkSession.getActiveSession()
            return session is not None
        except ImportError:
            return False

    # ------------------------------------------------------------------
    # Spark access
    # ------------------------------------------------------------------

    def _session(self) -> Any:
        """The active SparkSession, or a local one with Delta, started here."""
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise ImportError(
                "The 'delta' lineage backend needs PySpark and Delta Lake. "
                "Try: pip install 'ubunye-engine[delta]'"
            ) from exc
        spark = SparkSession.getActiveSession()
        if spark is not None:
            return spark
        logger.info(
            "No active SparkSession: starting a local one with Delta for the lineage " "store %s.",
            self._table_or_path,
        )
        builder = (
            SparkSession.builder.appName("ubunye-lineage")
            .config("spark.sql.extensions", _DELTA_EXTENSION)
            .config("spark.sql.catalog.spark_catalog", _DELTA_CATALOG)
        )
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass  # the jars may already be on the classpath (a cluster image)
        return builder.getOrCreate()

    def _exists(self, spark: Any) -> bool:
        loc = self._table_or_path
        if not is_path(loc):
            return bool(spark.catalog.tableExists(loc))
        try:
            from delta.tables import DeltaTable

            return bool(DeltaTable.isDeltaTable(spark, loc))
        except ImportError:
            try:
                spark.read.format("delta").load(loc).limit(0).collect()
                return True
            except Exception:
                return False

    def _latest(self, task_path: Optional[str] = None) -> Any:
        """The last row written for each run (of the task or task prefix), or None."""
        spark = self._session()
        if not self._exists(spark):
            return None
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        loc = self._table_or_path
        df = spark.read.format("delta").load(loc) if is_path(loc) else spark.table(loc)
        if task_path:
            tp = task_path.strip("/")
            df = df.filter((F.col("task_path") == tp) | F.col("task_path").startswith(tp + "/"))
        w = Window.partitionBy("task_path", "run_id").orderBy(F.col("written_at").desc())
        return df.withColumn("_n", F.row_number().over(w)).filter(F.col("_n") == 1).drop("_n")

    def _append(self, ctx: RunContext) -> None:
        spark = self._session()
        df = spark.createDataFrame([_row(ctx)], schema=_schema())
        writer = df.write.format("delta").mode("append")
        if is_path(self._table_or_path):
            writer.save(self._table_or_path)
        else:
            writer.saveAsTable(self._table_or_path)

    def _fallback(self, ctx: RunContext, exc: Exception) -> None:
        """Keep a record that could not be written (Decision 2), and say so."""
        from ubunye._internal import metadata_worker

        manifest = metadata_worker._FALLBACK_ROOT / ctx.run_id / "lineage.jsonl"
        doc = ctx.to_dict()
        doc["_fallback_at"] = datetime.now(timezone.utc).isoformat()
        doc["_lineage_backend"] = "delta"
        doc["_location"] = self._table_or_path
        try:
            manifest.parent.mkdir(parents=True, exist_ok=True)
            with manifest.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")
            kept = f"It was kept in {manifest}"
        except OSError as io_exc:
            kept = f"It could not be kept either ({io_exc})"
        logger.warning(
            "The run record of %s (run %s) was not written to the Delta lineage store %s: "
            "%s. %s; replay it with 'ubunye sync lineage --lineage-backend delta "
            "--lineage-dir %s'.",
            ctx.task_path,
            ctx.run_id,
            self._table_or_path,
            exc,
            kept,
            self._table_or_path,
        )

    # ------------------------------------------------------------------
    # RunRecordStore: the run record contract (ADR 011)
    # ------------------------------------------------------------------

    def save(self, ctx: RunContext) -> None:
        """Append the record; if that fails, keep it in a fallback manifest and warn."""
        try:
            self._append(ctx)
        except Exception as exc:
            self._fallback(ctx, exc)

    def load(self, task_path: str, run_id: str) -> RunContext:
        """One run of a task, by its run id or the first characters of it."""
        from pyspark.sql import functions as F

        latest = self._latest(task_path)
        rows: List[Any] = []
        if latest is not None:
            latest = latest.filter(F.col("task_path") == task_path.strip("/"))
            rows = latest.filter(F.col("run_id") == run_id).select("run_id", "record").collect()
            if not rows and run_id:
                rows = (
                    latest.filter(F.col("run_id").startswith(run_id))
                    .select("run_id", "record")
                    .limit(6)
                    .collect()
                )
        if len(rows) > 1:
            raise LineageRecordNotFoundError(
                f"Run id '{run_id}' fits {len(rows)} runs of '{task_path}'.",
                context={"Task": task_path, "Matches": ", ".join(r["run_id"] for r in rows[:5])},
                hint="Give more characters of the run id.",
            )
        if not rows:
            raise LineageRecordNotFoundError(
                f"No lineage record found for task '{task_path}' run '{run_id}'.",
                context={"Task": task_path, "Run ID": run_id, "Store": self._table_or_path},
                hint="Check the task path and run ID, or list runs with 'ubunye lineage list'.",
            )
        return _from_json(rows[0]["record"])

    def list_runs(self, task_path: str, n: int = 20) -> List[RunContext]:
        """The ``n`` newest records of one task, newest first."""
        from pyspark.sql import functions as F

        latest = self._latest(task_path)
        if latest is None:
            return []
        latest = latest.filter(F.col("task_path") == task_path.strip("/"))
        return self._collect(latest, n)

    def search(
        self,
        task_path: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: Optional[int] = None,
        *,
        task: Optional[str] = None,
        usecase: Optional[str] = None,
        pipeline: Optional[str] = None,
    ) -> List[Any]:
        """Records newest first, by task path (or its first parts), status and since.

        ``task``, ``usecase`` and ``pipeline`` are the older ``LineageRecord``
        search: they return LineageRecords, with a DeprecationWarning; use
        :meth:`search_records` for that.
        """
        if task is not None or usecase is not None or pipeline is not None:
            warnings.warn(
                "DeltaLineageBackend.search(task=, usecase=, pipeline=) is now "
                "search_records(...); search() takes task_path= and returns run records.",
                DeprecationWarning,
                stacklevel=2,
            )
            return self.search_records(
                task=task,
                usecase=usecase,
                pipeline=pipeline,
                status=status,
                since=since,
                limit=100 if limit is None else limit,
            )
        from pyspark.sql import functions as F

        latest = self._latest(task_path)
        if latest is None:
            return []
        if status:
            latest = latest.filter(F.col("status") == status)
        if since:
            latest = latest.filter(F.col("started_at") >= since)
        return self._collect(latest, limit)

    @staticmethod
    def _collect(df: Any, limit: Optional[int]) -> List[RunContext]:
        from pyspark.sql import functions as F

        df = df.orderBy(F.col("written_at").desc()).select("record")
        if limit is not None:
            df = df.limit(limit)
        return [_from_json(r["record"]) for r in df.collect()]

    # ------------------------------------------------------------------
    # The older LineageRecord methods (LineageBackend protocol)
    # ------------------------------------------------------------------

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

    def _say_local(self) -> None:
        if not self._warned_local:
            self._warned_local = True
            logger.warning(
                "No SparkSession when the Delta lineage backend was built: its LineageRecord "
                "methods use JSON files under %s, not a Delta table, and other jobs will not "
                "see them. Run records saved with save() go to Delta.",
                self._local_path,
            )

    def record(self, record: LineageRecord) -> None:
        """Persist a lineage record (non-blocking per Decision 1)."""
        self._get_worker().submit(record, run_id=record.run_id)

    def _record_sync(self, record: LineageRecord) -> None:
        """Synchronous write, called by the background worker thread."""
        if self._use_spark:
            from ubunye.lineage.storage import _record_to_ctx

            self._append(_record_to_ctx(record))
        else:
            self._write_local(record)

    def get_run(self, run_id: str) -> LineageRecord:
        """Load a single run record by ID."""
        if not self._use_spark:
            return self._get_run_local(run_id)
        from pyspark.sql import functions as F

        from ubunye.lineage.storage import _ctx_to_record

        latest = self._latest()
        rows = (
            []
            if latest is None
            else latest.filter(F.col("run_id") == run_id).select("record").limit(1).collect()
        )
        if not rows:
            raise LineageRecordNotFoundError(
                f"No lineage record found for run '{run_id}'.",
                context={"Run ID": run_id, "Table": self._table_or_path},
                hint="Check the run ID, or list runs with 'ubunye lineage list'.",
            )
        return _ctx_to_record(_from_json(rows[0]["record"]))

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
        """The older search, returning LineageRecords."""
        if not self._use_spark:
            return self._search_local(
                task=task,
                usecase=usecase,
                pipeline=pipeline,
                status=status,
                since=since,
                limit=limit,
            )
        from ubunye.lineage.storage import _ctx_to_record

        found = []
        for ctx in self.search(status=status, since=since):
            if task and ctx.task_name != task:
                continue
            if usecase and ctx.usecase != usecase:
                continue
            if pipeline and ctx.package != pipeline:
                continue
            found.append(_ctx_to_record(ctx))
            if len(found) >= limit:
                break
        return found

    def compact(self) -> None:
        """Optimize the underlying storage (Delta OPTIMIZE; nothing for local files)."""
        if not self._use_spark:
            return
        try:
            spark = self._session()
            loc = self._table_or_path
            spark.sql(f"OPTIMIZE delta.`{loc}`" if is_path(loc) else f"OPTIMIZE {loc}")
        except Exception as exc:
            logger.warning("Delta OPTIMIZE failed (non-fatal): %s", exc)

    # -- local JSON files (the older methods, without a SparkSession) ------------

    def _ensure_local_dir(self) -> Path:
        assert self._local_path is not None
        self._local_path.mkdir(parents=True, exist_ok=True)
        return self._local_path

    def _write_local(self, record: LineageRecord) -> None:
        self._say_local()
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
