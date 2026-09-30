"""Spark data-plane IO behind the Backend seam (issue #38).

The read/write *mechanism* for a Spark session, shared by ``SparkBackend`` and
``DatabricksBackend`` so the seam has one implementation, not two. Reads are a
``spark.read`` chain; writes delegate to :mod:`ubunye.adapters.spark.write_exec`,
so every existing Spark write (MERGE, dynamic partition overwrite, plain save)
behaves exactly as before — this file only moves the call site behind the seam.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Sequence

from ubunye.adapters.spark import write_exec
from ubunye.core.write_modes import ResolvedWriteMode


def read_frame(
    spark: Any,
    file_format: str,
    path: str,
    *,
    options: Optional[Dict[str, Any]] = None,
    schema: Optional[str] = None,
) -> Any:
    """Read a path into a Spark DataFrame (which satisfies ``DataFramePort``)."""
    reader = spark.read.format(file_format)
    if options:
        reader = reader.options(**options)
    if schema:
        reader = reader.schema(schema)
    return reader.load(path)


def frame_from_records(
    spark: Any, records: List[Dict[str, Any]], *, schema: Optional[str] = None
) -> Any:
    """A list of dicts as a Spark DataFrame: ``createDataFrame``, Spark's own rules.

    ``schema`` is a Spark DDL string. No records and no schema give a frame with
    no columns (``createDataFrame`` cannot infer from nothing).
    """
    if schema:
        return spark.createDataFrame(records, schema=schema)  # Spark parses the DDL
    if not records:
        from pyspark.sql.types import StructType

        return spark.createDataFrame([], StructType([]))
    return spark.createDataFrame(records)


def iter_records(df: Any) -> Iterator[Dict[str, Any]]:
    """Each row as a dict, one partition at a time (``toLocalIterator``).

    PySpark hands a ``timestamp`` back as a naive datetime in this process's
    local time. Those are made aware (UTC), as the pandas backend gives them, so
    a row means the same instant on both (F-051). ``timestamp_ntz`` stays naive.
    """
    stamps = _timestamp_type(df)
    for row in df.toLocalIterator():
        record = row.asDict(recursive=True)
        yield _aware(record, stamps) if stamps is not None else record


def _has_timestamp(t: Any) -> bool:
    name = type(t).__name__
    if name == "TimestampType":
        return True
    if name == "ArrayType":
        return _has_timestamp(t.elementType)
    if name == "MapType":
        return _has_timestamp(t.valueType)
    if name == "StructType":
        return any(_has_timestamp(f.dataType) for f in t.fields)
    return False


def _timestamp_type(df: Any) -> Any:
    """The frame's schema if it holds a ``timestamp`` anywhere, else None."""
    try:
        schema = df.schema
        return schema if _has_timestamp(schema) else None
    except Exception:  # noqa: BLE001 (a frame without a Spark schema: nothing to do)
        return None


def _aware(value: Any, t: Any) -> Any:
    import datetime as dt

    if value is None:
        return None
    name = type(t).__name__
    if name == "TimestampType" and isinstance(value, dt.datetime) and value.tzinfo is None:
        return value.astimezone(dt.timezone.utc)  # naive local time, as PySpark gives it
    if name == "StructType" and isinstance(value, dict):
        return {f.name: _aware(value.get(f.name), f.dataType) for f in t.fields}
    if name == "ArrayType" and isinstance(value, list):
        return [_aware(v, t.elementType) for v in value]
    if name == "MapType" and isinstance(value, dict):
        return {k: _aware(v, t.valueType) for k, v in value.items()}
    return value


def execute_write(
    spark: Any,
    df: Any,
    resolved: ResolvedWriteMode,
    *,
    connector: str,
    file_format: str,
    table: Optional[str] = None,
    path: Optional[str] = None,
    partition_by: Optional[Sequence[str]] = None,
    options: Optional[Dict[str, Any]] = None,
) -> None:
    """Execute a resolved write mode with Spark (unchanged behaviour)."""
    write_exec.apply(
        df,
        spark,
        resolved,
        connector=connector,
        file_format=file_format,
        table=table,
        path=path,
        partition_by=partition_by,
        options=options,
    )
