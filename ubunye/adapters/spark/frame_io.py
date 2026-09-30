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
    """Each row as a dict, one partition at a time (``toLocalIterator``)."""
    for row in df.toLocalIterator():
        yield row.asDict(recursive=True)


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
