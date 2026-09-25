"""The SparkSession behind a backend, for connectors that cannot work without one.

hive, jdbc, delta, unity, binary and rest_api build on Spark. On a backend with
no session (the pandas backend) they call :func:`spark_of` first, so the run
stops before any work with a message naming the connector and the way out,
instead of ``AttributeError: ... has no attribute 'spark'`` from deep inside.
"""

from __future__ import annotations

from typing import Any, Type

from ubunye.core.errors import UbunyeError


def spark_of(backend: Any, connector: str, *, error: Type[UbunyeError]) -> Any:
    """``backend.spark``, or ``error`` if this backend has no SparkSession."""
    spark = getattr(backend, "spark", None)
    if spark is None:
        kind = type(backend).__name__
        raise error(
            f"The '{connector}' connector needs Spark, and this run uses {kind}.",
            context={"connector": connector, "Backend": kind},
            hint="Run with --backend spark, or read and write csv/parquet/json "
            "paths with format: s3 for a run without Spark.",
        )
    return spark
