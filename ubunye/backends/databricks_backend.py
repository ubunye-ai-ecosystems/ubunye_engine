"""Databricks backend — reuses the active SparkSession instead of creating one.

On Databricks, a SparkSession is already running when a notebook or job starts.
This backend wraps that session so Ubunye readers/writers/transforms work
without the overhead of creating (and accidentally stopping) a second session.

Usage
-----
    from ubunye.backends.databricks_backend import DatabricksBackend

    backend = DatabricksBackend()  # grabs the active session
    backend.start()                # no-op, session already running
    backend.stop()                 # no-op, we don't own the session
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

from ubunye.core.interfaces import Backend

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class DatabricksBackend(Backend):
    """Backend that reuses the existing Databricks SparkSession.

    Parameters
    ----------
    spark : Optional[SparkSession]
        An explicit session to wrap. If *None* (the default), the active
        session is retrieved via ``SparkSession.getActiveSession()``.
    """

    def __init__(self, spark: Optional["SparkSession"] = None) -> None:
        self._spark: Optional["SparkSession"] = spark

    def start(self) -> None:
        """Attach to the active SparkSession if one wasn't provided."""
        if self._spark is not None:
            return

        from pyspark.sql import SparkSession  # type: ignore

        active = SparkSession.getActiveSession()
        if active is None:
            raise RuntimeError(
                "No active SparkSession found. "
                "DatabricksBackend expects Databricks (or another environment) "
                "to have an active session. Use SparkBackend if you need to create one."
            )
        self._spark = active

    def stop(self) -> None:
        """No-op — we don't own the session, so we never stop it."""
        pass

    def __enter__(self) -> "DatabricksBackend":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    @property
    def spark(self) -> "SparkSession":
        if self._spark is None:
            raise RuntimeError(
                "Spark session not attached. Call start() first or use context manager."
            )
        return self._spark

    @property
    def is_spark(self) -> bool:
        return True

    @property
    def app_name(self) -> str:
        if self._spark is not None:
            return self._spark.sparkContext.appName
        return "databricks"

    @property
    def conf_effective(self) -> Dict[str, str]:
        if self._spark is None:
            return {}
        return {k: v for k, v in self._spark.sparkContext.getConf().getAll()}
