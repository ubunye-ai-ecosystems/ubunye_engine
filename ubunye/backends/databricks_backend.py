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

from ubunye.core.errors import SparkSessionError
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

    def __init__(
        self,
        spark: Optional["SparkSession"] = None,
        conf: Optional[Dict[str, str]] = None,
    ) -> None:
        self._spark: Optional["SparkSession"] = spark
        self._conf: Dict[str, str] = dict(conf or {})

    def start(self) -> None:
        """Attach to the active SparkSession, and apply ``ENGINE.spark_conf`` to it."""
        if self._spark is None:
            from pyspark.sql import SparkSession  # type: ignore

            active = SparkSession.getActiveSession()
            if active is None:
                raise SparkSessionError(
                    "No active SparkSession found.",
                    context={"Backend": self.__class__.__name__},
                    hint=f"{self.__class__.__name__} expects an active session. "
                    "Use SparkBackend if you need to create one.",
                )
            self._spark = active

        self._apply_conf()

    def _apply_conf(self) -> None:
        """Apply the config's ``spark_conf`` to a session somebody else created.

        This used to be skipped entirely. The backend took no ``conf`` at all, so every
        ``ENGINE.spark_conf`` and every ``ENGINE.profiles`` block was **silently
        discarded** the moment a session already existed — which is always, on
        Databricks, and in any notebook, Glue job or pytest run. The config said one
        thing, the runtime did another, and nothing anywhere said so.

        Now it is applied. Some keys cannot be: Spark divides configuration into
        runtime keys (``spark.sql.shuffle.partitions``) and **static** ones
        (``spark.master``, ``spark.sql.extensions``, ``spark.driver.memory``) that are
        fixed when the JVM starts and can never be changed afterwards.

        A static key in a config is therefore a **request the engine cannot honour**,
        and it raises. Ignoring it quietly is what the old behaviour did, and it is how
        a pipeline ends up believing it configured something it did not. Two of those
        keys are actively dangerous: ``spark.master`` would mean the pipeline thinks it
        chose the cluster, and ``spark.sql.extensions`` would mean it thinks it enabled
        Delta.

        Static configuration belongs to whoever starts the session — the platform, the
        cluster policy, ``spark-submit`` — and not to the task.
        """
        if not self._conf or self._spark is None:
            return

        rejected: Dict[str, str] = {}
        for key, value in self._conf.items():
            try:
                self._spark.conf.set(key, str(value))
            except Exception as exc:  # noqa: BLE001 — Spark raises AnalysisException here
                rejected[key] = str(exc).splitlines()[0][:120]

        if rejected:
            raise SparkSessionError(
                f"{len(rejected)} spark_conf key(s) cannot be set on a session that "
                "already exists.",
                context={
                    "Backend": self.__class__.__name__,
                    **{f"Rejected: {k}": v for k, v in rejected.items()},
                },
                hint="These are STATIC Spark settings — fixed when the JVM starts, and "
                "not changeable afterwards. On Databricks, EMR, Dataproc or under "
                "spark-submit the session is created by the platform, so they must be "
                "set there (a cluster policy, a job conf, --conf) and removed from "
                "ENGINE.spark_conf. They were silently ignored before 0.4.0, which is "
                "why a pipeline could believe it had chosen a master, or enabled Delta, "
                "and be wrong.",
            )

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
            raise SparkSessionError(
                "Spark session not attached.",
                context={"Backend": "DatabricksBackend"},
                hint="Call start() first or use the backend as a context manager.",
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


# The class contains no Databricks code whatsoever: it attaches to a session somebody
# else created and declines to stop it. That is equally true of AWS Glue, of a
# spark-submit entry point on EMR or Dataproc, of a plain notebook, and of pytest.
#
# The name has misled people into thinking the engine has a Databricks dependency
# here. It does not. `DatabricksBackend` stays as an alias so nothing breaks.
AmbientSessionBackend = DatabricksBackend
