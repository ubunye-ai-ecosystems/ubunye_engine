"""
Spark backend implementation for Ubunye.

- Lazily imports pyspark so users can install the package without Spark.
- Provides a simple lifecycle: start() / stop().
- Supports context-manager usage: `with SparkBackend(...) as backend: ...`
- Guards against double-starts and exposes the effective Spark conf for debugging.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

from ubunye.core.errors import SparkSessionError
from ubunye.core.interfaces import Backend

if TYPE_CHECKING:  # only for type checkers; no runtime dependency on pyspark
    from pyspark.sql import SparkSession


class SparkBackend(Backend):
    """Creates and manages a SparkSession for a Ubunye run.

    Parameters
    ----------
    app_name : str
        Spark application name (appears in Spark UI/history server).
    conf : Optional[Dict[str, str]]
        Spark configuration key-values (e.g., {"spark.master": "yarn"}).

    Notes
    -----
    - pyspark is imported lazily inside `start()` to keep installation lightweight.
    - `spark` property is only valid after `start()` (or inside the context manager).
    """

    def __init__(self, app_name: str = "ubunye", conf: Optional[Dict[str, str]] = None) -> None:
        self._spark: Optional["SparkSession"] = None
        self._app_name = app_name
        self._conf = dict(conf or {})

    # -------------------------
    # Lifecycle
    # -------------------------
    def start(self) -> None:
        """Create (or reuse) a SparkSession.

        Safe to call multiple times; a second call is a no-op if a session already exists.
        """
        if self._spark is not None:
            return  # already started

        # Lazy import to avoid hard dependency during pip install
        from pyspark.sql import SparkSession  # type: ignore

        self._check_master_not_hijacked()

        builder = SparkSession.builder.appName(self._app_name)
        for k, v in self._conf.items():
            builder = builder.config(k, v)
        self._spark = builder.getOrCreate()

    def _check_master_not_hijacked(self) -> None:
        """Refuse to let a config override a master the platform already chose.

        This is the most expensive silent failure the engine had.

        Under ``spark-submit`` — which is how AWS EMR Serverless and GCP Dataproc
        Serverless start every job — the platform puts its own ``spark.master`` into the
        default SparkConf. If a task's ``ENGINE.spark_conf`` also sets ``spark.master``,
        the builder wins, and the job runs **entirely in the driver**: it ignores every
        executor, finishes, reports success, and bills you for the whole cluster it
        never touched.

        Nothing warns you. The output is correct — there is just far less of it per
        minute than there should be, forever.

        Which master to use is a fact about *where the job landed*, not about *what the
        job does*. It belongs to whoever launched the session. So if the platform has
        already said, and the config disagrees, that is a bug in the config, and it is
        one worth stopping for.
        """
        requested = self._conf.get("spark.master")
        if not requested:
            return

        existing = self._platform_master()
        if existing and existing != requested:
            raise SparkSessionError(
                "ENGINE.spark_conf sets 'spark.master', but the platform already chose one.",
                context={"Platform's master": existing, "Config wants": requested},
                hint="Remove 'spark.master' from ENGINE.spark_conf. Under spark-submit — "
                "which is how EMR Serverless and Dataproc run every job — overriding it "
                "makes the whole job run inside the driver: it ignores every executor, "
                "succeeds, and bills you for a cluster it never used. The master belongs "
                "to whoever launched the session, not to the task.",
            )

    @staticmethod
    def _platform_master() -> Optional[str]:
        """Whatever master the launcher already put in the default SparkConf.

        `spark-submit` populates it; a bare `python foo.py` does not. Its own method so
        the guard above can be tested without pyspark installed — the unit tier does not
        install Spark, and a test that has to be skipped is a test that does not run.
        """
        try:
            from pyspark import SparkConf  # type: ignore

            return SparkConf().get("spark.master", None)
        except Exception:  # noqa: BLE001 — no pyspark, or no defaults; nothing to clash with
            return None

    def stop(self) -> None:
        """Stop the SparkSession if running."""
        if self._spark is not None:
            try:
                self._spark.stop()
            finally:
                self._spark = None

    # Context manager support
    def __enter__(self) -> "SparkBackend":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def __del__(self) -> None:
        # Best-effort cleanup; not guaranteed to run in all interpreter shutdown scenarios.
        try:
            self.stop()
        except Exception:
            # Avoid noisy destructor exceptions at process teardown.
            pass

    # -------------------------
    # Properties
    # -------------------------
    @property
    def spark(self) -> "SparkSession":
        """Return the active SparkSession (after `start()`).

        Raises
        ------
        RuntimeError
            If `start()` has not been called.
        """
        if self._spark is None:
            raise SparkSessionError(
                "Spark session not started.",
                context={"Backend": "SparkBackend"},
                hint="Call start() first or use the backend as a context manager.",
            )
        return self._spark

    @property
    def is_spark(self) -> bool:
        """Whether this backend is Spark-based (always True here)."""
        return True

    @property
    def app_name(self) -> str:
        """Configured Spark app name."""
        return self._app_name

    @property
    def conf_input(self) -> Dict[str, str]:
        """The configuration dict passed into this backend at construction time."""
        return dict(self._conf)

    @property
    def conf_effective(self) -> Dict[str, str]:
        """The *effective* Spark configuration currently applied to the session.

        Returns an empty dict if the session hasn't been started yet.
        """
        if self._spark is None:
            return {}
        sc = self._spark.sparkContext
        # Convert list[tuple[str,str]] to dict[str,str]
        return {k: v for (k, v) in sc.getConf().getAll()}
