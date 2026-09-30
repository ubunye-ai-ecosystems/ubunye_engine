"""
Spark backend implementation for Ubunye.

- Lazily imports pyspark so users can install the package without Spark.
- Provides a simple lifecycle: start() / stop().
- Supports context-manager usage: `with SparkBackend(...) as backend: ...`
- Guards against double-starts and exposes the effective Spark conf for debugging.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Sequence

from ubunye.adapters.spark import frame_io
from ubunye.core.capabilities import (
    CATALOG,
    PARTITIONED_WRITES,
    PATH_IO,
    RECORDS,
    REMOTE_PATHS,
    SPARK,
    Capabilities,
)
from ubunye.core.errors import SparkSessionError
from ubunye.core.interfaces import Backend

#: What any Spark backend can do. File formats and write modes are "any": Spark
#: knows its own sources, and each connector declares the modes it supports.
SPARK_CAPABILITIES = Capabilities(
    features=frozenset({SPARK, PATH_IO, RECORDS, PARTITIONED_WRITES, REMOTE_PATHS, CATALOG}),
    distributed=True,
    lazy=True,
    needs_jvm=True,
)

#: The session time zone every backend reads and cuts time in, unless a task sets
#: ``spark.sql.session.timeZone`` (the pandas backend reads the same key). ADR 007.
TIME_ZONE_KEY = "spark.sql.session.timeZone"
DEFAULT_TIME_ZONE = "UTC"

if TYPE_CHECKING:
    from ubunye.core.ports import DataFramePort
    from ubunye.core.write_modes import ResolvedWriteMode

if TYPE_CHECKING:  # only for type checkers; no runtime dependency on pyspark
    from pyspark.sql import SparkSession


def _session_time_zone(session: Any, conf: Dict[str, str]) -> Optional[str]:
    """The zone a Spark session works in, or will: its own setting once it runs."""
    try:
        if session is not None:
            return str(session.conf.get(TIME_ZONE_KEY))
        if TIME_ZONE_KEY in conf:
            return str(conf[TIME_ZONE_KEY])
        from pyspark.sql import SparkSession

        running = SparkSession.getActiveSession()
        if running is not None:
            return str(running.conf.get(TIME_ZONE_KEY))
        return DEFAULT_TIME_ZONE
    except Exception:  # noqa: BLE001 (no pyspark, no JVM: unknown, never a failed run)
        return None


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

    name = "spark"
    REQUIRES_PACKAGES = ("pyspark",)
    CAPABILITIES = SPARK_CAPABILITIES

    def __init__(self, app_name: str = "ubunye", conf: Optional[Dict[str, str]] = None) -> None:
        self._spark: Optional["SparkSession"] = None
        # Whether start() created the session. One that was already running
        # belongs to whoever started it, and is never stopped here.
        self._owns_session = False
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

        # Check the config BEFORE importing Spark. A config that would hijack the
        # platform's master is wrong whether or not pyspark is installed, and there is
        # no sense loading a JVM to find that out.
        self._check_master_not_hijacked()

        # Lazy import to avoid hard dependency during pip install
        from pyspark.sql import SparkSession

        running = SparkSession.getActiveSession()
        builder = SparkSession.builder.appName(self._app_name)
        for k, v in self._conf.items():
            builder = builder.config(k, v)
        if running is None and TIME_ZONE_KEY not in self._conf:
            # A session this backend creates reads and cuts time in UTC, as the pandas
            # backend does, unless the task says otherwise (ADR 007). Left to the
            # JVM, a laptop in Johannesburg truncated a timestamp to its own midnight
            # and disagreed with pandas, and with the same task on a UTC cloud.
            # A session someone else started is never changed.
            builder = builder.config(TIME_ZONE_KEY, DEFAULT_TIME_ZONE)
        self._spark = builder.getOrCreate()
        self._owns_session = running is None

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
            from pyspark import SparkConf

            return SparkConf().get("spark.master", None)
        except Exception:  # noqa: BLE001 — no pyspark, or no defaults; nothing to clash with
            return None

    def stop(self) -> None:
        """Stop the SparkSession if this backend started it.

        A session that was already running when :meth:`start` attached to it (the
        user's own, or a notebook's) is left running.
        """
        if self._spark is not None:
            try:
                if self._owns_session:
                    self._spark.stop()
            finally:
                self._spark = None
                self._owns_session = False

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
    def timezone(self) -> Optional[str]:
        """The session time zone this backend works in, for the run record (ADR 007).

        Known before the session starts: the task's setting, else a running
        session's (which this backend would reuse, unchanged), else UTC.
        """
        return _session_time_zone(self._spark, self._conf)

    # -------------------------
    # Data-plane IO seam (#38)
    # -------------------------
    def read_frame(
        self,
        file_format: str,
        path: str,
        *,
        options: Optional[Dict[str, Any]] = None,
        schema: Optional[str] = None,
    ) -> "DataFramePort":
        return frame_io.read_frame(self.spark, file_format, path, options=options, schema=schema)

    def frame_from_records(
        self, records: List[Dict[str, Any]], *, schema: Optional[str] = None
    ) -> Any:
        return frame_io.frame_from_records(self.spark, records, schema=schema)

    def iter_records(self, frame: Any) -> Iterator[Dict[str, Any]]:
        return frame_io.iter_records(frame)

    def execute_write(
        self,
        df: "DataFramePort",
        resolved: "ResolvedWriteMode",
        *,
        connector: str,
        file_format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        partition_by: Optional[Sequence[str]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        frame_io.execute_write(
            self.spark,
            df,
            resolved,
            connector=connector,
            file_format=file_format,
            table=table,
            path=path,
            partition_by=partition_by,
            options=options,
        )

    def materialise(self, frame: Any) -> Optional[Any]:
        """Compute an output once for its checks, write and hash (ADR 009)."""
        from ubunye.adapters.spark import materialise

        return materialise.materialise(frame)

    def release(self, frame: Any) -> None:
        from ubunye.adapters.spark import materialise

        materialise.release(frame)

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
