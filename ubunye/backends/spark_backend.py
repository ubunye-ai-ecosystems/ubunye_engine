"""
Spark backend implementation for Ubunye.

- Lazily imports pyspark so users can install the package without Spark.
- Provides a simple lifecycle: start() / stop().
- Supports context-manager usage: `with SparkBackend(...) as backend: ...`
- Guards against double-starts and exposes the effective Spark conf for debugging.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

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

        builder = SparkSession.builder.appName(self._app_name)
        for k, v in self._conf.items():
            builder = builder.config(k, v)
        self._spark = builder.getOrCreate()

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
            raise RuntimeError(
                "Spark session not started. Call start() first or use context manager."
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
