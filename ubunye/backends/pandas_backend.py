"""Pandas backend — run a task on a laptop with nothing but Python.

The second real adapter behind the ``Backend`` port. A port with one adapter
has never been tested as a port; this is the proof (issue #38). ``start`` and
``stop`` are no-ops (there is no session and no JVM to manage), ``is_spark`` is
False, and there is deliberately **no** ``spark`` property — code that reaches
for one on this backend is asking for the thing the pandas backend exists to do
without.

Reads and writes go through :mod:`ubunye.adapters.pandas_io`, which handles the
generic path-based formats (csv, parquet, json). Lakehouse formats and managed
tables belong to the Spark backend, and their connectors say so, so a config
that asks for one under ``--backend pandas`` fails with a clear message rather
than a stray ``AttributeError``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from ubunye.adapters import pandas_io
from ubunye.core.capabilities import PATH_IO, Capabilities
from ubunye.core.interfaces import Backend
from ubunye.core.write_modes import NATIVE_SAVE_MODES


class PandasBackend(Backend):
    """Execute a Ubunye task with pandas, no Spark and no JVM."""

    name = "pandas"
    #: Local csv / json / parquet paths and the native save modes. No SparkSession,
    #: no partitioned folders, no cloud paths, no lakehouse modes: a task that
    #: needs any of those is refused before it starts.
    CAPABILITIES = Capabilities(
        features=frozenset({PATH_IO}),
        file_formats=pandas_io.SUPPORTED_FORMATS,
        write_modes=NATIVE_SAVE_MODES,
    )

    def __init__(
        self,
        app_name: str = "ubunye",
        *,
        timezone: Optional[str] = None,
        conf: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._app_name = app_name
        # Timestamp text is read as an instant in this zone, as Spark reads it in
        # spark.sql.session.timeZone. The same task conf sets both, so the two
        # backends agree; with neither set this is UTC, on every machine.
        self._timezone = timezone or (conf or {}).get("spark.sql.session.timeZone") or "UTC"

    def start(self) -> None:
        """No session to create."""

    def stop(self) -> None:
        """No session to stop."""

    def __enter__(self) -> "PandasBackend":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    @property
    def is_spark(self) -> bool:
        return False

    @property
    def app_name(self) -> str:
        return self._app_name

    @property
    def timezone(self) -> str:
        """The zone timestamp text is read in (``UTC`` unless set)."""
        return self._timezone

    def read_frame(
        self,
        file_format: str,
        path: str,
        *,
        options: Optional[Dict[str, Any]] = None,
        schema: Optional[str] = None,
    ) -> Any:
        return pandas_io.read_frame(
            file_format, path, options=options, schema=schema, timezone=self._timezone
        )

    def execute_write(
        self,
        df: Any,
        resolved: Any,
        *,
        connector: str,
        file_format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        partition_by: Optional[Sequence[str]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        pandas_io.execute_write(
            df,
            resolved,
            connector=connector,
            file_format=file_format,
            table=table,
            path=path,
            partition_by=partition_by,
            options=options,
            timezone=self._timezone,
        )
