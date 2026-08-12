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
from ubunye.core.interfaces import Backend


class PandasBackend(Backend):
    """Execute a Ubunye task with pandas, no Spark and no JVM."""

    def __init__(self, app_name: str = "ubunye") -> None:
        self._app_name = app_name

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

    def read_frame(
        self,
        file_format: str,
        path: str,
        *,
        options: Optional[Dict[str, Any]] = None,
        schema: Optional[str] = None,
    ) -> Any:
        return pandas_io.read_frame(file_format, path, options=options, schema=schema)

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
        )
