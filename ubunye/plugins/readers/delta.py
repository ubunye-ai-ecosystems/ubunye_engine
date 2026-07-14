"""Delta Lake reader plugin.

Reads a Delta table by path, by table name, or via a SQL query. ``format: delta``
was documented and accepted by the schema, but no reader was registered — those
configs failed with ``ReaderNotFoundError``.

Config keys:
  - path: "s3a://bucket/delta/claims/"   # path form
  - table: "main.fraud.claims"           # or table form (or db_name + tbl_name)
  - sql: "SELECT * FROM main.fraud.claims WHERE dt = '{{ dt }}'"
  - version_as_of: 12                    # optional time travel (by version)
  - timestamp_as_of: "2026-01-01"        # optional time travel (by timestamp)
  - options: {}                          # Spark reader options
"""

from __future__ import annotations

from typing import Any

from ubunye.core.errors import SourceReadError
from ubunye.core.interfaces import Reader


class DeltaReader(Reader):
    """Read a Spark DataFrame from a Delta table."""

    @classmethod
    def validate_config(cls, cfg):
        if cfg.get("path") or cfg.get("table") or (cfg.get("db_name") and cfg.get("tbl_name")):
            return []
        return ["format 'delta' requires 'path', 'table', or ('db_name' + 'tbl_name')"]

    def read(self, cfg: dict, backend) -> Any:
        spark = backend.spark

        sql = cfg.get("sql")
        if sql:
            return spark.sql(sql)

        table = cfg.get("table")
        if not table:
            db, tbl = cfg.get("db_name"), cfg.get("tbl_name")
            if db and tbl:
                table = f"{db}.{tbl}"
        path = cfg.get("path")

        if not (table or path):
            raise SourceReadError(
                "Delta reader requires 'path', 'table', ('db_name' + 'tbl_name'), or 'sql'.",
                context={"Format": "delta"},
                hint="Set path: 's3a://bucket/delta/...' or table: 'catalog.schema.table'.",
            )

        reader = spark.read.format("delta")

        options: dict = dict(cfg.get("options") or {})
        # Time travel — Spark expects these as reader options, not load() args.
        if cfg.get("version_as_of") is not None:
            options["versionAsOf"] = cfg["version_as_of"]
        if cfg.get("timestamp_as_of") is not None:
            options["timestampAsOf"] = cfg["timestamp_as_of"]

        for key, value in options.items():
            reader = reader.option(key, str(value))

        return reader.table(table) if table else reader.load(path)
