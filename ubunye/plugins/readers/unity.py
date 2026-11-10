"""
Unity Catalog table reader.

Supports:
- Qualified table reads: catalog.schema.table
- Custom SQL reads (Spark SQL), useful for UC governance with row/col ACLs.

Config keys:
  - table: "main.fraud.claims"            # preferred
    (or) catalog: "main", schema: "fraud", tbl_name: "claims"
  - sql: "SELECT * FROM main.fraud.claims WHERE ds='2025-01-01'"  # optional
  - options: {}  # extra DataFrameReader options if needed

Returns a Spark DataFrame.
"""

from __future__ import annotations

from typing import Any, Dict

from ubunye.adapters.spark.session import spark_of
from ubunye.core.capabilities import SPARK
from ubunye.core.errors import SourceReadError
from ubunye.core.interfaces import Reader


def _parts(cfg: Dict[str, Any]) -> str:
    """``catalog.schema.tbl_name`` when all three are set, else ``""``."""
    parts = (cfg.get("catalog"), cfg.get("schema"), cfg.get("tbl_name"))
    return ".".join(str(p) for p in parts) if all(parts) else ""


class UnityTableReader(Reader):
    """Read a DataFrame from a Unity Catalog table or SQL."""

    # The settings this connector reads (typos in any other key fail validation).
    # `db_name` is not read; it is listed so validate_config can say what to use.
    CONFIG_KEYS = frozenset({"catalog", "schema", "table", "db_name", "tbl_name", "sql"})

    # Needs a live SparkSession; checked before a run (ADR 002).
    REQUIRES = frozenset({SPARK})

    @classmethod
    def validate_config(cls, cfg):
        # A READER may be given a query. The writer below cannot -- and that difference
        # was impossible to express while the core validated both from one table.
        use = "set table: 'catalog.schema.table', or catalog + schema + tbl_name, or sql"
        if cfg.get("db_name"):
            return [f"format 'unity' does not read 'db_name' (a Hive key); {use}"]
        if cfg.get("table") or cfg.get("sql") or _parts(cfg):
            return []
        return [f"format 'unity' needs a table to read; {use}"]

    def read(self, cfg: Dict[str, Any], backend) -> Any:
        spark = spark_of(backend, "unity", error=SourceReadError)

        sql = cfg.get("sql")
        if sql:
            return spark.sql(sql)

        table = cfg.get("table") or _parts(cfg)
        if not table:
            raise SourceReadError(
                "Unity reader requires 'table', catalog/schema/tbl_name, or 'sql'.",
                context={"Format": "unity"},
                hint="Set table: 'catalog.schema.table' or provide catalog, schema, and tbl_name.",
            )

        # options are rarely used here, but allow pass-through
        # (Spark's .table() doesn't accept options; they are ignored)
        return spark.table(table)
