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

from ubunye.core.errors import SourceReadError
from ubunye.core.interfaces import Reader


class UnityTableReader(Reader):
    """Read a DataFrame from a Unity Catalog table or SQL."""

    @classmethod
    def validate_config(cls, cfg):
        # A READER may be given a query. The writer below cannot -- and that difference
        # was impossible to express while the core validated both from one table.
        if cfg.get("table") or cfg.get("sql") or (cfg.get("db_name") and cfg.get("tbl_name")):
            return []
        return ["format 'unity' requires 'table', ('db_name' + 'tbl_name'), or 'sql'"]

    def read(self, cfg: Dict[str, Any], backend) -> Any:
        spark = backend.spark

        sql = cfg.get("sql")
        if sql:
            return spark.sql(sql)

        table = cfg.get("table")
        if not table:
            # allow split pieces
            catalog, schema, name = cfg.get("catalog"), cfg.get("schema"), cfg.get("tbl_name")
            if not (catalog and schema and name):
                raise SourceReadError(
                    "Unity reader requires 'table', catalog/schema/tbl_name, or 'sql'.",
                    context={"Format": "unity"},
                    hint="Set table: 'catalog.schema.table' or provide catalog, schema, and tbl_name.",
                )
            table = f"{catalog}.{schema}.{name}"

        # options are rarely used here, but allow pass-through
        # (Spark's .table() doesn't accept options; they are ignored)
        return spark.table(table)
