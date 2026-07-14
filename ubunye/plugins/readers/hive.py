"""Hive reader plugin.

Reads from a Hive table or runs a SQL query using Spark.
"""

from __future__ import annotations

from typing import Any

from ubunye.core.errors import SourceReadError
from ubunye.core.interfaces import Reader


class HiveReader(Reader):
    """Read a Spark DataFrame from Hive using `db_name.tbl_name` or a custom SQL."""

    @classmethod
    def validate_config(cls, cfg):
        has_table = cfg.get("db_name") and cfg.get("tbl_name")
        if not has_table and not cfg.get("sql"):
            return ["format 'hive' requires either ('db_name' + 'tbl_name') or 'sql'"]
        return []

    def read(self, cfg: dict, backend) -> Any:
        spark = backend.spark
        sql = cfg.get("sql")
        if sql:
            return spark.sql(sql)
        db = cfg.get("db_name")
        tbl = cfg.get("tbl_name")
        if not (db and tbl):
            raise SourceReadError(
                "Hive reader requires either 'sql' or both 'db_name' and 'tbl_name'.",
                context={"Format": "hive"},
                hint="Set sql: 'SELECT ...' or set both db_name and tbl_name in your input config.",
            )
        return spark.table(f"{db}.{tbl}")
