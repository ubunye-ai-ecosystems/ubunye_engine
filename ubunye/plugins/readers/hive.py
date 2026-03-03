"""Hive reader plugin.

Reads from a Hive table or runs a SQL query using Spark.
"""
from __future__ import annotations

from typing import Any

from ubunye.core.interfaces import Reader


class HiveReader(Reader):
    """Read a Spark DataFrame from Hive using `db_name.tbl_name` or a custom SQL."""

    def read(self, cfg: dict, backend) -> Any:
        spark = backend.spark
        sql = cfg.get("sql")
        if sql:
            return spark.sql(sql)
        db = cfg.get("db_name")
        tbl = cfg.get("tbl_name")
        if not (db and tbl):
            raise ValueError("Provide either 'sql' or both 'db_name' and 'tbl_name'")
        return spark.table(f"{db}.{tbl}")
