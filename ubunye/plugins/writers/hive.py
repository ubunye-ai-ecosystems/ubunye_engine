"""Hive writer plugin.

Writes a DataFrame to a Hive Metastore table via ``saveAsTable``. The reader has
existed since the beginning; the writer was documented but never registered, so
``format: hive`` outputs failed with ``WriterNotFoundError``.

Config keys:
  - db_name + tbl_name: "fraud" + "claims"   # or table: "fraud.claims"
  - mode: append|overwrite|errorifexists|ignore|merge|overwrite_partitions
          (default: append)
  - file_format: parquet|orc|delta (default: parquet)
  - partitionBy: ["dt"]                      # optional
  - merge_keys: ["id"]                       # mode: merge, needs file_format: delta
  - options:                                 # Spark writer options
      compression: "snappy"
"""

from __future__ import annotations

from typing import Any, Dict

from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError
from ubunye.core.interfaces import Writer

SUPPORTED_MODES = write_modes.ALL_MODES
DEFAULT_MODE = "append"


def _qualify(cfg: Dict[str, Any]) -> str:
    table = cfg.get("table")
    if table:
        return table
    db, tbl = cfg.get("db_name"), cfg.get("tbl_name")
    if not (db and tbl):
        raise SinkWriteError(
            "Hive writer requires 'table' or both 'db_name' and 'tbl_name'.",
            context={"Format": "hive"},
            hint="Set db_name and tbl_name (or table: 'db.table') in your output config.",
        )
    return f"{db}.{tbl}"


class HiveWriter(Writer):
    """Write a Spark DataFrame to a Hive table."""

    def write(self, df: Any, cfg: dict, backend) -> None:
        full_name = _qualify(cfg)
        fmt = (cfg.get("file_format") or "parquet").lower()

        resolved = write_modes.resolve(
            cfg,
            connector="hive",
            supported=SUPPORTED_MODES,
            default=DEFAULT_MODE,
            file_format=fmt,
        )

        write_modes.apply(
            df,
            backend.spark,
            resolved,
            connector="hive",
            file_format=fmt,
            table=full_name,
            partition_by=cfg.get("partitionBy"),
            options=cfg.get("options"),
        )
