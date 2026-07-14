"""Delta Lake writer plugin.

Writes a DataFrame to a Delta table, addressed either by path or by table name.
``format: delta`` was declared in the config schema and documented long before a
writer existed for it — configs using it died with ``WriterNotFoundError``. This
is that writer.

Use this connector when the sink *is* a Delta table. Use ``s3`` with
``file_format: delta`` when you think of the sink as a path that happens to hold
Delta files — they are equivalent, and both support all six write modes.

Config keys:
  - path: "s3a://bucket/delta/claims/"     # path form
  - table: "main.fraud.claims"             # or table form (or db_name + tbl_name)
  - mode: append|overwrite|errorifexists|ignore|merge|overwrite_partitions
          (default: append)
  - partitionBy: ["dt"]                    # optional
  - merge_keys: ["id", "dt"]               # required for mode: merge
  - replace_where: "dt = '2026-01-01'"     # optional predicate for
                                           # mode: overwrite_partitions
  - options:                               # Spark writer options
      mergeSchema: "true"
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError
from ubunye.core.interfaces import Writer

SUPPORTED_MODES = write_modes.ALL_MODES
DEFAULT_MODE = "append"
FILE_FORMAT = "delta"


def _target(cfg: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Resolve the sink to exactly one of (table, path)."""
    table = cfg.get("table")
    if not table:
        db, tbl = cfg.get("db_name"), cfg.get("tbl_name")
        if db and tbl:
            table = f"{db}.{tbl}"

    path = cfg.get("path")
    if not (table or path):
        raise SinkWriteError(
            "Delta writer requires 'path' or 'table'.",
            context={"Format": "delta"},
            hint="Set path: 's3a://bucket/delta/...' or table: 'catalog.schema.table'.",
        )
    # A table name wins: writing to a registered table keeps the metastore in sync.
    return (table, None) if table else (None, path)


class DeltaWriter(Writer):
    """Write a Spark DataFrame to a Delta table, by path or by name."""

    SUPPORTS_MERGE = True
    MERGE_FILE_FORMATS = frozenset({"delta"})

    @classmethod
    def validate_config(cls, cfg):
        if cfg.get("path") or cfg.get("table") or (cfg.get("db_name") and cfg.get("tbl_name")):
            return []
        return ["format 'delta' requires 'path', 'table', or ('db_name' + 'tbl_name')"]

    def write(self, df: Any, cfg: dict, backend) -> None:
        table, path = _target(cfg)

        resolved = write_modes.resolve(
            cfg,
            connector="delta",
            supported=SUPPORTED_MODES,
            merge_formats=self.MERGE_FILE_FORMATS,
            default=DEFAULT_MODE,
            file_format=FILE_FORMAT,
        )

        write_modes.apply(
            df,
            backend.spark,
            resolved,
            connector="delta",
            file_format=FILE_FORMAT,
            table=table,
            path=path,
            partition_by=cfg.get("partitionBy"),
            options=cfg.get("options"),
        )
