"""
Unity Catalog table writer (Delta by default).

Writes a DataFrame as a managed UC table via saveAsTable, with options for
mode, partitioning, schema overwrite, table properties, and (optional) post-write
OPTIMIZE / ZORDER / VACUUM on Databricks.

Config keys:
  - table: "main.fraud.claims_curated"      # or catalog/schema/tbl_name
  - mode: append|overwrite|errorifexists|ignore|merge|overwrite_partitions
          (default: append)
  - merge_keys: ["policy_id", "ds"]         # required for mode: merge
  - replace_where: "ds = '2026-01-01'"      # optional Delta predicate for
                                            # mode: overwrite_partitions
  - partitionBy: ["ds", "region"]           # optional
  - file_format: delta|parquet (default: delta)  # UC+Delta is the typical choice
                                                  # Note: the top-level ``format`` key is
                                                  # the Ubunye plugin selector and is always
                                                  # "unity" here - do not confuse the two.
  - options:                                 # writer options (merged last)
      overwriteSchema: "true"                # Spark expects strings
      mergeSchema: "true"
      comment: "Curated fraud claims"
      tblproperties:
        quality: "bronze"                    # will be applied after creation
  - optimize:
      enabled: true                          # Databricks only
      zorder_by: ["policy_id", "ds"]
  - vacuum:
      hours: 168                             # Databricks only; default retention if omitted
"""

from __future__ import annotations

from typing import Any, Dict, List

from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError
from ubunye.core.interfaces import Writer

SUPPORTED_MODES = write_modes.ALL_MODES


def _qualify(cfg: Dict[str, Any]) -> str:
    table = cfg.get("table")
    if table:
        return table
    catalog, schema, name = cfg.get("catalog"), cfg.get("schema"), cfg.get("tbl_name")
    if not (catalog and schema and name):
        raise SinkWriteError(
            "Unity writer requires 'table' or catalog/schema/tbl_name.",
            context={"Format": "unity"},
            hint="Set table: 'catalog.schema.table' or provide catalog, schema, and tbl_name.",
        )
    return f"{catalog}.{schema}.{name}"


def _is_databricks(spark) -> bool:
    # Heuristic: presence of Databricks conf keys
    try:
        return any("databricks" in k for k, _ in spark.sparkContext.getConf().getAll())
    except Exception:
        return False


class UnityTableWriter(Writer):
    """Write DataFrame to a Unity Catalog table (Delta by default)."""

    def write(self, df: Any, cfg: Dict[str, Any], backend) -> None:
        spark = backend.spark
        full_name = _qualify(cfg)

        # The top-level cfg["format"] is the Ubunye plugin dispatch key
        # (always "unity" by the time we get here). The underlying Spark
        # source format lives in cfg["file_format"], matching the s3 writer.
        fmt = (cfg.get("file_format") or "delta").lower()

        resolved = write_modes.resolve(
            cfg,
            connector="unity",
            supported=SUPPORTED_MODES,
            default="append",
            file_format=fmt,
        )

        partition_by: List[str] = list(cfg.get("partitionBy", []) or [])
        options: Dict[str, Any] = dict(cfg.get("options", {}) or {})
        comment: str | None = options.pop("comment", None)  # we'll set via SQL after creation
        tblprops: Dict[str, str] = dict(options.pop("tblproperties", {}) or {})

        write_modes.apply(
            df,
            spark,
            resolved,
            connector="unity",
            file_format=fmt,
            table=full_name,  # saveAsTable — UC managed table
            partition_by=partition_by,
            options=options,
        )

        # Post-create table comment / properties (Spark doesn't support via DataFrameWriter directly)
        if comment:
            spark.sql(f"COMMENT ON TABLE {full_name} IS {repr(comment)}")
        if tblprops:
            # build properties string: 'key1'='v1', 'key2'='v2'
            props_sql = ", ".join([f"'{k}'='{v}'" for k, v in tblprops.items()])
            spark.sql(f"ALTER TABLE {full_name} SET TBLPROPERTIES ({props_sql})")

        # Optional Databricks optimizations
        if _is_databricks(spark):
            opt_cfg = cfg.get("optimize") or {}
            if opt_cfg.get("enabled"):
                zcols = opt_cfg.get("zorder_by") or []
                if zcols:
                    cols = ", ".join(zcols)
                    spark.sql(f"OPTIMIZE {full_name} ZORDER BY ({cols})")
                else:
                    spark.sql(f"OPTIMIZE {full_name}")

            vac_cfg = cfg.get("vacuum") or {}
            if "hours" in vac_cfg:
                hours = int(vac_cfg["hours"])
                spark.sql(f"VACUUM {full_name} RETAIN {hours} HOURS")
