"""
Unity Catalog table writer (Delta by default).

Writes a DataFrame as a managed UC table via saveAsTable, with options for
mode, partitioning, schema overwrite, table properties, and (optional) post-write
OPTIMIZE / ZORDER / VACUUM on Databricks.

Config keys:
  - table: "main.fraud.claims_curated"      # or catalog/schema/tbl_name
  - mode: append|overwrite|errorifexists|ignore (default: append)
  - partitionBy: ["ds", "region"]           # optional
  - format: delta|parquet (default: delta)  # UC+Delta is the typical choice
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

from ubunye.core.interfaces import Writer


def _qualify(cfg: Dict[str, Any]) -> str:
    table = cfg.get("table")
    if table:
        return table
    catalog, schema, name = cfg.get("catalog"), cfg.get("schema"), cfg.get("tbl_name")
    if not (catalog and schema and name):
        raise ValueError("Provide 'table' (catalog.schema.table) or catalog/schema/tbl_name.")
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

        mode = (cfg.get("mode") or "append").lower()
        fmt = (cfg.get("format") or "delta").lower()  # UC best practice is Delta

        partition_by: List[str] = list(cfg.get("partitionBy", []) or [])
        options: Dict[str, Any] = dict(cfg.get("options", {}) or {})
        comment: str | None = options.pop("comment", None)  # we'll set via SQL after creation
        tblprops: Dict[str, str] = dict(options.pop("tblproperties", {}) or {})

        writer = df.write.mode(mode).format(fmt)
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        for k, v in options.items():
            writer = writer.option(k, str(v))

        # Use saveAsTable for UC managed table
        writer.saveAsTable(full_name)

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
