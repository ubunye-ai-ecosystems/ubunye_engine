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

import logging
from typing import Any, Dict, List

from ubunye.adapters.spark import write_exec
from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError
from ubunye.core.interfaces import Writer

SUPPORTED_MODES = write_modes.ALL_MODES


logger = logging.getLogger(__name__)


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


# NOTE: `_is_databricks()` used to live here. It sniffed the platform by string-matching
# Spark conf keys for the substring "databricks", and used the guess to decide whether to
# run OPTIMIZE and VACUUM.
#
# It was wrong twice over.
#
# It guessed at its host — a connector should describe what it wants of the TARGET, not
# detect where it happens to be running. And the guess was factually wrong: OPTIMIZE,
# ZORDER and VACUUM are Delta features, and open-source Delta has had them for years. So
# anyone running Delta off Databricks silently did not get the maintenance they had
# explicitly asked for in their config, and nothing said so.
#
# Now: if the config asks for it, we ask the target for it. If the target cannot, that is
# reported, not guessed at in advance and not swallowed.


def _attempt(spark: Any, statement: str, what: str, table: str) -> None:
    """Run a maintenance statement, and report rather than pretend if it fails.

    The write already succeeded by the time we get here — the data is safe. Failing the
    whole task because a VACUUM is unsupported would throw away a good write over
    housekeeping. But skipping in silence is how someone believes their table is being
    compacted for a year while it is not.
    """
    try:
        spark.sql(statement)
        logger.info("%s: %s", what, statement)
    except Exception as exc:  # noqa: BLE001 — any engine that cannot do it
        logger.warning(
            "'%s' was requested for %s but this engine could not do it: %s. "
            "OPTIMIZE/ZORDER/VACUUM need a Delta target. The data was written; only the "
            "maintenance was skipped.",
            what,
            table,
            str(exc).splitlines()[0][:120],
        )


class UnityTableWriter(Writer):
    """Write DataFrame to a Unity Catalog table (Delta by default)."""

    SUPPORTS_MERGE = True
    MERGE_FILE_FORMATS = frozenset({"delta"})

    @classmethod
    def validate_config(cls, cfg):
        # A WRITER needs somewhere to write. Unlike the reader, `sql` is not an option --
        # a difference the core could not express while one table validated both roles.
        if cfg.get("table") or (cfg.get("catalog") and cfg.get("schema") and cfg.get("tbl_name")):
            return []
        return ["format 'unity' as an output requires 'table' or (catalog + schema + tbl_name)"]

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
            merge_formats=self.MERGE_FILE_FORMATS,
            default="append",
            file_format=fmt,
        )

        partition_by: List[str] = list(cfg.get("partitionBy", []) or [])
        options: Dict[str, Any] = dict(cfg.get("options", {}) or {})
        comment: str | None = options.pop("comment", None)  # we'll set via SQL after creation
        tblprops: Dict[str, str] = dict(options.pop("tblproperties", {}) or {})

        write_exec.apply(
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

        self._maintain(spark, full_name, cfg)

    @staticmethod
    def _maintain(spark: Any, table: str, cfg: Dict[str, Any]) -> None:
        """Run the table maintenance the CONFIG asked for. Do not guess at the host.

        OPTIMIZE, ZORDER and VACUUM are Delta features, not Databricks features — open
        source Delta has had them for years. The old code gated them behind a guess
        ("does the Spark conf mention databricks?") and, off Databricks, silently skipped
        maintenance the user had explicitly requested.

        Nobody asks for `optimize: {enabled: true}` by accident. If the target cannot do
        it, say so; do not decide in advance that it cannot, and do not swallow it.
        """
        opt = cfg.get("optimize") or {}
        if opt.get("enabled"):
            zcols = opt.get("zorder_by") or []
            statement = f"OPTIMIZE {table}"
            if zcols:
                statement += f" ZORDER BY ({', '.join(zcols)})"
            _attempt(spark, statement, "optimize", table)

        vac = cfg.get("vacuum") or {}
        if "hours" in vac:
            _attempt(spark, f"VACUUM {table} RETAIN {int(vac['hours'])} HOURS", "vacuum", table)
