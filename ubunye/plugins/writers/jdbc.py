"""
JDBC Writer plugin for Ubunye (Spark).

Supports:
- Writing to a table with mode: append|overwrite|errorifexists|ignore
  (merge / overwrite_partitions are Delta-only and rejected here)
- Credentials & driver
- Extra options merged last (e.g., batchsize, truncate for overwrite, isolationLevel)

Example config (config.yaml):
  outputs:
    policy_dim_out:
      format: jdbc
      url: "jdbc:postgresql://db:5432/insurance"
      table: "policy_dim"
      user: "{{ env.DB_USER }}"
      password: "{{ env.DB_PASSWORD }}"
      driver: "org.postgresql.Driver"
      mode: append
      options:
        batchsize: 5000
        isolationLevel: "READ_COMMITTED"
"""

from __future__ import annotations

from typing import Any, Dict

from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError
from ubunye.core.interfaces import Writer

# Spark's JDBC sink speaks only the four native save modes. MERGE / dynamic
# partition overwrite are lakehouse concepts with no JDBC equivalent — asking
# for them is a config error, not something to silently downgrade.
SUPPORTED_MODES = write_modes.NATIVE_SAVE_MODES


class JdbcWriter(Writer):
    """Write a Spark DataFrame to any JDBC destination using Spark's JDBC connector."""

    REQUIRED = ("url", "table")

    SUPPORTS_MERGE = False  # a JDBC target has no MERGE in Spark
    MERGE_FILE_FORMATS = frozenset()

    @classmethod
    def validate_config(cls, cfg):
        errors = []
        if not cfg.get("url"):
            errors.append("format 'jdbc' requires 'url'")
        if not cfg.get("table") and not cfg.get("dbtable"):
            errors.append("format 'jdbc' as an output requires 'table'")
        return errors

    def write(self, df: Any, cfg: Dict[str, Any], backend) -> None:
        """
        Build a `df.write.format("jdbc")` with all given options and save.

        Parameters
        ----------
        df : DataFrame
            Spark DataFrame to write.
        cfg : dict
            Writer configuration (see module docstring for keys).
        backend : SparkBackend
            Ubunye Spark backend (unused directly, but enforced for consistency).
        """
        for key in self.REQUIRED:
            if key not in cfg or not cfg.get(key):
                raise SinkWriteError(
                    f"JDBC writer requires '{key}'.",
                    context={"Format": "jdbc", "Missing": key},
                    hint=f"Set '{key}' in your JDBC output config.",
                )

        url = cfg["url"]
        dbtable = cfg["table"] or cfg.get("dbtable")
        if not dbtable:
            raise SinkWriteError(
                "JDBC writer requires 'table' or 'dbtable'.",
                context={"Format": "jdbc"},
                hint="Set table: 'schema.table' in your JDBC output config.",
            )

        driver = cfg.get("driver")
        user = cfg.get("user")
        password = cfg.get("password")
        resolved = write_modes.resolve(
            cfg,
            connector="jdbc",
            supported=SUPPORTED_MODES,
            merge_formats=self.MERGE_FILE_FORMATS,
            default="append",
        )

        writer = (
            df.write.format("jdbc")
            .mode(resolved.save_mode)
            .option("url", url)
            .option("dbtable", dbtable)
        )

        if driver:
            writer = writer.option("driver", driver)
        if user is not None:
            writer = writer.option("user", user)
        if password is not None:
            writer = writer.option("password", password)

        # Extras merged last (batchsize, truncate, isolationLevel, createTableOptions, etc.)
        for k, v in (cfg.get("options") or {}).items():
            writer = writer.option(k, str(v))

        writer.save()
