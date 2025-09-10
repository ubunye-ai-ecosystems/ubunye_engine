"""
JDBC Writer plugin for Ubunye (Spark).

Supports:
- Writing to a table with mode: append|overwrite|errorifexists|ignore
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
from ubunye.core.interfaces import Writer


class JdbcWriter(Writer):
    """Write a Spark DataFrame to any JDBC destination using Spark's JDBC connector."""

    REQUIRED = ("url", "table")

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
                raise ValueError(f"JDBC writer requires '{key}'")

        url = cfg["url"]
        dbtable = cfg["table"] or cfg.get("dbtable")
        if not dbtable:
            raise ValueError("Provide 'table' (or 'dbtable') for JDBC writer")

        driver = cfg.get("driver")
        user = cfg.get("user")
        password = cfg.get("password")
        mode = (cfg.get("mode") or "append").lower()

        writer = df.write.format("jdbc").mode(mode).option("url", url).option("dbtable", dbtable)

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
