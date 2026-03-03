"""
JDBC Reader plugin for Ubunye (Spark).

Supports:
- Table read:      dbtable="schema.table"
- SQL subquery:    sql="SELECT ...", auto-wrapped as "( <sql> ) AS t"
- Credentials:     url, user, password, driver
- Tuning:          fetchsize, partitionColumn, lowerBound, upperBound, numPartitions
- Extra options:   options: { <key>: <value>, ... } (merged last)

Example config (config.yaml):
  inputs:
    policy_jdbc:
      format: jdbc
      url: "jdbc:postgresql://db:5432/insurance"
      table: policy_dim                  # OR: sql: "SELECT * FROM policy_dim WHERE ..."
      user: "{{ env.DB_USER }}"
      password: "{{ env.DB_PASSWORD }}"
      driver: "org.postgresql.Driver"
      fetchsize: 10000
      partitionColumn: "policy_id"
      lowerBound: 1
      upperBound: 500000
      numPartitions: 8
      options:
        ssl: "true"

Returned object is a Spark DataFrame.
"""
from __future__ import annotations

from typing import Any, Dict

from ubunye.core.interfaces import Reader


class JdbcReader(Reader):
    """Read a Spark DataFrame from any JDBC source using Spark's built-in JDBC connector."""

    REQUIRED = ("url",)
    TABLE_KEYS = ("table", "dbtable", "sql")

    def read(self, cfg: Dict[str, Any], backend) -> Any:
        """
        Build a `spark.read.format("jdbc")` with all given options.

        Parameters
        ----------
        cfg : dict
            Reader configuration (see module docstring for keys).
        backend : SparkBackend
            Ubunye Spark backend (must expose .spark).
        """
        spark = backend.spark

        # ---- validate minimal settings ----
        for key in self.REQUIRED:
            if key not in cfg or not cfg.get(key):
                raise ValueError(f"JDBC reader requires '{key}'")

        url = cfg["url"]
        driver = cfg.get("driver")
        user = cfg.get("user")
        password = cfg.get("password")

        # Determine dbtable: prefer explicit table/dbtable; else wrap SQL subquery.
        dbtable = cfg.get("table") or cfg.get("dbtable")
        sql = cfg.get("sql")
        if not dbtable and not sql:
            raise ValueError("Provide either 'table'/'dbtable' or 'sql' for JDBC reader")

        if sql and not dbtable:
            # Spark needs a subquery wrapped in parentheses with an alias.
            dbtable = f"( {sql} ) AS t"

        reader = spark.read.format("jdbc").option("url", url)

        # Core connection props
        reader = reader.option("dbtable", dbtable)
        if driver:
            reader = reader.option("driver", driver)
        if user is not None:
            reader = reader.option("user", user)
        if password is not None:
            reader = reader.option("password", password)

        # Tuning / partitioning (all optional)
        if "fetchsize" in cfg:
            reader = reader.option("fetchsize", str(cfg["fetchsize"]))
        if "partitionColumn" in cfg:
            # When present, Spark will do parallel JDBC reads.
            for key in ("partitionColumn", "lowerBound", "upperBound", "numPartitions"):
                if key in cfg and cfg[key] is not None:
                    reader = reader.option(key, str(cfg[key]))

        # Arbitrary extras merged last (so they win)
        for k, v in (cfg.get("options") or {}).items():
            reader = reader.option(k, str(v))

        return reader.load()
