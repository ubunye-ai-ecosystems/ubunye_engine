# Connectors — Overview

Connectors are the I/O layer of Ubunye. Each connector is a **Reader** or **Writer** plugin
registered under the `ubunye.readers` or `ubunye.writers` entry point group.

---

## Built-in connectors

| Format | Reader | Writer | Notes |
|---|---|---|---|
| [`hive`](hive.md) | Yes | Yes | Hive Metastore via Spark SQL |
| [`jdbc`](jdbc.md) | Yes | Yes | PostgreSQL, MySQL, Oracle, SQL Server, … |
| [`unity`](unity.md) | Yes | Yes | Databricks Unity Catalog (3-part names) |
| [`s3`](s3.md) | Yes | Yes | S3, HDFS, ADLS — any Spark-readable path |
| [`delta`](s3.md) | Yes | Yes | Delta Lake — by path or table name |
| `binary` | Yes | No | Raw binary files |
| [`rest_api`](rest_api.md) | Yes | Yes | HTTP REST with auth, pagination, retry |

---

## How connectors are loaded

The `PluginRegistry` discovers connectors at startup via Python entry points:

```toml
# pyproject.toml
[project.entry-points."ubunye.readers"]
hive     = "ubunye.plugins.readers.hive:HiveReader"
jdbc     = "ubunye.plugins.readers.jdbc:JdbcReader"
rest_api = "ubunye.plugins.readers.rest_api:RestApiReader"

[project.entry-points."ubunye.writers"]
s3       = "ubunye.plugins.writers.s3:S3Writer"
rest_api = "ubunye.plugins.writers.rest_api:RestApiWriter"
```

Any installed package that declares these entry points is automatically picked up.
List all discovered connectors:

```bash
ubunye plugins
```

---

## Connector config structure

Every connector is declared as an `IOConfig` block:

```yaml
CONFIG:
  inputs:
    <name>:
      format: <format>          # selects the connector
      # connector-specific fields below
      options: {}               # raw Spark reader/writer options

  outputs:
    <name>:
      format: <format>
      mode: overwrite           # overwrite | append | merge
      options: {}
```

`IOConfig` uses `extra="allow"`, so plugin-specific keys (e.g. `auth`, `pagination`)
pass through unchanged.

---

## Writing your own connector

See [Writing a Plugin](plugin_guide.md) for a step-by-step guide to building a
custom Reader or Writer and registering it with the entry point system.
