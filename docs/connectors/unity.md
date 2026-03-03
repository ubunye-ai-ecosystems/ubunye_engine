# Unity Catalog Connector

Reads and writes Databricks Unity Catalog tables using three-part names
(`catalog.schema.table`). Requires a Databricks cluster with Unity Catalog enabled.

---

## Read

```yaml
CONFIG:
  inputs:
    features:
      format: unity
      table: main.fraud.feature_store     # three-part name (preferred)
```

Alternatively, use `db_name` + `tbl_name` or a SQL query:

```yaml
    features:
      format: unity
      db_name: fraud                      # schema name
      tbl_name: feature_store             # table name
      # catalog is inferred from the active Unity Catalog

    # or with SQL:
    features_filtered:
      format: unity
      sql: >-
        SELECT * FROM main.fraud.feature_store
        WHERE dt = '{{ dt }}'
```

!!! note "Requirement"
    At least one of `table`, (`db_name` + `tbl_name`), or `sql` is required.

---

## Write

```yaml
CONFIG:
  outputs:
    predictions:
      format: unity
      table: main.fraud.model_predictions
      mode: append
```

---

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `format` | `"unity"` | Yes | Selects this connector |
| `table` | string | Conditional | Three-part Unity Catalog name |
| `db_name` | string | Conditional | Schema name (without catalog) |
| `tbl_name` | string | Conditional | Table name |
| `sql` | string | Conditional | Spark SQL query |
| `mode` | `overwrite` \| `append` | No | Write mode (outputs only) |
| `options` | dict | No | Spark reader/writer options |

---

## Spark configuration for Unity Catalog

On Databricks, Unity Catalog is the default catalog when configured in the workspace.
For local Spark sessions connecting to Databricks, set:

```yaml
ENGINE:
  spark_conf:
    spark.databricks.service.token: "{{ env.DATABRICKS_TOKEN }}"
    spark.databricks.service.address: "https://{{ env.DATABRICKS_HOST }}"
```

---

## Access control

Unity Catalog enforces column-level and row-level security.
Ensure the service principal or user has `SELECT` (read) and `MODIFY` (write) privileges
on the target table via the Databricks Catalog Explorer or `GRANT` SQL statements.

---

## Differences from `hive`

| | `hive` | `unity` |
|---|---|---|
| Name resolution | Two-part (`db.table`) | Three-part (`catalog.schema.table`) |
| Governance | None beyond Hive ACLs | Column masking, row filters, audit logs |
| Delta by default | No | Yes |
| Databricks required | No | Yes |
