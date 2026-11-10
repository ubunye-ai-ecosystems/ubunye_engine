# Unity Catalog Connector

Reads and writes Databricks Unity Catalog tables, named in three parts
(`catalog.schema.table`). Use it for tasks that run on Databricks with Unity
Catalog enabled.

---

## Read

```yaml
CONFIG:
  inputs:
    features:
      format: unity
      table: main.fraud.feature_store     # three-part name (preferred)
```

Alternatively, use a SQL query:

```yaml
    features_filtered:
      format: unity
      sql: >-
        SELECT * FROM main.fraud.feature_store
        WHERE dt = '{{ dt }}'
```

!!! note "Requirement"
    A read needs `table`, `sql`, or `catalog` + `schema` + `tbl_name`.
    `db_name` is a Hive key: `ubunye validate` refuses it on a unity read.

!!! note "Names with a hyphen: quote them with `| ident`"
    Databricks catalogs are often named like `aws-x-catalog`. In SQL that name must
    be in backticks, or it reads as a subtraction. When the name comes from a job
    parameter or an environment variable, write
    `{{ env.TELM_READ_CATALOG | ident }}`: it adds the backticks once, whether the
    value arrives with them or not, and quotes each part of a
    `catalog.schema.table`. See [SQL names](../config/jinja.md#sql-names-ident).

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

A write needs `table`, or `catalog` + `schema` + `tbl_name`. Tables are written as
Delta unless `file_format` says otherwise.

### Table comment, properties and maintenance

```yaml
    claims_curated:
      format: unity
      table: main.fraud.claims_curated
      options:
        comment: "Curated fraud claims"     # COMMENT ON TABLE after the write
        tblproperties:
          quality: "silver"                 # ALTER TABLE ... SET TBLPROPERTIES
      optimize:
        enabled: true                       # OPTIMIZE after the write
        zorder_by: [policy_id]              # ... ZORDER BY (policy_id)
      vacuum:
        hours: 168                          # VACUUM ... RETAIN 168 HOURS
```

`optimize` and `vacuum` need a Delta table. If the engine cannot run them, the
data is still written and a warning says the maintenance was skipped.

### Write modes

All seven [write modes](../config/io.md#write-modes) work on `unity`; the Inputs &
Outputs page owns how each behaves. One example, a partition overwrite for a
backfill, which replaces only the partitions in the incoming data:

```yaml
    claims_daily:
      format: unity
      table: main.fraud.claims_daily
      mode: overwrite_partitions
      partitionBy: [ds]
      # or, as a Delta predicate:
      # replace_where: "ds = '{{ dt }}'"
```

A `merge` updates matched rows on `merge_keys` and inserts the rest; the first run
creates the table. See [`merge`](../config/io.md#merge).

---

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `format` | `"unity"` | Yes | Selects this connector |
| `table` | string | Conditional | Three-part Unity Catalog name |
| `catalog`, `schema`, `tbl_name` | string | Conditional | Writes only: the name in three parts, instead of `table` |
| `sql` | string | Conditional | Reads only: Spark SQL query |
| `file_format` | string | No | Writes only: `delta` (default) or `parquet` |
| `mode` | see [Write modes](../config/io.md#write-modes) | No | Default `append`. All seven modes supported |
| `partitionBy` | list | No | Partition columns; required for `mode: overwrite_partitions` unless `replace_where` is set |
| `merge_keys` | list \| string | Conditional | Required for `mode: merge` |
| `replace_where` | string | No | Delta predicate for `mode: overwrite_partitions`; on a Delta `append`, the batch's rows, replaced by each run ([Delta appends](s3.md#appends-that-land-once-and-reruns-that-correct)) |
| `optimize` | dict | No | Writes only: `enabled`, `zorder_by` |
| `vacuum` | dict | No | Writes only: `hours` of history to keep |
| `options` | dict | No | Spark writer options; `comment` and `tblproperties` are applied to the table |

---

## Spark configuration for Unity Catalog

On Databricks, Unity Catalog is the default catalog when configured in the workspace.
The engine uses the session that is already running there (`--backend databricks`,
or the default on Databricks). It does not open a remote connection to a workspace
itself, so run Unity tasks on Databricks. `ENGINE.catalog` and `ENGINE.schema_name`
set the default catalog and schema (see [Engine & Profiles](../config/engine.md)).

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
