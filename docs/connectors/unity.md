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

Alternatively, use a SQL query:

```yaml
    features_filtered:
      format: unity
      sql: >-
        SELECT * FROM main.fraud.feature_store
        WHERE dt = '{{ dt }}'
```

!!! note "Requirement"
    A read needs `table` or `sql`. Give the full three-part name in `table`.
    (`ubunye validate` also accepts `db_name` + `tbl_name` on a read, but the
    reader does not use them, so such a read fails when it runs.)

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

### MERGE (upsert)

Matched rows are updated on `merge_keys`, unmatched rows inserted. The first run
creates the table.

```yaml
    claims_curated:
      format: unity
      table: main.fraud.claims_curated
      mode: merge
      merge_keys: [policy_id, ds]
```

### Partition overwrite (backfills)

Replace only the partitions in the incoming DataFrame:

```yaml
    claims_daily:
      format: unity
      table: main.fraud.claims_daily
      mode: overwrite_partitions
      partitionBy: [ds]
      # or, as a Delta predicate:
      # replace_where: "ds = '{{ dt }}'"
```

---

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `format` | `"unity"` | Yes | Selects this connector |
| `table` | string | Conditional | Three-part Unity Catalog name |
| `catalog`, `schema`, `tbl_name` | string | Conditional | Writes only: the name in three parts, instead of `table` |
| `sql` | string | Conditional | Reads only: Spark SQL query |
| `file_format` | string | No | Writes only: `delta` (default) or `parquet` |
| `mode` | see [Write modes](../config/io.md#write-modes) | No | Default `append`. All six modes supported |
| `partitionBy` | list | No | Partition columns; required for `mode: overwrite_partitions` unless `replace_where` is set |
| `merge_keys` | list \| string | Conditional | Required for `mode: merge` |
| `replace_where` | string | No | Delta predicate for `mode: overwrite_partitions` |
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
