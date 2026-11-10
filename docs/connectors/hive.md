# Hive Connector

Reads and writes tables in a Hive metastore (the catalog that maps a
`database.table` name to files) through Spark SQL. Use it on a Spark cluster that
has a Hive metastore configured (`spark.sql.catalogImplementation: hive`).

---

## Read

```yaml
CONFIG:
  inputs:
    raw_claims:
      format: hive
      db_name: raw          # database name
      tbl_name: claims      # table name
```

Alternatively, supply a `sql` query:

```yaml
    raw_claims:
      format: hive
      sql: >-
        SELECT id, amount, event_date
        FROM raw.claims
        WHERE event_date >= '{{ dt }}'
```

!!! note "Requirement"
    For a read, either (`db_name` + `tbl_name`) **or** `sql` is required. For a
    write, `db_name` + `tbl_name` is required. Anything else is a validation error.

---

## Write

```yaml
CONFIG:
  outputs:
    clean_claims:
      format: hive
      db_name: clean
      tbl_name: claims
      mode: overwrite
      file_format: parquet
      options:
        compression: snappy
```

---

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `format` | `"hive"` | Yes | Selects this connector |
| `db_name` | string | Conditional | Database name (required unless a read sets `sql`) |
| `tbl_name` | string | Conditional | Table name (required unless a read sets `sql`) |
| `sql` | string | Conditional | SQL query, reads only (alternative to `db_name` + `tbl_name`) |
| `file_format` | string | No | Writes only: `parquet` (default), `orc`, `delta` |
| `mode` | see [Write modes](../config/io.md#write-modes) | No | Default `append`. All six modes supported; `mode: merge` requires `file_format: delta` |
| `merge_keys` | list \| string | Conditional | Required for `mode: merge` |
| `partitionBy` | list | No | Writes only: partition columns |
| `replace_where` | string | No | With `file_format: delta`: on an `append`, the batch's rows, replaced by each run ([Delta appends](s3.md#appends-that-land-once-and-reruns-that-correct)) |
| `options` | dict | No | Spark reader/writer options |

---

## Spark configuration for Hive

```yaml
ENGINE:
  spark_conf:
    spark.sql.catalogImplementation: "hive"
    spark.sql.warehouse.dir: "/user/hive/warehouse"
    hive.metastore.uris: "thrift://metastore-host:9083"
```

These keys are fixed when the Spark session starts. Where the platform starts it
(Databricks, EMR, `spark-submit`), set them there instead. See
[Spark keys fixed at session start](../config/engine.md#spark-keys-fixed-at-session-start).

---

## Partitioned writes

`partitionBy` is a top-level list, not a Spark option:

```yaml
  outputs:
    events_partitioned:
      format: hive
      db_name: clean
      tbl_name: events
      mode: overwrite
      partitionBy: [event_date]
```

To re-run a single day without dropping the rest of the table, use
`mode: overwrite_partitions` instead of `overwrite`. How each
[write mode](../config/io.md#write-modes) behaves, `merge` included (it needs
`file_format: delta` on `hive`), is on the Inputs & Outputs page.

```yaml
  outputs:
    events_partitioned:
      format: hive
      db_name: clean
      tbl_name: events
      mode: overwrite_partitions
      partitionBy: [event_date]
```
