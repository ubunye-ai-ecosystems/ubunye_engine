# Hive Connector

Reads and writes Hive Metastore tables via Spark SQL.
Requires PySpark with a configured Hive metastore (`spark.sql.catalogImplementation: hive`).

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

These keys are fixed when the Spark session starts, so they work only when
Ubunye creates the session (`ubunye run` on the `spark` backend). Where the
platform starts the session (Databricks, EMR, `spark-submit`), set them there
instead; in `ENGINE.spark_conf` they are refused. See
[Engine & Profiles](../config/engine.md).

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
`mode: overwrite_partitions` instead of `overwrite`:

```yaml
  outputs:
    events_partitioned:
      format: hive
      db_name: clean
      tbl_name: events
      mode: overwrite_partitions
      partitionBy: [event_date]
```

