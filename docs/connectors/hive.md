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
    Either (`db_name` + `tbl_name`) **or** `sql` is required. Providing neither raises a
    validation error.

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
| `db_name` | string | Conditional | Database name (required unless `sql` / `table` is set) |
| `tbl_name` | string | Conditional | Table name (required unless `sql` / `table` is set) |
| `table` | string | Conditional | `db.table`, as an alternative to `db_name` + `tbl_name` |
| `sql` | string | Conditional | SQL query for reads (alternative to `db_name` + `tbl_name`) |
| `file_format` | string | No | `parquet` (default), `orc`, `delta` |
| `mode` | see [Write modes](../config/io.md#write-modes) | No | Default `append`. All six modes supported; `mode: merge` requires `file_format: delta` |
| `partitionBy` | list | No | Partition columns |
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

Or use Spark's `partitionBy` via the options dict — keys are passed as
`df.write.options(**options)` before `.saveAsTable()`.
