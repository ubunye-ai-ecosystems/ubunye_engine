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
      mode: overwrite       # overwrite | append
      options:
        fileFormat: parquet
        compression: snappy
```

---

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `format` | `"hive"` | Yes | Selects this connector |
| `db_name` | string | Conditional | Database name (required unless `sql` is set) |
| `tbl_name` | string | Conditional | Table name (required unless `sql` is set) |
| `sql` | string | Conditional | SQL query (alternative to `db_name` + `tbl_name`) |
| `mode` | `overwrite` \| `append` | No | Write mode (outputs only) |
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

```yaml
  outputs:
    events_partitioned:
      format: hive
      db_name: clean
      tbl_name: events
      mode: overwrite
      options:
        partitionBy: event_date
```

Or use Spark's `partitionBy` via the options dict — keys are passed as
`df.write.options(**options)` before `.saveAsTable()`.
