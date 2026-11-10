# JDBC Connector

Reads and writes any database that has a JDBC driver (Java's standard way to talk
to a database): PostgreSQL, MySQL, Oracle, SQL Server, Redshift, Snowflake, and
more. Needs Spark. Passwords are given as [`secret://`](../config/secrets.md)
references.

---

## Read

```yaml
CONFIG:
  inputs:
    customers:
      format: jdbc
      url: "jdbc:postgresql://db-host:5432/mydb"
      table: public.customers          # or use sql:
      user: "{{ env.DB_USER }}"
      password: "secret://env/DB_PASS"
      options:
        fetchsize: "10000"
```

Parallelised read (requires a numeric partition column). The tuning keys can be
top-level keys or go under `options`; both reach Spark the same way:

```yaml
    large_table:
      format: jdbc
      url: "jdbc:postgresql://db-host:5432/mydb"
      table: public.transactions
      user: "{{ env.DB_USER }}"
      password: "secret://env/DB_PASS"
      options:
        partitionColumn: id
        lowerBound: "1"
        upperBound: "10000000"
        numPartitions: "16"
        fetchsize: "50000"
```

Custom SQL:

```yaml
    recent_orders:
      format: jdbc
      url: "jdbc:postgresql://db-host:5432/mydb"
      sql: "SELECT * FROM orders WHERE created_at >= '{{ dt }}'"
      user: "{{ env.DB_USER }}"
      password: "secret://env/DB_PASS"
```

!!! note "Requirement"
    `url` is required. A read needs `table` (or `dbtable`) **or** `sql`. A write
    needs `table`.

---

## Write

```yaml
CONFIG:
  outputs:
    predictions:
      format: jdbc
      url: "jdbc:postgresql://db-host:5432/mydb"
      table: ml.predictions
      user: "{{ env.DB_USER }}"
      password: "secret://env/DB_PASS"
      mode: append
      options:
        batchsize: "5000"
        truncate: "true"
```

### A batch that lands once

Spark's JDBC writer commits each partition on its own, so a plain append that fails
part way leaves part of the batch, and a rerun inserts it again. Name the batch's
rows with `replace_where` and the append lands once:

```yaml
    payments:
      format: jdbc
      url: "jdbc:postgresql://db-host:5432/billing"
      table: billing.payments
      mode: append
      replace_where: "month = '{{ dt }}'"
```

Spark writes the batch into a staging table beside the target
(`ubunye_stage_<id>`, same schema), then one database transaction deletes the rows
the predicate selects and inserts the staged rows. It commits or changes nothing,
and the staging table is dropped. A retry after a crash, and `--rerun` of a finished
batch, replace the batch from the current data. The predicate is SQL in your
database's dialect. The database user needs to create and drop a table in the
target's schema. A staging table a killed run left behind holds no target data; the
next run of the batch replaces it. Without `replace_where` an append is Spark's own,
as before.

---

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `format` | `"jdbc"` | Yes | Selects this connector |
| `url` | string | Yes | JDBC connection URL |
| `table` | string | Conditional | Fully qualified table name (required for writes; for reads unless `sql` is set) |
| `dbtable` | string | No | Spark's own name for `table`; reads accept either |
| `sql` | string | Conditional | SQL query, reads only (alternative to `table`) |
| `driver` | string | No | JDBC driver class, e.g. `org.postgresql.Driver` |
| `user` | string | No | Database username |
| `password` | string | No | Database password, as a [`secret://`](../config/secrets.md) reference (`secret://env/DB_PASS`, `secret://aws-sm/prod/db#password`) |
| `fetchsize`, `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` | string or int | No | Read tuning, passed to Spark |
| `mode` | `append` \| `overwrite` \| `errorifexists` \| `ignore` | No | Default `append`. Spark's native save modes only; `merge` and `overwrite_partitions` are refused by this connector. `ubunye plan` catches that before a run; `ubunye validate` does not ([write modes](../config/io.md#write-modes)) |
| `replace_where` | string | No | On an `append`: SQL selecting the batch's rows; the batch is staged, then those rows are replaced in one transaction ([above](#a-batch-that-lands-once)) |
| `options` | dict | No | Spark JDBC options, applied last |

---

## JDBC URLs by database

| Database | URL pattern |
|---|---|
| PostgreSQL | `jdbc:postgresql://host:5432/db` |
| MySQL | `jdbc:mysql://host:3306/db` |
| Oracle | `jdbc:oracle:thin:@host:1521/SID` |
| SQL Server | `jdbc:sqlserver://host:1433;databaseName=db` |
| Redshift | `jdbc:redshift://cluster.id.region.redshift.amazonaws.com:5439/db` |

---

## JDBC driver setup

The JDBC driver JAR must be on the Spark classpath:

```yaml
ENGINE:
  spark_conf:
    spark.jars: "/opt/spark/jars/postgresql-42.7.0.jar"
    # or for multiple JARs:
    # spark.jars: "/opt/spark/jars/pg.jar,/opt/spark/jars/mysql.jar"
```

`spark.jars` is fixed when the Spark session starts (see
[Spark keys fixed at session start](../config/engine.md#spark-keys-fixed-at-session-start)).
On Databricks, install the driver via the cluster Libraries UI; elsewhere pass it
to `spark-submit --jars`.
