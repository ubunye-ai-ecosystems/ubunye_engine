# JDBC Connector

Reads and writes any JDBC-compatible database: PostgreSQL, MySQL, Oracle, SQL Server,
Redshift, Snowflake (via JDBC driver), and more.

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
      password: "{{ env.DB_PASS }}"
      options:
        fetchsize: "10000"
```

Parallelised read (requires a numeric partition column):

```yaml
    large_table:
      format: jdbc
      url: "jdbc:postgresql://db-host:5432/mydb"
      table: public.transactions
      user: "{{ env.DB_USER }}"
      password: "{{ env.DB_PASS }}"
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
      password: "{{ env.DB_PASS }}"
```

!!! note "Requirement"
    `url` is required. Either `table` **or** `sql` is required.

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
      password: "{{ env.DB_PASS }}"
      mode: append
      options:
        batchsize: "5000"
        truncate: "true"
```

---

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `format` | `"jdbc"` | Yes | Selects this connector |
| `url` | string | Yes | JDBC connection URL |
| `table` | string | Conditional | Fully qualified table name (required unless `sql` set) |
| `sql` | string | Conditional | SQL query for reads (alternative to `table`) |
| `user` | string | No | Database username |
| `password` | string | No | Database password — use `{{ env.VAR }}` |
| `mode` | `overwrite` \| `append` | No | Write mode (outputs only) |
| `options` | dict | No | Spark JDBC options |

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

On Databricks, install the driver via the cluster Libraries UI.
