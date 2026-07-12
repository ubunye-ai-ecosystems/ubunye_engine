# Inputs & Outputs

`CONFIG.inputs` and `CONFIG.outputs` are dictionaries where each key is a logical name
and each value is an `IOConfig` — a connector declaration.

---

## Structure

```yaml
CONFIG:
  inputs:
    <logical_name>:
      format: <format_type>
      # format-specific fields...

  outputs:
    <logical_name>:
      format: <format_type>
      mode: append | overwrite | errorifexists | ignore | merge | overwrite_partitions
      # format-specific fields...
```

- At least one input and one output are required.
- Logical names are arbitrary; they are passed to your transform as dict keys.

---

## Supported formats

| `format` | Direction | Connector |
|---|---|---|
| `hive` | read / write | Hive Metastore tables via Spark |
| `jdbc` | read / write | Any JDBC-compatible database |
| `unity` | read / write | Databricks Unity Catalog |
| `s3` | read / write | S3 or HDFS paths (Parquet, CSV, JSON…) |
| `delta` | read / write | Delta Lake tables by path or name |
| `binary` | read | Raw binary files |
| `rest_api` | read / write | HTTP REST endpoints |

---

## Common fields

These fields apply to most formats:

| Field | Type | Description |
|---|---|---|
| `format` | string | Required. One of the format types above. |
| `options` | dict | Spark reader/writer options (e.g. `header`, `delimiter`). |
| `mode` | see [Write modes](#write-modes) | Write mode (outputs only). |

---

## Format-specific fields

### `hive`

```yaml
format: hive
db_name: raw             # required if sql is not set
tbl_name: claims         # required if sql is not set
sql: "SELECT ..."        # alternative to db_name + tbl_name
```

### `jdbc`

```yaml
format: jdbc
url: "jdbc:postgresql://host:5432/db"   # required
table: public.claims                    # required unless sql is set
sql: "SELECT * FROM public.claims"      # alternative to table
user: "{{ env.DB_USER }}"
password: "{{ env.DB_PASS }}"
options:
  fetchsize: "10000"
  partitionColumn: id
  lowerBound: "1"
  upperBound: "1000000"
  numPartitions: "8"
```

### `unity`

```yaml
format: unity
table: main.fraud.claims       # three-part Unity Catalog name
# or:
db_name: fraud
tbl_name: claims
sql: "SELECT ..."
```

### `s3` / `delta`

```yaml
# S3 — path-based (any Spark-readable format)
format: s3
path: s3://my-bucket/data/claims/
options:
  header: "true"

# Delta — by path
format: delta
path: s3://my-bucket/delta/claims
mode: overwrite

# Delta — by table name
format: delta
table: main.fraud.claims
mode: append
```

### `binary`

```yaml
format: binary
path: /mnt/raw/documents/
```

### `rest_api`

```yaml
format: rest_api
url: "https://api.example.com/v1/records"
auth:
  type: bearer
  token: "{{ env.API_TOKEN }}"
pagination:
  type: cursor
  cursor_field: next_cursor
  page_size: 500
headers:
  Accept: application/json
```

See [REST API Connector](../connectors/rest_api.md) for the full REST API reference.

---

## Extra fields pass-through

`IOConfig` uses `extra="allow"`, so any plugin-specific keys you add are passed
through to the connector via `model_dump()`. This is how the REST API connector
receives `auth`, `pagination`, and `headers` without schema changes.

---

## Write modes

The first four are Spark's native `SaveMode` values. `merge` and
`overwrite_partitions` are Ubunye's lakehouse modes — Spark has no save mode for
either, so the engine implements them in `ubunye.core.write_modes`.

| Mode | Behaviour |
|---|---|
| `append` | Insert new rows without touching existing data |
| `overwrite` | Drop existing data and replace entirely |
| `errorifexists` | Fail if the target already exists (alias: `error`) |
| `ignore` | Do nothing if the target already exists |
| `merge` | Delta MERGE (upsert) on `merge_keys`. Creates the target on first run |
| `overwrite_partitions` | Replace only the partitions present in the DataFrame; leave the rest of the table intact |

Not every connector can honour every mode. Asking for one it cannot do raises
`SinkWriteError` **before any rows are written** — it is never silently downgraded.

| Connector | Supported modes | Default |
|---|---|---|
| `unity` | all six | `append` |
| `s3` | all six (`merge` and `replace_where` need `file_format: delta`) | `append` |
| `jdbc` | native four only — no `merge` / `overwrite_partitions` | `append` |
| `rest_api` | `append` (a REST sink has no table to merge into) | `append` |

Every writer defaults to `append`: the mode that cannot destroy data you did not
mean to destroy. Set `mode` explicitly when you want anything else.

### `merge`

Requires `merge_keys` — the columns that identify a row. Matched rows are updated,
unmatched rows inserted. On the first run the target does not exist yet, so the
engine creates it with a plain overwrite instead of a MERGE.

```yaml
    customer_features:
      format: s3
      path: s3://my-bucket/delta/features/
      file_format: delta
      mode: merge
      merge_keys: [id, dt]        # or options.merge_keys: "id,dt"
```

### `overwrite_partitions`

The backfill mode: re-run one day without wiping the rest of the table. Two ways
to say which partitions to replace —

```yaml
    # 1. Dynamic — replace whatever partitions the DataFrame contains
    daily_sales:
      format: s3
      path: s3://my-bucket/sales/
      mode: overwrite_partitions
      partitionBy: [dt]

    # 2. Predicate — replace exactly what the expression matches (Delta only)
    daily_sales:
      format: s3
      path: s3://my-bucket/delta/sales/
      file_format: delta
      mode: overwrite_partitions
      replace_where: "dt = '{{ dt }}'"
```

Without `partitionBy` or `replace_where`, dynamic overwrite silently degrades to a
full-table wipe — so the engine refuses the config instead.
