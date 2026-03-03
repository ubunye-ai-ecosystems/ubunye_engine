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
      mode: overwrite | append | merge
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
| `mode` | `overwrite` \| `append` \| `merge` | Write mode (outputs only). |

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

| Mode | Behaviour |
|---|---|
| `overwrite` | Drop existing data and replace entirely |
| `append` | Insert new rows without touching existing data |
| `merge` | Delta MERGE (upsert); requires Delta format and merge keys in connector options |
