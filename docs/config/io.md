# Inputs & Outputs

This page is for anyone writing a task's `config.yaml`. It shows how to say what a
task reads (`CONFIG.inputs`) and writes (`CONFIG.outputs`), and it is the home page
for **write modes**: how an output treats data already in its target.

Each entry has a name you choose and a block that names a connector with `format:`.
A connector is the built-in code that reads or writes one kind of storage (see
[Connectors](../connectors/overview.md)).

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
| `s3` | read / write | Any path: S3, ADLS, GCS, HDFS, local (Parquet, CSV, JSON...) |
| `delta` | read / write | Delta Lake tables by path or name |
| `binary` | read | Raw binary files |
| `rest_api` | read / write | HTTP REST endpoints |

`format` may also name any connector installed as a plugin. `ubunye plugins`
lists what is installed; an unknown `format` is refused with that list.

---

## Common fields

These fields apply to most formats:

| Field | Type | Description |
|---|---|---|
| `format` | string | Required. One of the format types above. |
| `options` | dict | Spark reader/writer options (e.g. `header`, `delimiter`). |
| `mode` | see [Write modes](#write-modes) | Write mode (outputs only). |
| `merge_keys` | list or string | Key columns for `mode: merge` (outputs only). |
| `replace_where` | string | Delta predicate for `mode: overwrite_partitions` (outputs only). On a Delta or JDBC `append`, the rows of the run's batch: each run replaces them, so a rerun corrects the batch instead of adding it twice. |

---

## Format-specific fields

### `hive`

```yaml
format: hive
db_name: raw             # required if sql is not set
tbl_name: claims         # required if sql is not set
sql: "SELECT ..."        # alternative to db_name + tbl_name (inputs only)
```

As an output, `hive` needs `db_name` + `tbl_name`.

### `jdbc`

```yaml
format: jdbc
url: "jdbc:postgresql://host:5432/db"   # required
table: public.claims                    # required unless sql is set (always for outputs)
sql: "SELECT * FROM public.claims"      # alternative to table (inputs only)
user: "{{ env.DB_USER }}"
password: "secret://env/DB_PASS"        # see Secrets
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
sql: "SELECT ..."              # inputs only
# outputs also take catalog + schema + tbl_name instead of table
```

A name that comes from a variable goes through `| ident`, which quotes it once
(a catalog like `aws-x-catalog` needs backticks in SQL):
`sql: "SELECT * FROM {{ env.TELM_READ_CATALOG | ident }}.raw.trips"`. See
[SQL names](jinja.md#sql-names-ident).

### `s3` / `delta`

```yaml
# S3: path-based; file_format picks the file type (default parquet)
format: s3
path: s3://my-bucket/data/claims/
file_format: csv
options:
  header: "true"

# Delta: by path
format: delta
path: s3://my-bucket/delta/claims
mode: overwrite

# Delta: by table name
format: delta
table: main.fraud.claims
mode: append
```

### `binary`

```yaml
format: binary
path: /mnt/raw/documents/
path_glob_filter: "*.pdf"   # optional
recursive: true             # optional
```

### `rest_api`

```yaml
format: rest_api
url: "https://api.example.com/v1/records"
auth:
  type: bearer
  token: "secret://env/API_TOKEN"
pagination:
  type: cursor
  cursor_response_key: next_cursor
headers:
  Accept: application/json
```

See [REST API Connector](../connectors/rest_api.md) for the full REST API reference.

---

## Connector keys are checked

An input or output block carries whatever keys its connector reads, and the
whole block is passed to the connector. This is how the REST API connector
receives `auth`, `pagination` and `headers` without schema changes.

Each built-in connector declares the keys it reads, so any other key is refused
by `ubunye validate`, with the closest real one:

```text
outputs.clean: 'paht' is not a setting of 's3'; did you mean 'path'?
```

Each connector also says what it requires (`s3` needs `path`, `jdbc` needs `url`,
and so on), checked the same way. Options for Spark itself go under `options`.
A third-party connector that declares no keys accepts any key.

---

## Write modes

The write mode (`mode:` on an output) says what happens to data already in the
target. The first four are Spark's own save modes. `merge` and
`overwrite_partitions` are added by Ubunye: Spark has no save mode for either.

| Mode | Behaviour |
|---|---|
| `append` | Insert new rows without touching existing data |
| `overwrite` | Drop existing data and replace entirely |
| `errorifexists` | Fail if the target already exists (alias: `error`) |
| `ignore` | Do nothing if the target already exists |
| `merge` | Delta MERGE (upsert) on `merge_keys`. Creates the target on first run |
| `overwrite_partitions` | Replace only the partitions present in the DataFrame; leave the rest of the table intact |

Not every connector can honour every mode. Asking for one it cannot do raises
`SinkWriteError` **before any rows are written**. It is never silently downgraded.

`ubunye plan` catches this before a run. `ubunye validate` does not check write
modes against the connector, so a `jdbc` output with `mode: merge` passes
`validate` and fails `plan`:

```text
  problem: outputs.predictions: Write mode 'merge' is not supported by the 'jdbc' connector.
```

| Connector | Supported modes | Default |
|---|---|---|
| `delta` | all six | `append` |
| `unity` | all six | `append` |
| `s3` | all six (`merge` and `replace_where` need `file_format: delta`) | `append` |
| `hive` | all six (`merge` needs `file_format: delta`) | `append` |
| `jdbc` | native four only: no `merge` / `overwrite_partitions` | `append` |
| `rest_api` | `append` (a REST sink has no table to merge into) | `append` |
| `binary` | none: read-only, rejected in `CONFIG.outputs` | none |

Every writer defaults to `append`: the mode that cannot destroy data you did not
mean to destroy. Set `mode` explicitly when you want anything else.

### `merge`

Requires `merge_keys`: the columns that identify a row. Matched rows are updated,
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

The backfill mode: re-run one day without wiping the rest of the table. Three ways
to say which partitions to replace:

```yaml
    # 1. Dynamic: replace whatever partitions the DataFrame contains
    daily_sales:
      format: s3
      path: s3://my-bucket/sales/
      mode: overwrite_partitions
      partitionBy: [dt]

    # 2. Predicate: replace exactly what the expression matches (Delta only)
    daily_sales:
      format: delta
      path: s3://my-bucket/delta/sales/
      mode: overwrite_partitions
      replace_where: "dt = '{{ dt }}'"

    # 3. A table: INSERT OVERWRITE of just the incoming partitions
    daily_sales:
      format: hive
      db_name: clean
      tbl_name: sales
      mode: overwrite_partitions
      partitionBy: [dt]
```

Without `partitionBy` or `replace_where`, dynamic overwrite silently degrades to a
full-table wipe, so the engine refuses it instead. `ubunye plan` reports this
before a run (`validate` does not):

```text
  problem: outputs.daily_sales: Write mode 'overwrite_partitions' requires 'partitionBy' or 'replace_where'.
```

The `partitionBy` form also runs on the pandas backend; `replace_where` needs Delta,
so Spark.

### `replace_where` on an append

On a Delta or JDBC `append`, `replace_where` names the rows of the run's batch, so a
rerun replaces the batch instead of adding it twice. Each connector page owns the
details: [Delta appends](../connectors/s3.md#appends-that-land-once-and-reruns-that-correct)
and [JDBC appends](../connectors/jdbc.md#a-batch-that-lands-once).

??? note "Under the hood"
    Checking a mode against what a connector supports is done once, the same way
    on every backend. Running `merge` and `overwrite_partitions` is Spark work
    (`MERGE INTO`, dynamic partition overwrite), done by the Spark backend.

    For `overwrite_partitions` the engine picks the mechanism the target actually
    supports: Delta's own write option for Delta, `INSERT OVERWRITE` for an existing
    table, and Spark's `partitionOverwriteMode=DYNAMIC` for a path. Each route is
    covered by an integration test against a real Spark session.
