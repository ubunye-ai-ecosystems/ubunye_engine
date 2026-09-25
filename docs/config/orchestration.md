# Orchestration

The `ORCHESTRATION` section provides metadata for exporting your task to an
orchestration platform. It does **not** affect how `ubunye run` executes the task —
it is used only by `ubunye export`.

---

## Structure

```yaml
ORCHESTRATION:
  type: airflow           # required. Implemented today: airflow, databricks
  schedule: "0 2 * * *"  # cron expression
  retries: 3
  owner: data-engineering
  tags:
    - fraud
    - etl
  databricks:             # Databricks-specific cluster settings
    cluster_id: "0123-456789-abcde"
    node_type_id: "Standard_DS3_v2"
    num_workers: 4
```

---

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `airflow` or `databricks` | required | Target platform. `prefect` and `dagster` are accepted by the schema but have no exporter yet (see issue #29) |
| `schedule` | string | `null` | Cron expression for the DAG/workflow schedule |
| `retries` | int | `2` | Number of automatic retries on failure |
| `owner` | string | `null` | Team or person responsible (shown in Airflow UI) |
| `tags` | list of strings | `[]` | Labels for filtering in the orchestration UI |
| `databricks` | dict | `null` | Databricks-specific job cluster settings |

Extra fields are allowed and passed through to the relevant exporter.

---

## Airflow export

```bash
ubunye export airflow \
    -c pipelines/fraud/etl/claims/config.yaml \
    -o dags/claims_etl.py \
    --profile prod
```

The generated DAG contains a single `BashOperator` that runs:

```bash
ubunye run -d pipelines -u fraud -p etl -t claims --profile prod
```

Schedule, retries, owner, and tags are read from `ORCHESTRATION`.

---

## Databricks export

```bash
ubunye export databricks \
    -c pipelines/fraud/etl/claims/config.yaml \
    -o jobs/claims_etl.json \
    --profile prod
```

The generated JSON can be submitted with the Databricks CLI:

```bash
databricks jobs create --json-file jobs/claims_etl.json
databricks jobs run-now --job-id <ID>
```

Cluster settings from `ORCHESTRATION.databricks` are embedded in the job JSON.

---

## Example — full Airflow config

```yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    raw:
      format: hive
      db_name: raw
      tbl_name: claims
  transform: {}
  outputs:
    clean:
      format: delta
      path: s3://datalake/clean/claims
      mode: overwrite

ORCHESTRATION:
  type: airflow
  schedule: "30 1 * * *"    # 01:30 UTC daily
  retries: 2
  owner: fraud-team
  tags:
    - fraud
    - daily
    - etl
```


## Spark Declarative Pipelines (Spark 4.1+)

Spark 4.1 ships its own pipeline format. `ubunye export spark-pipeline` writes a
task in it, so the task can run where only Spark is installed, or be handed to a
team that standardised on it:

```bash
ubunye export spark-pipeline -c pipelines/shop/orders/clean/config.yaml -o clean-pipeline -dt 2026-07-13
cd clean-pipeline && spark-pipelines run      # needs pyspark[connect] 4.1+
```

It writes `spark-pipeline.yml`, a `transformations/` module and a copy of the task
in `task/`. Every input becomes a temporary view that reads what the config says,
the task's `transform()` runs unchanged, and every output becomes a materialized
view of the same name. The config is rendered at export time (`-dt`, `--var`, the
environment).

What does not carry over is reported when you export: a `merge` (or any
non-overwrite) mode becomes a full recompute, outputs go to the pipeline's catalog
rather than their paths, and `CONFIG.expectations` are not applied. A task that
uses `secret://` references is refused (the pipeline would hold them in plain
text). Supported readers: `s3` and `delta` paths, `binary` files, `hive` and
`unity` tables.

The run-anywhere example exported this way and run by `spark-pipelines` on Spark
4.2 wrote the same data, row hash for row hash, as Glue, Dataproc, Kubernetes and
Azure Container Apps.
