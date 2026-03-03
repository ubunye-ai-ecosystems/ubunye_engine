# Orchestration

The `ORCHESTRATION` section provides metadata for exporting your task to an
orchestration platform. It does **not** affect how `ubunye run` executes the task —
it is used only by `ubunye export`.

---

## Structure

```yaml
ORCHESTRATION:
  type: airflow           # required — airflow | databricks | prefect | dagster
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
| `type` | `airflow` \| `databricks` \| `prefect` \| `dagster` | required | Target orchestration platform |
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
  transform:
    type: noop
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
