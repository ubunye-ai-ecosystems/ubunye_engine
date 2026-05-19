# Deploy to Databricks

Deploy a pipeline task to Databricks as a scheduled job — from `config.yaml`
to a running job in one command.

---

## Prerequisites

Install the engine with the Databricks extra:

```bash
pip install ubunye-engine[databricks]
```

Set your Databricks credentials:

```bash
export DATABRICKS_HOST="https://adb-123456789.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
```

---

## Create `targets.yaml`

Define your deploy targets in a `targets.yaml` file at the **usecase level**
(shared across all pipelines and tasks):

```
pipelines/
    fraud_detection/
        targets.yaml              ← usecase-level targets
        ingestion/
            claim_etl/
                config.yaml
                transformations.py
```

```yaml
# fraud_detection/targets.yaml
targets:
  dev:
    host: "https://adb-dev-123.azuredatabricks.net"
    token_env: DATABRICKS_TOKEN        # env var holding the PAT
    workspace_path: /Workspace/ubunye   # where files are uploaded
    spark_version: "13.3.x-scala2.12"
    node_type_id: "i3.xlarge"
    num_workers: 0                      # 0 = single-node cluster

  prod:
    host: "https://adb-prod-456.azuredatabricks.net"
    token_env: DATABRICKS_TOKEN_PROD
    workspace_path: /Workspace/ubunye
    node_type_id: "i3.2xlarge"
    num_workers: 4
```

### Task-level overrides

A task can override specific fields by adding its own `targets.yaml`:

```yaml
# fraud_detection/ingestion/claim_etl/targets.yaml
targets:
  dev:
    num_workers: 2                     # this task needs more compute
    spark_conf:
      spark.sql.shuffle.partitions: "200"
```

The task-level file is deep-merged on top of the usecase-level one. Fields
you don't override are inherited.

---

## Preview with `--dry-run`

```bash
ubunye deploy databricks \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --target dev --dry-run
```

Prints the job spec JSON without touching Databricks.

---

## Deploy

```bash
ubunye deploy databricks \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --target dev
```

This:

1. Validates the task `config.yaml`
2. Authenticates to the Databricks workspace
3. Uploads `config.yaml`, `transformations.py`, and helpers to
   `/Workspace/ubunye/fraud_detection/ingestion/claim_etl/`
4. Generates a wrapper notebook that calls `ubunye.run_task()`
5. Creates (or updates) a Databricks job named
   `ubunye-fraud_detection-ingestion-claim_etl-dev`

If the job already exists (same name), it is updated in place. Every Ubunye-managed
job is tagged with `ubunye_managed: "true"` so it can be identified later.

---

## Ad-hoc deploy (no targets.yaml)

For quick one-off deploys, pass the host directly:

```bash
ubunye deploy databricks \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --host "https://adb-123.azuredatabricks.net" \
    --dry-run
```

---

## Configuration reference

### `DatabricksTargetConfig` fields

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | str | **required** | Databricks workspace URL |
| `token_env` | str | `DATABRICKS_TOKEN` | Environment variable holding the PAT |
| `workspace_path` | str | `/Workspace/ubunye` | Root path for uploaded files |
| `spark_version` | str | `13.3.x-scala2.12` | Databricks Runtime version |
| `node_type_id` | str | `i3.xlarge` | Instance type for the job cluster |
| `num_workers` | int | `0` | Number of workers (0 = single-node) |
| `spark_conf` | dict | `{}` | Spark configuration overrides |
| `aws_attributes` | dict | `{}` | AWS-specific cluster attributes |

---

## CLI flags

```
ubunye deploy databricks --help
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--usecase-dir` | `-d` | required | Root directory of pipelines |
| `--usecase` | `-u` | required | Usecase name |
| `--package` | `-p` | required | Pipeline/package name |
| `--task` | `-t` | required | Task name |
| `--target` | | `dev` | Deploy target name |
| `--mode` | `-m` | `PROD` | Config profile for the job |
| `--data-timestamp` | `-dt` | | Data timestamp passed to the task |
| `--dry-run` | | `false` | Print job spec without deploying |
| `--host` | | | Ad-hoc workspace URL (skips targets.yaml) |
| `--token` | | | Env var name for the token (ad-hoc) |
