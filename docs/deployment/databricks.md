# Deploy to Databricks

Deploy a pipeline task to Databricks as a job, from `config.yaml`
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
    # serverless: true is the default: the job gets no cluster block

  prod:
    host: "https://adb-prod-456.azuredatabricks.net"
    token_env: DATABRICKS_TOKEN_PROD
    workspace_path: /Workspace/ubunye
    serverless: false                   # run on a job cluster instead
    spark_version: "13.3.x-scala2.12"
    node_type_id: "i3.2xlarge"
    num_workers: 4
```

With `serverless: true` (the default) the job runs on serverless compute and
the cluster fields (`spark_version`, `node_type_id`, `num_workers`,
`spark_conf`, `aws_attributes`) are not used. Set `serverless: false` to get a
`new_cluster` built from them.

### Task-level overrides

A task can override specific fields by adding its own `targets.yaml`:

```yaml
# fraud_detection/ingestion/claim_etl/targets.yaml
targets:
  prod:
    num_workers: 8                     # this task needs more compute
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
4. Generates a wrapper notebook that installs the engine and calls
   `ubunye.run_task()`. If `transformations.py` imports Narwhals (one transform
   for every engine, see [Execution backends](../backends.md)), the notebook
   installs it too: the runtime brings Spark and pandas, not Narwhals.
5. Creates (or updates) a Databricks job named
   `ubunye-fraud_detection-ingestion-claim_etl-dev`

If the job already exists (same name), it is updated in place. Every Ubunye-managed
job is tagged with `ubunye_managed: "true"` (plus `ubunye_usecase`,
`ubunye_pipeline`, `ubunye_task` and `ubunye_target`) so it can be identified
later. If the task's `config.yaml` has `ORCHESTRATION.schedule` (a Quartz cron
expression), the job gets that schedule in UTC; without it the job has no
schedule and runs when you start it.

`deploy databricks` signs in with a personal access token read from the
`token_env` variable. For service principal (OAuth) sign-in in CI, see
[Databricks authentication](../databricks-auth.md).

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
| `serverless` | bool | `true` | Run on serverless compute; `false` builds a job cluster from the fields below |
| `spark_version` | str | `13.3.x-scala2.12` | Databricks Runtime version (job cluster only) |
| `node_type_id` | str | `i3.xlarge` | Instance type (job cluster only) |
| `num_workers` | int | none, sent as `0` | Number of workers, 0 = single node (job cluster only) |
| `spark_conf` | dict | `{}` | Spark configuration overrides (job cluster only) |
| `aws_attributes` | dict | `{}` | AWS-specific cluster attributes (job cluster only) |
| `max_concurrent_runs` | int | `1` | How many runs of the job may run at once |

Unknown fields are refused.

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
