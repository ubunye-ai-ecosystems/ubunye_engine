# Deployment

Deployment means one thing here: something launches your task on a schedule, in
an environment, with credentials. The task folder itself never changes. This
page is the map; each environment has its own page with the exact steps.

## Where you can deploy today

| Environment | Command | Status | Page |
|---|---|---|---|
| Databricks | `ubunye deploy databricks`, or bundles | Proven end to end in the [proving ground](proving-ground/latest.md), runs in CI | [Databricks](deployment/databricks.md) |
| AWS Glue 5 | `ubunye deploy glue` | Proven in the proving ground | [One command to the cloud](deployment/serverless.md) |
| GCP Dataproc Serverless | `ubunye deploy dataproc` | Proven in the proving ground | [One command to the cloud](deployment/serverless.md) |
| Azure Container Apps | `ubunye deploy container-apps` | Proven in the proving ground | [One command to the cloud](deployment/serverless.md) |
| Kubernetes | `ubunye deploy k8s` | Proven in the proving ground (kind) | [Kubernetes](deployment/kubernetes.md) |
| Docker | `ubunye deploy dockerfile` | Working images, the base for Kubernetes and the clouds | [One command to the cloud](deployment/serverless.md) |
| AWS EMR Serverless | `ubunye deploy emr-serverless` | Tested as a plan only: a free plan AWS account cannot use EMR | [AWS](deployment/aws.md) |
| Anything with spark-submit | `python -m ubunye` | Supported | [Anywhere](deployment/anywhere.md) |

One rule applies everywhere and is worth learning once: the task never chooses
its own cluster. Leave `spark.master` out of your config. Whoever launches the
job owns that decision, and the engine refuses a config that tries to override
a platform's choice, because a silent single machine run on paid compute is
worse than an error.

---

## The Databricks flow in detail

Ubunye pipelines run on Databricks via **Databricks Asset Bundles (DABs)** and are deployed through **GitHub Actions**.
For a single task, `ubunye deploy databricks` creates or updates the job directly (see the
[CLI reference](cli.md#deploy-databricks)), and `ubunye init github-actions` writes a
workflow like the one below.

!!! note "DABs belong in the usecase repo"
    Bundle definitions (`bundles/`, `databricks.yml`) are usecase-specific and live in the
    usecase repository, not in the Ubunye Engine repo. The engine provides the runtime;
    the usecase repo defines the jobs.

---

## Deployment flow

```
Push code -> GitHub Actions validates -> Merge to main -> GitHub Actions deploys bundle -> Databricks runs on schedule
```

| Stage | Where | What happens |
|---|---|---|
| PR | GitHub Actions | `ubunye validate --all` + `pytest` |
| Merge to main | GitHub Actions | `databricks bundle deploy --target nonprod` |
| Production deploy | GitHub Actions (manual) | `databricks bundle deploy --target prod` |
| Execution | Databricks | Scheduled job runs the pipeline |

---

## Databricks Asset Bundles

Jobs are defined as code in the usecase repo.

### `databricks.yml` (usecase repo root)

Defines bundle name and deployment targets:

```yaml
bundle:
  name: "my-usecase-pipelines"

include:
  - "bundles/*.yaml"

targets:
  nonprod:
    mode: development
    default: true
    workspace:
      host: ${DATABRICKS_HOST}
    variables:
      mode: "nonprod"
      unity_catalog: "aws-db-nonprod-tl-catalog"

  prod:
    mode: production
    workspace:
      host: ${DATABRICKS_HOST}
    variables:
      mode: "prod"
      unity_catalog: "aws-db-prod-tl-catalog"
```

### Job definitions (`bundles/*.yaml`)

Each YAML file in `bundles/` defines a Databricks job:

```yaml
resources:
  jobs:
    monthly_rewards:
      name: "ubunye-monthly-rewards"
      schedule:
        quartz_cron_expression: "0 0 6 1 * ?"
        timezone_id: "UTC"
      tasks:
        - task_key: "run_pipeline"
          python_wheel_task:
            package_name: "ubunye_engine"
            entry_point: "ubunye"
            parameters: ["run", "-d", "...", "-u", "...", "-p", "...", "-t", "...", "-m", "PROD"]
```

---

## GitHub Actions workflow

The deploy workflow (`.github/workflows/deploy.yml` in the usecase repo) handles CI and CD:

**On pull request:**

- Validates pipeline configs with `ubunye validate`
- Runs unit tests with `pytest`

**On merge to main:**

- Deploys the Databricks Asset Bundle to nonprod

### Required secrets

Two auth flows are supported; pick one per workspace. See
[Databricks Authentication](databricks-auth.md) for the full setup.

| Flow | Secrets |
|---|---|
| PAT (Free Edition / single-user) | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| Service principal + OAuth *(recommended, paid workspaces)* | `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` |

The Databricks CLI auto-selects OAuth when `DATABRICKS_CLIENT_ID` and
`DATABRICKS_CLIENT_SECRET` are both set; otherwise it falls back to PAT.

### Example workflow

```yaml
name: deploy
on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  validate:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e .[dev]
      - run: ubunye validate -d ./pipelines -u my_usecase -p my_package --all
      - run: pytest tests/unit/ -v --tb=short -m "not integration"

  deploy-nonprod:
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target nonprod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

---

## Python API on Databricks

On Databricks, use the Python API instead of the CLI. The API reuses the active SparkSession:

```python
import ubunye

# Single task
outputs = ubunye.run_task(
    task_dir="/Workspace/pipelines/fraud_detection/ingestion/claim_etl",
    mode="nonprod",
    dt="202510",
)

# Multiple tasks
results = ubunye.run_pipeline(
    usecase_dir="/Workspace/pipelines",
    usecase="fraud_detection",
    package="ingestion",
    tasks=["claim_etl", "feature_engineering"],
    mode="nonprod",
    dt="202510",
)
```

The Python API sets descriptive Spark app names (`ubunye:<usecase>.<package>.<task>`) for easy
identification in the Spark UI and history server.

!!! tip "Why the Python API on Databricks?"
    The Python API runs in the notebook's own process, attaches to the session
    Databricks already has running, and hands the output frames back to you.
    The CLI also uses a running session when there is one (`--backend` not
    given), but returns nothing you can inspect.

---

## Dev notebooks

`ubunye init` generates a dev notebook at `notebooks/<task>_dev.ipynb` for each task.
The notebook mirrors the production pipeline interactively:

1. **Parameters**: `dbutils.widgets` for `effective_year_month` and `mode`
2. **Setup**: installs the engine, loads config, creates `DatabricksBackend`
3. **Extract**: reads all inputs, prints each input's columns (no scan)
4. **Inspect Sources**: `display()` each input DataFrame
5. **Transform**: runs the `Task` class from `transformations.py`
6. **Inspect Outputs**: `display()` each output DataFrame
7. **Load**: writes outputs (commented out by default)
8. **Sandbox**: Spark session exposed for free exploration

For the same steps with less code, use `ubunye.notebook()` (see the
[API reference](api.md#notebook-api-interactive)).

The notebook reads from the same `config.yaml` so there is zero drift between dev and production.
