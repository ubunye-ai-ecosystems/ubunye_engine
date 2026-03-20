# Deployment

Ubunye pipelines run on Databricks via **Databricks Asset Bundles (DABs)** and are deployed through **GitHub Actions**.

!!! note "DABs belong in the usecase repo"
    Bundle definitions (`bundles/`, `databricks.yml`) are usecase-specific and live in the
    usecase repository, not in the Ubunye Engine repo. The engine provides the runtime;
    the usecase repo defines the jobs.

---

## Deployment flow

```
Push code → GitHub Actions validates → Merge to main → GitHub Actions deploys bundle → Databricks runs on schedule
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
      unity_catalog: "aws-db-nonprod-aic-catalog"

  prod:
    mode: production
    workspace:
      host: ${DATABRICKS_HOST}
    variables:
      mode: "prod"
      unity_catalog: "aws-db-prod-aic-catalog"
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
            parameters: ["run", "-d", "...", "-t", "..."]
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

| Secret | Description |
|---|---|
| `DATABRICKS_HOST` | Workspace URL (e.g. `https://adb-1234.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | Personal access token or service principal token |

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
      - run: pip install databricks-cli
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

!!! tip "Why not the CLI on Databricks?"
    The CLI creates a **new** SparkSession, wasting the one Databricks already has running.
    The Python API detects the active session and reuses it.

---

## Dev notebooks

`ubunye init` generates a dev notebook at `notebooks/<task>_dev.ipynb` for each task.
The notebook mirrors the production pipeline interactively:

1. **Parameters** — `dbutils.widgets` for `effective_year_month` and `mode`
2. **Setup** — loads config, creates `DatabricksBackend`
3. **Extract** — reads all inputs, prints row counts
4. **Inspect Sources** — `display()` each input DataFrame
5. **Transform** — runs the `Task` class from `transformations.py`
6. **Inspect Outputs** — `display()` each output DataFrame
7. **Load** — writes outputs (commented out by default)
8. **Sandbox** — Spark session exposed for free exploration

The notebook reads from the same `config.yaml` so there is zero drift between dev and production.
