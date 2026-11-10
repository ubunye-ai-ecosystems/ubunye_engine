# Deploy to Databricks

This page shows how to run a task as a Databricks job. It is for anyone with a
Databricks workspace, from Free Edition to a paid workspace.

## Two ways to deploy, and which to pick

| | `ubunye deploy databricks` | Databricks Asset Bundles |
|---|---|---|
| What it is | One engine command that uploads one task and creates or updates its job | Jobs written as YAML files in your repo, deployed with the Databricks CLI (`databricks bundle deploy`) |
| Files you write | `targets.yaml` (where to deploy) | `databricks.yml`, job files, a notebook that calls the engine |
| Sign in | A personal access token only | A token, or a service principal with OAuth |
| Good for | Getting one task running fast; small teams | Several tasks in one job, reviewed job definitions, production CI |

Databricks Asset Bundles (often called DABs) are Databricks' own way to keep
jobs as code. A **personal access token** (PAT) is a password-like string tied
to one user. A **service principal** is a robot user for CI. See
[Databricks authentication](../databricks-auth.md) for both.

Both ways run the same task folder. The rest of this page covers
`ubunye deploy databricks` first, then [bundles](#databricks-asset-bundles).

---

## `ubunye deploy databricks`

**The problem:** you have a task that runs on your laptop and you want it as a
Databricks job.
**When to use it:** one task per job, signed in with a personal access token.
**The command:**

```bash
ubunye deploy databricks -d pipelines -u shop -p orders -t clean --target dev
```

### Before you start

1. Install the engine with the Databricks extra:

    ```bash
    pip install "ubunye-engine[databricks]"
    ```

2. Make a personal access token in the workspace (**User Settings → Developer →
   Access tokens → Generate new token**) and put it in an environment variable.
   The default name is `DATABRICKS_TOKEN`:

    ```bash
    export DATABRICKS_TOKEN="dapi..."
    ```

    You can use another name: set `token_env` in `targets.yaml`, or pass
    `--token MY_VAR_NAME`.

3. Tell the command which workspace to use. There are two ways:

    - a `targets.yaml` file (below), chosen with `--target`; or
    - `--host https://...` for a one-off deploy. With no `--host`, the
      `DATABRICKS_HOST` environment variable is used.

    A `targets.yaml`, when there is one, wins. With neither a `targets.yaml` nor
    a host, it stops with `No targets.yaml found and no --host flag provided.`

### Create `targets.yaml`

Put `targets.yaml` in the use case folder, so every pipeline and task in it
shares the same targets. This is the layout
`ubunye init -d pipelines -u shop -p orders -t clean` makes, with
`targets.yaml` added:

```
pipelines/
    shop/
        targets.yaml              ← use case level targets
        orders/
            clean/
                config.yaml
                transformations.py
```

```yaml
# pipelines/shop/targets.yaml
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
    spark_version: "17.3.x-scala2.13"
    node_type_id: "i3.2xlarge"
    num_workers: 4
```

**Serverless** compute means Databricks starts and stops the machines for you;
you pick no cluster. With `serverless: true` (the default) the cluster fields
(`spark_version`, `node_type_id`, `num_workers`, `spark_conf`,
`aws_attributes`) are not used. Set `serverless: false` to get a job cluster
built from them.

#### Task level overrides

A task can change some fields with its own `targets.yaml` in the task folder:

```yaml
# pipelines/shop/orders/clean/targets.yaml
targets:
  prod:
    num_workers: 8                     # this task needs more compute
    spark_conf:
      spark.sql.shuffle.partitions: "200"
```

The task file is merged on top of the use case file. Fields you don't set
are kept.

### Preview with `--dry-run`

```bash
ubunye deploy databricks -d pipelines -u shop -p orders -t clean \
    --target dev --dry-run
```

It prints the job spec and touches nothing in Databricks. Real output, with
the `dev` target above:

```
{
  "name": "ubunye-shop-orders-clean-dev",
  "tags": {
    "ubunye_managed": "true",
    "ubunye_usecase": "shop",
    "ubunye_pipeline": "orders",
    "ubunye_task": "clean",
    "ubunye_target": "dev"
  },
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "run_ubunye_task",
      "notebook_task": {
        "notebook_path": "/Workspace/ubunye/shop/orders/clean/run_clean"
      }
    }
  ]
}

[DRY RUN] Job spec for 'ubunye-shop-orders-clean-dev' printed above. No changes made to Databricks.
```

There is no cluster block, so the job runs on serverless compute.

### Deploy

```bash
ubunye deploy databricks -d pipelines -u shop -p orders -t clean --target dev
```

This:

1. Checks the task's `config.yaml`.
2. Signs in to the workspace with the token.
3. Uploads `config.yaml`, `transformations.py` and helper files to
   `/Workspace/ubunye/shop/orders/clean/`.
4. Writes a small notebook there, `run_clean`, that installs the engine and
   calls `ubunye.run_task()`. If `transformations.py` imports Narwhals (one
   transform for every engine, see [Execution backends](../backends.md)), the
   notebook installs it too: the runtime brings Spark and pandas, not Narwhals.
5. Creates the job `ubunye-shop-orders-clean-dev`, or updates it in place if a
   job with that name exists.

Every job it makes is tagged `ubunye_managed: "true"`, plus `ubunye_usecase`,
`ubunye_pipeline`, `ubunye_task` and `ubunye_target`, so you can find it later.

**Schedule.** If the task's `config.yaml` has `ORCHESTRATION.schedule`, the job
gets that schedule, in UTC. The value is a **Quartz cron** expression, the
format Databricks uses: it starts with seconds, and uses `?` for "any day".
For example `0 0 6 1 * ?` means 06:00 on the first day of every month. With no
schedule, the job runs only when you start it.

### What success looks like

The command prints one line:

```
[OK] Job 'ubunye-shop-orders-clean-dev' deployed (id=<job id>, target=dev)
```

Then start the job and look at it:

- **From the same command:** add `--run`. It starts a run once the job is
  deployed and prints the run's link.
- **In the workspace:** open **Jobs & Pipelines** (called **Workflows** in
  older workspaces), search for `ubunye-shop-orders-clean-dev`, and press
  **Run now**. The job page is also at `<host>/#job/<job id>`.
- **From a terminal**, with the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/):
  `databricks jobs run-now <job id>`.
- **The files** are under `/Workspace/ubunye/shop/orders/clean/` in the
  workspace browser, next to the `run_clean` notebook.

If the token variable is empty the command stops with `AuthNotFoundError`. A
token the workspace refuses gives `AuthInvalidError`. See [Errors](../errors.md).

### One-off deploy without `targets.yaml`

Pass the host directly, or set `DATABRICKS_HOST` (`--host` reads it). The host
is used only when there is no `targets.yaml`:

```bash
ubunye deploy databricks -d pipelines -u shop -p orders -t clean \
    --host "https://adb-123.azuredatabricks.net" --dry-run
```

The target name is then `dev`, and the job is `ubunye-shop-orders-clean-dev`.

### Deploy from GitHub Actions

`ubunye init github-actions` writes a workflow that runs this command:

```bash
ubunye init github-actions -d pipelines -u shop -p orders -t clean
```

It writes `.github/workflows/shop_orders.yml`. One job does everything: on a
pull request it checks the config and runs tests; on a push to `main` it also
deploys. The important parts, trimmed:

```yaml
jobs:
  test-and-deploy:
    runs-on: ubuntu-latest
    env:
      PIPELINE_DIR: pipelines/shop/orders
      DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
    steps:
      # ... checkout, Python 3.11, Java 17, pip install -e ".[spark,dev]"
      - name: Validate config
        run: ubunye validate -d pipelines -u shop -p orders --all
      # ... unit tests from pipelines/shop/orders/tests, if that folder exists
      # ... skip the deploy when DATABRICKS_HOST or DATABRICKS_TOKEN is empty
      - name: Deploy to Databricks
        if: ${{ steps.check_secrets.outputs.has_secrets == 'true' && github.event_name == 'push' && github.ref == 'refs/heads/main' }}
        run: |
          ubunye deploy databricks \
            -d pipelines -u shop -p orders -t clean \
            --target nonprod
      # ... "Run job (manual trigger)": the same command with --run
```

Before it can deploy:

- Add the secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` to the repository
  (**Settings → Secrets and variables → Actions**).
- That is all. With no `targets.yaml` the deploy reads the workspace from the
  `DATABRICKS_HOST` secret. A committed `targets.yaml` with a `nonprod` target
  (change it with `--target`) wins when there is one.
- To deploy and start the job by hand, run the workflow from the **Actions**
  tab with `run_after_deploy` ticked. The run step prints the run's link.

Run `ubunye init github-actions --help` for `--extras`, `--target`,
`--no-deploy` and `--output`.

### Configuration reference

#### `targets.yaml` fields

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | str | **required** | Databricks workspace URL |
| `token_env` | str | `DATABRICKS_TOKEN` | Environment variable holding the PAT |
| `workspace_path` | str | `/Workspace/ubunye` | Root path for uploaded files |
| `serverless` | bool | `true` | Run on serverless compute; `false` builds a job cluster from the fields below |
| `spark_version` | str | `17.3.x-scala2.13` | Databricks Runtime version (job cluster only); 17.3 LTS, as `ubunye export databricks` |
| `node_type_id` | str | `i3.xlarge` | Instance type (job cluster only) |
| `num_workers` | int | none, sent as `0` | Number of workers, 0 = single node (job cluster only) |
| `spark_conf` | dict | `{}` | Spark configuration overrides (job cluster only) |
| `aws_attributes` | dict | `{}` | AWS-specific cluster attributes (job cluster only) |
| `max_concurrent_runs` | int | `1` | How many runs of the job may run at once |

Unknown fields are refused.

#### CLI flags

`ubunye deploy databricks --help` lists them; the full table is in the
[CLI reference](../cli.md#deploy-databricks).

| Flag | Short | Default | Description |
|---|---|---|---|
| `--usecase-dir` | `-d` | required | The pipelines folder |
| `--usecase` | `-u` | required | Use case name |
| `--pipeline` | `-p` | required | Pipeline name |
| `--task` | `-t` | required | Task name |
| `--target` | | `dev` | Deploy target name |
| `--mode` | `-m` | `PROD` | Profile for the job |
| `--data-timestamp` | `-dt` | | Data timestamp passed to the task |
| `--dry-run` | | `false` | Print job spec without deploying |
| `--host` | | | One-off workspace URL (skips targets.yaml) |
| `--token` | | `DATABRICKS_TOKEN` | Env var name that holds the token |

---

## Databricks Asset Bundles

**The problem:** you want jobs reviewed as code, several tasks in one job, or
CI that signs in as a service principal.
**When to use it:** production on a paid workspace, or any team that already
uses bundles.
**The command:** `databricks bundle deploy --target nonprod`, run by the
[Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/).

Bundle files (`databricks.yml`, `bundles/`) belong in **your** repo, the one
that holds your pipelines, not in the engine repo. The engine is the runtime;
your repo defines the jobs.

A full working bundle is in the engine repo:
[`examples/production/titanic_databricks`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/titanic_databricks).
The snippets below follow it.

### The flow

```
Push code -> GitHub Actions checks it -> Merge to main -> GitHub Actions deploys the bundle -> Databricks runs it on schedule
```

| Stage | Where | What happens |
|---|---|---|
| Pull request | GitHub Actions | `ubunye validate --all` and `pytest` |
| Merge to main | GitHub Actions | `databricks bundle deploy --target nonprod` |
| Production deploy | GitHub Actions (started by hand) | `databricks bundle deploy --target prod` |
| Run | Databricks | The job runs the task on its schedule |

### `databricks.yml` (your repo's root)

It names the bundle, declares its variables and lists the targets:

```yaml
bundle:
  name: my-usecase-pipelines

include:
  - bundles/*.yaml

variables:
  catalog:
    description: Unity Catalog catalog the tasks write to.
    default: dev_catalog

targets:
  nonprod:
    mode: development
    default: true
    # No workspace.host here: the Databricks CLI reads it from DATABRICKS_HOST.

  prod:
    mode: production
    variables:
      catalog: prod_catalog
```

Two rules the Databricks CLI enforces:

- **Don't write `host: ${DATABRICKS_HOST}`.** The CLI refuses variables in
  sign-in fields ("Variable interpolation is not supported for fields that
  configure authentication"). Leave `workspace.host` out and set the
  `DATABRICKS_HOST` environment variable, or write the URL itself.
- **Declare every variable** under `variables:` before a target sets it.

**Unity Catalog** is Databricks' catalog of tables, volumes and who may use
them. A catalog holds schemas; a schema holds tables.

!!! note "Quoting catalog names"
    A catalog passed as a job parameter (`catalog: ${var.catalog}`) is often
    named with hyphens, like `aws-x-catalog`, and SQL needs those names in
    backticks. In `config.yaml`, pass every such name through `| ident`
    (`SELECT * FROM {{ catalog | ident }}.raw.trips`, with the parameter handed
    over as `variables={"catalog": ...}`). It quotes the name once,
    whether the parameter carries backticks or not. See
    [SQL names](../config/jinja.md#sql-names-ident).

### Job files (`bundles/*.yaml`)

Each file in `bundles/` defines jobs. The job runs a small notebook that calls
the engine:

```yaml
resources:
  jobs:
    monthly_orders:
      name: ubunye-monthly-orders
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: "0 0 6 1 * ?"   # 06:00 on day 1 of each month
        timezone_id: "UTC"
      tasks:
        - task_key: run_pipeline
          notebook_task:
            notebook_path: ./notebooks/run_task.py
            base_parameters:
              task_dir: ${workspace.file_path}/pipelines/shop/orders/clean
              mode: "PROD"
              catalog: ${var.catalog}
          # No cluster block: the task runs on serverless compute.
```

`${workspace.file_path}` is where the bundle uploads your repo's files. Add
`sync: include: [pipelines/**, notebooks/**]` to `databricks.yml` so the
pipelines and notebook travel with the bundle.

The notebook reads the parameters as widgets, installs the engine and runs the
task. A minimal version:

```python
# Databricks notebook source
# MAGIC %pip install "ubunye-engine==0.9.0"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("task_dir", "")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"])

import ubunye

ubunye.run_task(
    task_dir=dbutils.widgets.get("task_dir"),
    mode=dbutils.widgets.get("mode"),
)
```

The titanic example's
[`run_titanic.py`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/titanic_databricks/notebooks)
is a full one, using `ubunye.notebook()`.

### A GitHub Actions workflow for bundles

This workflow checks pull requests and deploys the bundle on merge. Unlike
the generated workflow above, it uses the Databricks CLI, so it can sign in
either way. Set **either** `DATABRICKS_TOKEN` **or** the pair
`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`; the CLI picks the
service principal when both of the pair are set. Which to choose, and how to
make them: [Databricks authentication](../databricks-auth.md).

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
      - run: ubunye validate -d ./pipelines -u shop -p orders --all
      - run: pytest tests/unit/ -v --tb=short -m "not integration"

  deploy-nonprod:
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST:          ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_TOKEN:         ${{ secrets.DATABRICKS_TOKEN }}
      DATABRICKS_CLIENT_ID:     ${{ secrets.DATABRICKS_CLIENT_ID }}
      DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target nonprod
```

| Sign-in | Secrets |
|---|---|
| Personal access token (Free Edition, one user) | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| Service principal with OAuth (recommended on paid workspaces) | `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` |

The engine repo's own bundle workflows (`.github/workflows/*_databricks*.yml`)
work this way.

### What success looks like

`databricks bundle deploy` logs `Uploading bundle files to /Workspace/Users/<you or the principal>/.bundle/...`
and ends with `Deployment complete!`. Then run the job with
`databricks bundle run monthly_orders --target nonprod`, or find
`ubunye-monthly-orders` under **Jobs & Pipelines** in the workspace. In
`mode: development`, Databricks puts `[dev <your name>]` in front of the job
name so two people sharing a workspace don't overwrite each other's jobs.

---

## The Python API on Databricks

In a notebook or a notebook job, call the engine from Python instead of the
CLI. It uses the Spark session Databricks already started:

```python
import ubunye

# One task
outputs = ubunye.run_task(
    task_dir="/Workspace/pipelines/fraud_detection/ingestion/claim_etl",
    mode="DEV",
    dt="202510",
)

# Several tasks in one pipeline
results = ubunye.run_pipeline(
    usecase_dir="/Workspace/pipelines",
    usecase="fraud_detection",
    pipeline="ingestion",
    tasks=["claim_etl", "feature_engineering"],
    mode="DEV",
    dt="202510",
)
```

It names the Spark app `ubunye:<usecase>.<pipeline>.<task>`, so you can find
the run in the Spark UI and history server.

!!! tip "Why the Python API on Databricks?"
    The Python API runs in the notebook's own process, uses the session
    Databricks already has running, and hands the output tables back to you.
    The CLI also uses a running session when there is one (`--backend` not
    given), but returns nothing you can inspect.

More on the Python API: [API reference](../api.md).

## Dev notebooks

`ubunye init` writes a dev notebook for each task, at
`<task folder>/notebooks/<task>_dev.ipynb`. It runs the same steps as the job,
one cell at a time:

1. **Parameters**: `dbutils.widgets` for `effective_year_month` and `mode`
2. **Setup**: installs the engine, loads the config, creates the Databricks backend
3. **Extract**: reads all inputs, prints each input's columns (no scan)
4. **Inspect sources**: `display()` each input table
5. **Transform**: runs the `Task` class from `transformations.py`
6. **Inspect outputs**: `display()` each output table
7. **Load**: writes outputs (commented out by default)
8. **Sandbox**: the Spark session, for free exploring

For the same steps with less code, use `ubunye.notebook()` (see the
[API reference](../api.md#notebook-api-interactive) and
[Notebooks](../patterns/notebook.md)).

The notebook reads the same `config.yaml` as the job, so dev and production
cannot drift apart.
