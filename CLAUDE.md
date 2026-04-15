# Ubunye Engine — Developer Guide

## Overview
Config-driven, Spark-native framework for building and deploying ML/ETL pipelines.
Users write the same code in notebooks and production without worrying about infrastructure.

## Quick Start
```bash
pip install -e .                # install in dev mode
ubunye version                  # v0.1.0
ubunye plugins                  # list discovered plugins
```

## CLI Reference

### `ubunye` (top-level)
Commands: `init`, `plugins`, `config`, `validate`, `plan`, `run`, `version`, `lineage`, `models`, `test`

---

### `ubunye init`
Scaffold task folders with `config.yaml` and `transformations.py`.

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--usecase-dir` | `-d` | yes | — | Root directory for the use case |
| `--usecase` | `-u` | yes | — | Use case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) to scaffold (repeatable) |
| `--overwrite` | | no | `no-overwrite` | Overwrite existing files |

---

### `ubunye validate`
Validate config file(s) without executing the pipeline. Runs Pydantic + Jinja resolution.

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--usecase-dir` | `-d` | yes | — | Root directory of pipelines |
| `--usecase` | `-u` | yes | — | Use case name |
| `--package` | `-p` | yes | — | Pipeline/package name |
| `--task-list` | `-t` | no | — | Task(s) to validate (repeatable) |
| `--all` | | no | false | Validate all tasks in the package |
| `--profile` | | no | — | Profile to validate against (e.g. dev, prod) |
| `--data-timestamp` | `-dt` | no | — | Data timestamp for Jinja rendering |

**Examples:**
```bash
ubunye validate -d ./pipelines -u fraud_detection -p ingestion -t claim_etl
ubunye validate -d ./pipelines -u fraud_detection -p ingestion --all
ubunye validate -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --profile dev
```

---

### `ubunye config`
Show and validate config files (similar to validate but also resolves Spark conf).

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--usecase-dir` | `-d` | yes | — | Root directory |
| `--usecase` | `-u` | yes | — | Use case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) (repeatable) |
| `--data-timestamp` | `-dt` | no | — | Data timestamp |
| `--data-timestamp-format` | `-dtf` | no | — | Timestamp format |
| `--mode` | `-m` | no | `DEV` | Run mode |

---

### `ubunye plan`
Print the planned inputs → transform → outputs for task(s).

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--usecase-dir` | `-d` | yes | — | Root directory |
| `--usecase` | `-u` | yes | — | Use case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) (repeatable) |
| `--data-timestamp` | `-dt` | no | — | Data timestamp |
| `--data-timestamp-format` | `-dtf` | no | — | Timestamp format |
| `--mode` | `-m` | no | `DEV` | Run mode |

---

### `ubunye run`
Run one or more tasks within a package sequentially.

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--usecase-dir` | `-d` | yes | — | Root directory for the use case |
| `--usecase` | `-u` | yes | — | Use case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) to execute (repeatable) |
| `--data-timestamp` | `-dt` | no | — | Data timestamp for Jinja rendering |
| `--data-timestamp-format` | `-dtf` | no | — | Timestamp format |
| `--mode` | `-m` | no | `DEV` | Run mode (DEV/PROD) |
| `--deploy-mode` | | no | `client` | Spark deploy mode (cluster/client) |
| `--lineage` | | no | false | Record lineage for this run |
| `--lineage-dir` | | no | `.ubunye/lineage` | Root directory for lineage records |

**Example:**
```bash
ubunye run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl -m PROD --lineage
```

---

### `ubunye test run`
Run one or more tasks with a test profile and report PASS/FAIL per task.

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--usecase-dir` | `-d` | yes | — | Root directory of pipelines |
| `--usecase` | `-u` | yes | — | Use case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) to test (repeatable) |
| `--profile` | | no | `test` | Config profile to use |
| `--data-timestamp` | `-dt` | no | — | Data timestamp |
| `--lineage / --no-lineage` | | no | `lineage` | Record lineage for each test run |
| `--lineage-dir` | | no | `.ubunye/lineage` | Lineage directory |

**Example:**
```bash
ubunye test run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl
```

---

### `ubunye lineage`
Sub-commands: `show`, `list`, `compare`, `search`, `trace`

#### `ubunye lineage show`
Show a run record as formatted JSON (latest or specific run).

| Flag | Short | Required | Default |
|------|-------|----------|---------|
| `--usecase-dir` | `-d` | yes | — |
| `--usecase` | `-u` | yes | — |
| `--package` | `-p` | yes | — |
| `--task` | `-t` | yes | — |
| `--run-id` | | no | latest |
| `--lineage-dir` | | no | `.ubunye/lineage` |

#### `ubunye lineage list`
List recent runs for a task (newest first).

| Flag | Short | Required | Default |
|------|-------|----------|---------|
| `--usecase-dir` | `-d` | yes | — |
| `--usecase` | `-u` | yes | — |
| `--package` | `-p` | yes | — |
| `--task` | `-t` | yes | — |
| `--n` | `-n` | no | `10` |
| `--lineage-dir` | | no | `.ubunye/lineage` |

#### `ubunye lineage compare`
Diff two run records — highlight changes in hashes, row counts, and status.

| Flag | Short | Required | Default |
|------|-------|----------|---------|
| `--usecase-dir` | `-d` | yes | — |
| `--usecase` | `-u` | yes | — |
| `--package` | `-p` | yes | — |
| `--task` | `-t` | yes | — |
| `--run-id1` | | yes | — |
| `--run-id2` | | yes | — |
| `--lineage-dir` | | no | `.ubunye/lineage` |

#### `ubunye lineage search`
Search all recorded runs across tasks with optional filters.

| Flag | Short | Required | Default |
|------|-------|----------|---------|
| `--usecase-dir` | `-d` | yes | — |
| `--task` | `-t` | no | — |
| `--usecase` | `-u` | no | — |
| `--package` | `-p` | no | — |
| `--status` | | no | — |
| `--since` | | no | — |
| `--lineage-dir` | | no | `.ubunye/lineage` |

#### `ubunye lineage trace`
Print the input → transform → output data flow graph for a run.

| Flag | Short | Required | Default |
|------|-------|----------|---------|
| `--usecase-dir` | `-d` | yes | — |
| `--usecase` | `-u` | yes | — |
| `--package` | `-p` | yes | — |
| `--task` | `-t` | yes | — |
| `--run-id` | | no | latest |
| `--lineage-dir` | | no | `.ubunye/lineage` |

---

### `ubunye models`
Sub-commands: `list`, `info`, `promote`, `demote`, `rollback`, `archive`, `compare`

All model sub-commands require: `--use-case` (`-u`), `--model` (`-m`), `--store` (`-s`).

#### `ubunye models list`
List all registered versions for a model (newest first).

#### `ubunye models info`
Show full details of a specific model version as JSON. Requires `--version` (`-v`).

#### `ubunye models promote`
Promote a model version to a higher lifecycle stage.
Requires `--version` (`-v`), `--to` (staging | production). Optional `--promoted-by`.

#### `ubunye models demote`
Demote a model version to a lower lifecycle stage.
Requires `--version` (`-v`), `--to` (development | staging | archived).

#### `ubunye models rollback`
Roll back production to a specific previous version. Requires `--version` (`-v`).

#### `ubunye models archive`
Archive a model version. Requires `--version` (`-v`).

#### `ubunye models compare`
Compare metrics between two model versions. Requires `--versions` (two version strings, repeatable).

---

### `ubunye plugins`
List discovered Reader/Writer/Transform plugins. No flags.

**Current plugins:**
- Readers: `hive`, `jdbc`, `rest_api`, `s3`, `unity`
- Writers: `jdbc`, `rest_api`, `s3`, `unity`
- Transforms: `model`, `noop`

### `ubunye version`
Print version string. No flags. Currently: `v0.1.0`.

## Entry Points

Ubunye has two entry points:

### CLI (`ubunye run`) — for terminals, CI, Jenkins
```bash
ubunye run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl -m PROD --lineage
```

### Python API (`ubunye.run_task()`) — for Databricks notebooks and jobs
```python
import ubunye

# Single task
outputs = ubunye.run_task(
    task_dir="pipelines/fraud_detection/ingestion/claim_etl",
    mode="nonprod",
    dt="202510",
)

# Multiple tasks
results = ubunye.run_pipeline(
    usecase_dir="pipelines",
    usecase="fraud_detection",
    package="ingestion",
    tasks=["claim_etl", "feature_engineering"],
    mode="nonprod",
    dt="202510",
)
```

The Python API auto-detects an active SparkSession (Databricks) and reuses it
instead of creating a new one. Pass `spark=` explicitly to override.

## Deployment Pattern

- **CI (GitHub Actions)**: On PR → validate configs + unit tests. On merge to main → deploy Databricks Asset Bundle to nonprod.
- **CD (Databricks Asset Bundles)**: Jobs defined as code in the **usecase repo** (not this engine repo). Deployed via `databricks bundle deploy --target <nonprod|prod>`.
- **Execution**: All pipeline execution happens on Databricks, not in GitHub Actions. CI is validation-only.

DABs (`bundles/`, `databricks.yml`) belong in the usecase repo, not in the engine.

## Source Layout
- `ubunye/api.py` — Public Python API (`run_task`, `run_pipeline`)
- `ubunye/core/` — Engine, Registry, interfaces (Reader/Writer/Transform/Task/Backend), `hooks.py` (Hook / HookChain observability abstraction)
- `ubunye/config/` — YAML loader + Pydantic v2 schema
- `ubunye/backends/spark_backend.py` — SparkSession lifecycle (creates new session)
- `ubunye/backends/databricks_backend.py` — Reuses active SparkSession (Databricks)
- `ubunye/plugins/readers/` — Hive, JDBC, Unity Catalog, REST API, S3 readers
- `ubunye/plugins/writers/` — S3, JDBC, Unity Catalog, REST API writers
- `ubunye/plugins/transforms/` — noop, model transforms
- `ubunye/plugins/ml/` — BaseModel, SklearnModel, SparkMLModel, BatchPredictMixin, MLflowLoggingMixin
- `ubunye/cli/main.py` — Typer CLI entry point
- `ubunye/cli/lineage.py` — lineage sub-commands
- `ubunye/cli/models.py` — models sub-commands
- `ubunye/cli/test_cmd.py` — test sub-commands
- `ubunye/lineage/` — run provenance: RunContext, StepRecord, LineageRecorder, FileSystemLineageStore
- `ubunye/telemetry/` — events, mlflow, prometheus, otel, monitors
- `ubunye/telemetry/hooks/` — built-in Hook implementations (EventLoggerHook, OTelHook, PrometheusHook, LegacyMonitorsHook); registered via `ubunye.hooks` entry-point group
- `ubunye/orchestration/` — Airflow/Databricks exporters
- `pipelines/` — example pipeline tasks
- `examples/` — fraud_detection, rest_api examples

## Running Tests
```bash
pytest                           # run full test suite
pytest tests/ -k "test_config"   # run specific tests
```

## Key Conventions
- Timestamps are passed via `-dt` (short for `--data-timestamp`), not `--dt`
- Mode defaults to `DEV` (uppercase), not "nonprod" or "dev"
- Deploy mode defaults to `client`
- `validate` has `--all` flag to validate all tasks in a package; `run` does not
- `validate` has `--profile` flag; `run` uses `-m/--mode` instead
- `test run` defaults profile to `test` and lineage is ON by default
- `lineage` sub-commands use `--task` (`-t` singular), not `--task-list`
- `models` sub-commands use `--use-case` (`-u`), `--model` (`-m`), `--store` (`-s`)
- `load_config()` accepts a directory path (for validate) or a file path (for run/plan/config)
- Task code lives in `transformations.py` inside each task directory
- Config lives in `config.yaml` inside each task directory
- Spark app name convention: `ubunye:<usecase>.<package>.<task>` (set by the Python API for easy identification in Spark UI)
- `merged_spark_conf(mode)` silently returns base config when mode doesn't match any profile key (no error)
- Folder convention: `<usecase_dir>/<usecase>/<package>/<task>/config.yaml`
- Config top-level keys: `MODEL` (etl/ml), `VERSION` (semver), `ENGINE` (spark_conf + profiles), `CONFIG` (inputs/transform/outputs)
- Jinja variables: `{{ dt }}`, `{{ dtf }}`, `{{ mode }}`, `{{ env.VAR_NAME }}`
