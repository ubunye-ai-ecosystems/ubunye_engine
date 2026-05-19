# Claude Code Prompt — Ubunye Python API + Deployment Pattern

Status: done (2026-05-19). Sanitized: client-specific references removed.

## Context

Ubunye Engine is a config-first, Spark-native ETL/ML framework with a Typer CLI (`ubunye run`). The CLI works well for terminals, GitHub Actions, and Jenkins. But on Databricks — the primary production environment — the CLI is problematic because:

1. Databricks already has a Spark session running. The CLI starts a NEW subprocess with a NEW Spark session, wasting resources.
2. Databricks doesn't natively run CLI commands. You have to use `!ubunye run` in notebooks or `subprocess.run()` in scripts, which are workarounds.
3. Other enterprise frameworks solve this by exposing a Python API that runs natively inside Databricks.

## What was implemented

### 1. Python API (`ubunye/api.py`)

Public Python API that wraps the existing engine:
- Reuses an existing Spark session if one is running (Databricks), or creates a new one if not
- Accepts the same parameters as the CLI: task_dir, mode, dt, task_list
- Supports running multiple tasks sequentially
- Returns the outputs map from the engine

```python
import ubunye

ubunye.run_task(
    task_dir="pipelines/my_usecase/my_pipeline/my_task",
    mode="nonprod",
    dt="202510"
)

ubunye.run_pipeline(
    usecase_dir="pipelines",
    usecase="my_usecase",
    package="my_pipeline",
    tasks=["feature_engineering", "scoring"],
    mode="nonprod",
    dt="202510"
)
```

### 2. Runner script pattern for Databricks

Python script that Databricks Workflow executes:
- Parses Databricks widget parameters
- Calls `ubunye.run_pipeline()` to run tasks sequentially
- Sets Unity Catalog and schema using the existing Spark session

### 3. Databricks Asset Bundle definition

Follows standard DAB conventions:
- Define the job as code (not manually in the UI)
- Two targets: nonprod and prod
- Cluster config with UNITY_CATALOG and UNITY_SCHEMA env vars

### 4. GitHub Actions deploy workflow

- On PR: validate configs (`ubunye validate`) + run unit tests
- On merge to main: deploy the Databricks Asset Bundle to nonprod
- No `ubunye run` on GitHub Actions — all execution happens on Databricks

### 5. CLAUDE.md, docs, and scaffolding

Project-level context, deployment guide, and `ubunye init` notebook scaffolding.

## Critical rules

1. DO NOT modify any existing Ubunye Engine files except `ubunye/__init__.py` (to add exports)
2. `ubunye/api.py` must reuse existing Spark sessions on Databricks
3. The runner script must use the Python API, not the CLI
4. GitHub Actions deploy step uses `databricks bundle deploy`, not `ubunye run`
