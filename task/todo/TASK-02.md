# Claude Code — Ubunye Engine Tasks

## First step — before generating anything

Run these commands and capture the output:
```
ubunye --help
ubunye run --help
ubunye validate --help
ubunye init --help
ubunye plan --help
ubunye test --help
ubunye models --help
ubunye lineage --help
ubunye config --help
ubunye plugins
ubunye version
```

Use the real output to populate CLAUDE.md and MEMORY.md. Do not guess CLI flags — document exactly what the tool supports.

Also read these files before writing any code:
- `ubunye/backends/spark_backend.py` — understand how SparkSession is created
- `ubunye/core/runtime.py` — Engine class and EngineContext
- `ubunye/core/interfaces.py` — Task contract
- `ubunye/config/loader.py` — load_config function
- `ubunye/config/schema.py` — config schema and validation
- `ubunye/models/base.py` — UbunyeModel contract
- `ubunye/cli/main.py` — how CLI discovers and runs tasks, especially `_run_single_task()` and the `run` command

## Execution order

Follow this order strictly:
1. Read spark_backend.py first
2. Implement Python API (ubunye/api.py)
3. Updated deploy.yml
4. Docs (CLAUDE.md, MEMORY.md, TODO.md)
5. Notebook scaffolding in init (last)

## Task 1: Python API (`ubunye/api.py`)

Create a public Python API that wraps the existing engine. This is the core deliverable.

The API must:
- Check if a SparkSession already exists (Databricks) before creating a new one
- If a session exists, reuse it and do NOT stop it when done
- If no session exists, create one via SparkBackend and stop it when done
- Accept the same parameters as the CLI: task_dir, mode, dt
- Support running multiple tasks sequentially
- Use descriptive Spark app names: `ubunye:<usecase>.<package>.<task>`

Implementation pattern for session handling:

```python
from pyspark.sql import SparkSession
from ubunye.backends.spark_backend import SparkBackend

def _get_or_create_backend(usecase, package, task, conf=None):
    active = SparkSession.getActiveSession()
    app_name = f"ubunye:{usecase}.{package}.{task}"

    if active is not None:
        # Reuse existing session (Databricks, Jupyter, etc.)
        backend = SparkBackend(app_name=app_name, conf=conf)
        backend._spark = active
        return backend, False     # False = don't kill it
    else:
        # Create new session (local, Docker, CI)
        backend = SparkBackend(app_name=app_name, conf=conf)
        backend.start()
        return backend, True      # True = we own it, clean up after

def run_task(task_dir, mode="DEV", dt=None):
    """Run a single Ubunye task."""
    ...
    backend, we_created_it = _get_or_create_backend(usecase, package, task, conf)
    try:
        # run the task using existing _run_single_task logic
        ...
    finally:
        if we_created_it:
            backend.stop()

def run_pipeline(usecase_dir, usecase, package, tasks, mode="DEV", dt=None):
    """Run multiple tasks sequentially sharing one backend."""
    ...
```

Expose both functions in `ubunye/__init__.py`:
```python
from ubunye.api import run_task, run_pipeline
```

Do NOT create a separate DatabricksBackend class. The session detection happens inside the API only.

Look at `ubunye/cli/main.py` `_run_single_task()` (lines 250-340) and the `run` command (lines 343-437) — replicate that logic in the API without subprocess or CLI.

Check what `merged_spark_conf(mode)` does when mode doesn't match any profile key (e.g., passing "nonprod" when profiles are keyed on "DEV"/"PROD"). Document the behavior in MEMORY.md.

## Task 2: Updated `.github/workflows/deploy.yml`

The deploy workflow should:
- On PR to main: validate configs (`ubunye validate --all`) + run unit tests (`pytest`)
- On merge to main: deploy the Databricks Asset Bundle using `databricks bundle deploy --target nonprod`
- No `ubunye run` on GitHub Actions — ALL execution happens on Databricks
- Requires secrets: DATABRICKS_HOST, DATABRICKS_TOKEN
- Install Databricks CLI: `pip install databricks-cli`

## Task 3: CLAUDE.md (root level — MERGE with existing, do not replace)

Merge the following into the existing CLAUDE.md:
- Two entry points: CLI (`ubunye run`) for terminals, Python API (`ubunye.run_task()`) for Databricks
- Deployment pattern: GitHub Actions for CI (validate + test), Databricks Asset Bundles for CD (deploy + execute)
- Full CLI reference from the --help outputs captured in step 1
- Note that --mode/-m defaults to "DEV", -dt is for data timestamps
- Note there is no --profile flag
- Spark app naming convention: `ubunye:<usecase>.<package>.<task>`

## Task 4: MEMORY.md (root level)

Key decisions and learnings:
- Ubunye is CLI-first but needs Python API for Databricks (CLI creates new Spark session, wastes the one Databricks already has)
- Python API detects existing SparkSession via getActiveSession(). If exists, reuse and don't stop. If not, create and stop when done.
- No separate DatabricksBackend needed — session detection handled in api.py
- GitHub Actions cannot run `ubunye run` against Unity Catalog — local Spark doesn't support multi-part namespaces. CI is validation only.
- Unity Catalog names with hyphens (aws-db-nonprod-aic-catalog) need backticks in SQL
- --mode/-m is the CLI flag for environment switching, not --profile. Default is "DEV"
- -dt/--data-timestamp is how you pass effective_year_month. Injected as {{ dt }} in Jinja
- Config uses format: unity with sql: for Unity Catalog inputs
- Databricks Asset Bundles (DABs) is the deployment mechanism — jobs defined as code in the usecase repo, not in the engine
- Deployment flow: push code → GitHub Actions validates → merge to main → GitHub Actions deploys bundle → Databricks runs on schedule
- Spark app name follows convention: ubunye:<usecase>.<package>.<task> for easy identification in Spark UI
- Document what merged_spark_conf(mode) returns when mode doesn't match any profile key

## Task 5: TODO.md (root level)

### Plugins (no engine changes — register as entry points)
- [ ] Feature store connector — register as `ubunye.readers.feature_store` and `ubunye.writers.feature_store`. Same plugin system as Hive, Delta, etc. Reads/writes to Databricks Feature Store or any feature store backend.
- [ ] Drift detection transform — register as `ubunye.transforms.drift`. Takes two DataFrames (reference + current), outputs drift metrics (PSI, KS, mean shift). Just another transform plugin.
- [ ] Model sync transform — register as `ubunye.transforms.model_sync`. Copies models between environments (nonprod → prod registry). Transform plugin.

### New CLI commands
- [ ] `ubunye deploy` command — new CLI command in `cli/`. Reads pipeline YAML (e.g., rewards_pipeline.yaml), generates Databricks Asset Bundle definition, and deploys via Databricks CLI. Eliminates manual job creation in Databricks UI.

### Repo hygiene
- [ ] Pre-commit hooks — add `.pre-commit-config.yaml` with black, ruff, mypy, yaml-lint. Repo config, not engine code.
- [ ] Fix CI unit tests — currently failing. Get all 288 tests passing in GitHub Actions.
- [ ] Migrate `setup.py` → `pyproject.toml` — single source of truth for packaging, dependencies, and entry points.

### Documentation
- [ ] Add deployment guide to docs (`docs/getting_started/deployment.md`) — covers CLI usage, Python API, Databricks deployment with DABs, GitHub Actions CI pattern.
- [ ] Add end-to-end example using Python API on Databricks.
- [ ] Support `USE CATALOG` / `USE SCHEMA` in config.yaml ENGINE section natively.

## Task 6: Notebook scaffolding in `ubunye init`

Rule #1 exception: You MAY modify ubunye/cli/main.py ONLY to add notebook scaffolding to the init command. Do not change any other existing files.

When `ubunye init` creates a new task, it currently generates config.yaml and transformations.py. Update it to also generate `notebooks/<task>_dev.ipynb`.

The dev notebook should use Databricks magic functions for a native experience:

Cell structure:

```
%md ## Parameters
→ dbutils.widgets for parameters (effective_year_month, mode)

%md ## Setup
→ %pip install ubunye-engine
→ Load config using Python API, print summary of inputs/outputs

%md ## Extract
→ Read all inputs using Ubunye readers
→ Print row counts for each input

%md ## Inspect Sources
→ display() each input DataFrame

%sql (optional validation query)

%md ## Transform
→ Import and run the Task class from transformations.py
→ Print output keys

%md ## Inspect Outputs
→ display() each output DataFrame with row counts

%md ## Load (disabled by default)
→ Write outputs using Ubunye writers
→ ALL CODE IN THIS CELL MUST BE COMMENTED OUT so DS don't accidentally write to prod

%md ## Sandbox
→ Spark session exposed for free exploration
→ spark = SparkSession.getActiveSession()
```

The notebook must:
- Use the Python API (not CLI, not subprocess)
- Use display() instead of .show()
- Use dbutils.widgets for parameterization
- Use %md cells for markdown between steps
- Use %sql cells for quick data validation
- Read from the same config.yaml so there's zero drift between dev and production
- Have the Load cell commented out by default

Look at ubunye/cli/main.py lines 57-111 where init currently scaffolds files. Add notebook generation following the same pattern.

## Critical rules

1. DO NOT modify any existing Ubunye Engine files EXCEPT:
   - ubunye/__init__.py (to add exports for run_task, run_pipeline)
   - ubunye/cli/main.py (ONLY to add notebook scaffolding to init command)
2. No separate DatabricksBackend class — session detection happens in api.py
3. GitHub Actions deploys bundles, does NOT run ubunye run
4. The pipelines-activate/ directory already exists — do NOT generate or modify anything inside it
5. CLAUDE.md should be merged with existing content, not replaced
6. All CLI flags and defaults must come from actual --help output, not assumptions
7. Spark app name must follow: ubunye:<usecase>.<package>.<task>
8. DABs and runner scripts are usecase-specific — they belong in the usecase repo, not in the engine