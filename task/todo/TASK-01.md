# Claude Code Prompt — Ubunye Python API + Deployment Pattern

## Context

Ubunye Engine is a config-first, Spark-native ETL/ML framework with a Typer CLI (`ubunye run`). The CLI works well for terminals, GitHub Actions, and Jenkins. But on Databricks — the primary production environment — the CLI is problematic because:

1. Databricks already has a Spark session running. The CLI starts a NEW subprocess with a NEW Spark session, wasting resources.
2. Databricks doesn't natively run CLI commands. You have to use `!ubunye run` in notebooks or `subprocess.run()` in scripts, which are workarounds.
3. Other frameworks in the ABSA ecosystem (like Rialto from CPDS) solve this by exposing a Python API that runs natively inside Databricks.

## What to implement

### 1. Python API (`ubunye/api.py`)

Create a public Python API that wraps the existing engine. Do NOT change any existing Ubunye Engine code — this is a new file that imports and uses existing components.

The API should:
- Reuse an existing Spark session if one is running (Databricks), or create a new one if not
- Accept the same parameters as the CLI: task_dir, mode, dt, task_list
- Support running multiple tasks sequentially (like `-t feature_engineering -t reward_scoring`)
- Return the outputs map from the engine

```python
# Usage on Databricks:
import ubunye

ubunye.run_task(
    task_dir="pipelines-activate/smart_telematics_usecases/monthly_rewards/feature_engineering",
    mode="nonprod",
    dt="202510"
)

# Or run multiple tasks:
ubunye.run_pipeline(
    usecase_dir="pipelines-activate",
    usecase="smart_telematics_usecases",
    package="monthly_rewards",
    tasks=["feature_engineering", "reward_scoring"],
    mode="nonprod",
    dt="202510"
)
```

Look at these existing files to understand how the engine works internally:
- `ubunye/core/runtime.py` — `Engine` class and `EngineContext`
- `ubunye/core/interfaces.py` — `Task` contract
- `ubunye/backends/spark_backend.py` — `SparkBackend`
- `ubunye/config/loader.py` — `load_config` function
- `ubunye/cli/main.py` — `_run_single_task` function and `run` command (this is what the API should replicate without subprocess)

Important: The `SparkBackend` creates a new SparkSession. On Databricks, a session already exists. The API should detect if a SparkSession is already active and reuse it instead of creating a new one.

Also expose `run_task` and `run_pipeline` in `ubunye/__init__.py` so users can do `import ubunye; ubunye.run_task(...)`.

### 2. Runner script for Databricks (`pipelines-activate/run_monthly_rewards.py`)

A Python script that Databricks Workflow will execute. It:
- Parses Databricks widget parameters (effective_year_month, mode)
- Calls `ubunye.run_pipeline()` to run both tasks sequentially
- Sets the Unity Catalog and schema using the existing Spark session before running

```python
# This script is called by a Databricks Workflow task
# No CLI, no subprocess, native Python
```

### 3. Databricks Asset Bundle definition (`bundles/monthly_rewards.yaml`)

Following the ABSA CPDS pattern for Databricks Asset Bundles:
- Define the job as code (not manually in the UI)
- Two targets: nonprod and prod
- Schedule: 1st of every month at 06:00 UTC
- Task runs the runner script
- Cluster config with UNITY_CATALOG and UNITY_SCHEMA env vars
- nonprod catalog: `aws-db-nonprod-aic-catalog`
- prod catalog: `aws-db-prod-aic-catalog`
- schema: `default` for both

### 4. `databricks.yml` (root level)

The Databricks bundle config that references the job definition and defines targets (nonprod workspace, prod workspace).

### 5. Updated `deploy.yml` for GitHub Actions

The deploy workflow should:
- On PR: validate configs (`ubunye validate`) + run unit tests
- On merge to main: deploy the Databricks Asset Bundle to nonprod using `databricks bundle deploy --target nonprod`
- No `ubunye run` on GitHub Actions — all execution happens on Databricks

Requires secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
Requires Databricks CLI installed: `pip install databricks-cli`

### 6. CLAUDE.md (root level)

Project-level context about Ubunye Engine. Include:
- What Ubunye Engine is (config-first, Spark-native ETL/ML framework)
- Two entry points: CLI (`ubunye run`) for terminals, Python API (`ubunye.run_task()`) for Databricks
- Core architecture: Task interface, config schema, readers/writers/transforms
- How the CLI discovers and runs tasks
- The UbunyeModel contract
- Config conventions: MODEL, VERSION, ENGINE, CONFIG, Jinja templating
- Folder convention: `<dir>/<usecase>/<package>/<task>/config.yaml`
- Deployment pattern: GitHub Actions for CI (validate + test), Databricks Asset Bundles for CD (deploy + execute)
- CLI flags: `-d` usecase-dir, `-u` usecase, `-p` package, `-t` task-list, `-m` mode (default DEV), `-dt` data-timestamp

### 7. CLAUDE.md (pipelines-activate/smart_telematics_usecases/)

Domain context for Activate telematics. Include:
- What Activate is (ABSA digital insurance, telematics, up to 40% cashback for good driving)
- Monthly rewards pipeline: feature_engineering → reward_scoring
- Six Unity Catalog source tables with descriptions
- Key fields: sentiance_id, imei_number, policy_number, item_no, effective_year_month
- Trip scoring features: hard_accel, hard_brake, legal, overall_absa_v1, distance, trip_duration, late_drive_duration
- Environment switching: nonprod = aws-db-nonprod-aic-catalog, prod = aws-db-prod-aic-catalog, schema = default
- Business rules: policy_status/cover_status must be "Policy", 3-month maturation window

### 8. MEMORY.md (root level)

A file capturing key decisions and learnings from building this pipeline:

- Ubunye Engine is CLI-first but needs a Python API for Databricks (CLI creates a new Spark session, wasting the one Databricks already has)
- GitHub Actions cannot run `ubunye run` against Unity Catalog — local Spark doesn't support multi-part namespaces. CI is validation only.
- Unity Catalog names with hyphens (aws-db-nonprod-aic-catalog) need backticks in SQL
- `--mode` / `-m` is the CLI flag for environment switching, not `--profile`. Default is "DEV". Value is injected as `{{ mode }}` in Jinja.
- `-dt` / `--data-timestamp` is how you pass effective_year_month. Injected as `{{ dt }}` in Jinja.
- Config uses `format: unity` with `sql:` for Unity Catalog inputs. Use `{{ env.UNITY_CATALOG | default('aws-db-nonprod-aic-catalog') }}` for catalog switching, or set catalog at session level with `USE CATALOG`.
- Databricks Asset Bundles (DABs) is the deployment mechanism — defines jobs as code in the repo, deployed via `databricks bundle deploy`
- The deployment flow: push code → GitHub Actions validates → merge to main → GitHub Actions deploys bundle → Databricks runs on schedule
- Rialto (CPDS) follows this same pattern: runner script + asset bundle + GitHub Actions deploy. Ubunye should follow the same pattern for consistency within ABSA.
- The rewards pipeline has no actual ML model training — the GBTRegressor imports in the notebook are dead code. It's pure ETL + business rules.

### 9. TODO.md

Backlog of items to address:
- [ ] Add `ubunye deploy` CLI command that generates and deploys Databricks Asset Bundles from `rewards_pipeline.yaml`
- [ ] Add deployment guide to Ubunye docs (`docs/getting_started/deployment.md`)
- [ ] Migrate `setup.py` to `pyproject.toml` (already identified)
- [ ] Add pre-commit hooks (already identified)
- [ ] Fix CI unit tests (already identified)
- [ ] Add end-to-end example using the Python API on Databricks
- [ ] Support `USE CATALOG` / `USE SCHEMA` in config.yaml ENGINE section natively
- [ ] Paste the full rewards notebook (truncated at imputation step) to complete the reward_scoring transformations.py
### 10. Update `ubunye init` scaffolding

When `ubunye init` creates a new task, it currently generates `config.yaml` and 
`transformations.py`. Update it to also generate `notebooks/<task>_dev.ipynb`.

The dev notebook should mirror the production pipeline interactively, using the 
same config and transformations but broken into cells for step-by-step execution:

- Cell 1: Setup — load config, print inputs/outputs
- Cell 2: Extract — read all inputs using Ubunye readers, print row counts
- Cell 3: Inspect sources — display() each input DataFrame
- Cell 4: Transform — import and run the Task class from transformations.py
- Cell 5: Inspect outputs — display() each output DataFrame with row counts
- Cell 6: Load — write outputs using Ubunye writers (COMMENTED OUT by default so DS don't accidentally write to prod)
- Cell 7: Sandbox — expose the Spark session for free exploration

The notebook must use the Python API (not CLI), use Databricks-native display(), 
and read from the same config.yaml so there's zero drift between dev and production.

Look at `ubunye/cli/main.py` lines 57-111 where `init` currently scaffolds files. 
Add the notebook generation there following the same pattern.

The dev notebook should use Databricks magic functions for a native experience:

- `%pip install ubunye-engine` — install in first cell
- `%run` — if shared utilities need to be loaded
- `%sql` cells for quick data validation:
```
  %sql
  SELECT COUNT(*) FROM `aws-db-nonprod-aic-catalog`.`default`.activate_telematics_transports
```
- `%md` cells for markdown documentation between steps:
```
  %md
  ## Step 2: Extract
  Read all inputs defined in config.yaml
```
- `display()` instead of `.show()` for DataFrame inspection
- `dbutils.widgets` for parameterization:
```python
  dbutils.widgets.text("effective_year_month", "202510")
  dbutils.widgets.dropdown("mode", "nonprod", ["nonprod", "prod"])
```

The notebook cell structure should be:

%md ## Parameters
→ dbutils.widgets for effective_year_month, mode

%md ## Setup
→ %pip install, load config, print summary

%md ## Extract
→ Read inputs, print row counts

%md ## Inspect Sources
→ display() each DataFrame

%sql (optional validation query)

%md ## Transform
→ Run the Task class from transformations.py

%md ## Inspect Outputs
→ display() each output DataFrame

%md ## Load (disabled by default)
→ Write outputs — ALL COMMENTED OUT

%md ## Sandbox
→ Spark session exposed for free exploration
## File structure to generate

```
ubunye/api.py                                          # NEW: Python API
ubunye/__init__.py                                     # UPDATED: expose run_task, run_pipeline

pipelines-activate/
├── run_monthly_rewards.py                             # Runner script for Databricks
└── smart_telematics_usecases/
    ├── CLAUDE.md                                      # Domain context
    └── monthly_rewards/
        └── (existing files unchanged)

bundles/
└── monthly_rewards.yaml                               # Databricks Asset Bundle job definition

databricks.yml                                         # Bundle config with targets

.github/
└── workflows/
    └── deploy.yml                                     # Updated: validate + deploy bundle

CLAUDE.md                                              # Project-level context
MEMORY.md                                              # Key decisions and learnings
TODO.md                                                # Backlog
```

## Critical rules

1. DO NOT modify any existing Ubunye Engine files except `ubunye/__init__.py` (to add exports)
2. `ubunye/api.py` must reuse existing Spark sessions on Databricks
3. The runner script must use the Python API, not the CLI
4. The asset bundle must follow ABSA CPDS conventions
5. GitHub Actions deploy step uses `databricks bundle deploy`, not `ubunye run`
6. All context in CLAUDE.md and MEMORY.md must come from what we discussed — do not invent information