# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Ubunye Engine — config-driven, Spark-native framework for ETL/ML pipelines. A pipeline is a
`<usecase_dir>/<usecase>/<package>/<task>/` folder containing `config.yaml` (declarative
inputs/transform/outputs) and `transformations.py` (user logic). The same task folder runs on a
laptop, YARN, K8s, or Databricks — only the `ENGINE.profiles` block changes.

## Dev setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]           # dev extras include pytest, hypothesis, ruff, black
pre-commit install              # black + ruff run on commit (never skip with --no-verify)
pytest tests/unit -v            # fast unit tier — what CI runs
pytest -m integration           # needs local Spark + Java 11
pytest tests/ -k test_config    # single test by keyword
```

CI (`.github/workflows/ci.yml`) runs `pytest tests/unit -m "not integration"` with coverage. All
other workflows under `.github/workflows/*_databricks.yml` fire end-to-end examples against a real
Databricks workspace and are gated on secrets; treat them as integration probes, not PR gates.

## CLI surface

`ubunye <cmd> --help` is authoritative — don't hard-code flag tables here. Top-level commands:
`init`, `validate`, `config`, `plan`, `run`, `test run`, `export {airflow,databricks}`,
`lineage {show,list,compare,search,trace}`, `models {list,info,promote,demote,rollback,archive,compare}`,
`plugins`, `version`.

**Non-obvious CLI gotchas** (every single one has bitten someone):

- Timestamp flag is `-dt` / `--data-timestamp`, **not** `--dt`.
- `--mode` / `-m` defaults to `DEV` (uppercase). Modes are case-sensitive when matching profile keys.
- `validate` takes `--profile`; `run` takes `-m/--mode`. They are not aliases.
- `validate` has `--all` for all tasks in a package; `run` does not — repeat `-t` instead.
- `test run` defaults profile to `test` and `--lineage` is **on** by default (opposite of `run`).
- `lineage *` sub-commands use singular `--task` / `-t`, not `--task-list`.
- `models *` sub-commands use `--use-case` (`-u`), `--model` (`-m`), `--store` (`-s`) — different
  flag shape from the pipeline commands.
- `merged_spark_conf(mode)` silently returns base conf if `mode` doesn't match any profile key. No
  error, no warning. Verify the profile name matches the YAML before blaming the engine.

## Architecture — the parts that aren't obvious from a single file

### Two entry points, one execution path

- **CLI** (`ubunye.cli.main`): used from terminals and CI. Creates a fresh `SparkBackend`.
- **Python API** (`ubunye.api.run_task` / `run_pipeline`): used from Databricks notebooks and jobs.
  Auto-detects an active `SparkSession` via `SparkSession.getActiveSession()` and wraps it in
  `DatabricksBackend` instead of creating a new one. See `ubunye/api.py::_detect_backend` — this is
  why the same code works identically in a notebook and at the shell.

Both paths converge in `ubunye.core.task_runner.execute_user_task`, which loads
`transformations.py` via a `sys.path` injection of the task directory. The task dir is prepended
to `sys.path` so sibling modules (`transformations.py`, helpers) resolve correctly — a regression
here broke multi-task pipelines once (commit `6362942`), keep the import contract in mind when
touching the runner.

### Plugins are entry-points, not imports

Readers, writers, transforms, ML models, monitors, and hooks are all discovered via
`importlib.metadata` entry-point groups declared in `pyproject.toml`:

- `ubunye.readers` — `hive`, `jdbc`, `s3`, `unity`, `rest_api`
- `ubunye.writers` — `s3`, `jdbc`, `unity`, `rest_api`
- `ubunye.transforms` — `noop`, `model`
- `ubunye.ml` — `sklearn`, `sparkml`
- `ubunye.monitors` — `mlflow`, `lineage`
- `ubunye.hooks` — `events`, `otel`, `prometheus`

The `format: <name>` field in a config selects the plugin. Adding a connector = write the class,
register the entry point in `pyproject.toml`, reinstall with `pip install -e .`. Don't wire plugins
by direct import inside the engine.

### Hooks vs. monitors

`ubunye/core/hooks.py` defines the `Hook` / `HookChain` observability abstraction — the modern
path. Built-in hooks live in `ubunye/telemetry/hooks/` (events, otel, prometheus) and are
always-on via the `ubunye.hooks` entry point. The legacy `ubunye.monitors` group (mlflow, lineage)
is wrapped through `MonitorHook`; prefer the hook interface for new observability code.

### Config schema

`ubunye/config/` loads YAML through Pydantic v2 + Jinja2 (applied after YAML parse, before
validation). Top-level keys: `MODEL` (`etl`/`ml`), `VERSION` (semver), `ENGINE` (base `spark_conf`
plus per-mode `profiles`), `CONFIG` (`inputs` / `transform` / `outputs`), optional `ORCHESTRATION`
(defaults consumed by `ubunye export`). Jinja context: `{{ dt }}`, `{{ dtf }}`, `{{ mode }}`,
`{{ env.VAR_NAME }}`. `load_config()` accepts either a directory (for `validate --all`) or a file
path (for `run`/`plan`/`config`).

### Deployment split

DABs (`bundles/`, `databricks.yml`) and Airflow DAGs belong in the **usecase repo**, not in this
engine repo. CI here is validation-only; execution happens on Databricks. `ubunye export` produces
the scheduler artifacts that the usecase repo commits.

## Source layout (only what isn't discoverable)

- `ubunye/api.py` — public `run_task` / `run_pipeline`, backend auto-detection.
- `ubunye/core/task_runner.py` — the `sys.path`-injecting task loader. Touch carefully.
- `ubunye/core/hooks.py` — Hook / HookChain abstraction. New observability goes here.
- `ubunye/backends/{spark,databricks}_backend.py` — session lifecycle. Databricks reuses active
  session; Spark creates a new one.
- `ubunye/plugins/ml/` — `BaseModel`, `SklearnModel`, `SparkMLModel`, `BatchPredictMixin`,
  `MLflowLoggingMixin`. The contract for user-defined models lives here.
- `ubunye/lineage/` — `RunContext`, `StepRecord`, `LineageRecorder`, `FileSystemLineageStore`.
- `ubunye/orchestration/` — `AirflowExporter`, `DatabricksExporter` (used by `ubunye export`).
- `examples/production/` — reference pipelines with a **byte-identical `transformations.py`**
  invariant between `titanic_local` and `titanic_databricks`, enforced in CI via `diff -q`. When
  editing either, mirror the change in the other or the Databricks workflow fails. Read
  `examples/production/README.md` before touching these.

## Conventions baked into the engine

- Folder: `<usecase_dir>/<usecase>/<package>/<task>/{config.yaml,transformations.py}`. The Python
  API derives the Spark app name `ubunye:<usecase>.<package>.<task>` from the last three path
  parts — renaming the dirs changes what shows up in the Spark UI.
- Deploy mode defaults to `client`.
- Default lineage dir: `.ubunye/lineage`.
- Version is read from package metadata (`importlib.metadata.version("ubunye-engine")`), so
  `pip install -e .` is required for `ubunye version` to report anything other than `unknown`.

## Workflow in this repo

The `tasks/` scratchpad tracks work in flight (not authoritative — GitHub issues + changelog are):

- `tasks/todo/task-NN.md` — queued bugs and coverage gaps. `task-00.md` is the umbrella strategy.
- `tasks/done/task-NN.md` — finished work, same number, with a `Status: done (date)` line
  appended. **Move, don't copy.**

Four project-level subagents automate the loop (see `.claude/agents/README.md`):

1. `fire-tester` — runs a production example end-to-end on Databricks, files findings to
   `tasks/todo/`. Use for prompts like "run the titanic example", "does the ML lifecycle still
   work".
2. `engine-fixer` — picks up a filed bug, produces **one atomic commit per bug** (failing test
   first, then minimal fix). Does not release.
3. `example-author` — scaffolds a new `examples/production/*` pipeline when a coverage-gap task
   calls for one.
4. `task-curator` — tidies `tasks/` and keeps numbering/metadata honest.

Hard invariants these agents preserve (and you should too):

- No releases. The `pypip` GitHub environment requires a human reviewer.
- Never skip pre-commit hooks (`--no-verify`) or signing.
- One commit per fix — no bundled bug-fix commits.
- **Docs + changelog move with code.** Every code change touches `docs/` and `docs/changelog.md`
  in the same commit; this is tracked in user memory and has been called out before.

## Related docs in this repo

- `README.md` — user-facing pitch + quickstart.
- `DEV_README.md` — environment bootstrap. Partially stale (lists `dagster`/`prefect` exporters
  that don't exist and a `doctor` command that was never shipped). Trust the code over this file.
- `docs/` — full MkDocs site (`mkdocs serve` to preview). `mkdocs.yml` nav lists what exists.
- `tasks/README.md` — scratchpad conventions.
- `examples/production/README.md` — portability contract, CE-vs-paid-workspace matrix.
