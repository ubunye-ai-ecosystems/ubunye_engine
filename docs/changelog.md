# Changelog

All notable changes to Ubunye Engine will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- **Production reference example: Titanic ML end-to-end on Databricks** —
  `examples/production/titanic_ml_databricks/` demonstrates the full ML
  lifecycle: `UbunyeModel` subclass (sklearn RandomForest), MLflow param/
  metric/artifact logging, filesystem-backed `ModelRegistry` on a UC volume,
  and a `PromotionGate` on validation AUC. Two serverless jobs share one
  Asset Bundle — `titanic_train` registers and auto-promotes; `titanic_predict`
  loads the current production/staging model and writes a Unity Catalog
  Delta predictions table. A one-row `training_metrics` audit row is appended
  per training run. CI (`.github/workflows/titanic_ml_databricks.yml`) runs
  pandas/sklearn unit tests, diffs the two `model.py` copies for drift, and
  validates/deploys the bundle when Databricks secrets are configured. See
  the example's `README.md`.

### Changed

- **GitHub Actions bumped to Node 24-capable majors.** `actions/checkout@v4`
  → `@v6`, `actions/setup-python@v5` → `@v6` across all nine workflow files.
  Resolves the deprecation warning surfaced on 2026-04-16 runs ahead of the
  Node 20 removal deadline (2026-09-16). No behavioural change.

### Fixed

- N/A

---

## [0.1.6] — 2026-04-15

### Changed

- **`titanic_databricks` DAB switched to serverless + Unity Catalog.** Removed
  the `new_cluster` block and the DBFS bootstrap; the notebook now provisions
  a UC volume, downloads the Titanic CSV into it at runtime, and the writer
  emits a Unity Catalog managed Delta table (`workspace.titanic.survival_by_class`
  by default). Validated end-to-end on Databricks Free Edition. The portability
  contract with `titanic_local` (byte-identical `transformations.py`) is
  unchanged — only the deployment wrapper (config + notebook + DAB) differs.
- **`jhb_weather_databricks` DAB switched to serverless compute.** Removed
  the `new_cluster` block (and the `existing_cluster_id` escape hatch) from
  `databricks.yml`. Notebook tasks with no cluster spec route to serverless
  on both Free Edition and paid workspaces; the notebook installs
  `ubunye-engine` at runtime via `%pip`. Default `weather_catalog` changed
  from `main` to `workspace` because Free Edition auto-provisions only the
  `workspace` catalog. Paid-workspace users override via
  `--var="weather_catalog=main"`. README documents the rationale.
- **`databricks_deploy.yml` (titanic) skips deploy gracefully when secrets
  are absent.** Mirrors the soft-skip pattern in `jhb_weather_databricks.yml`
  so PRs from forks (or repos that have not configured
  `DATABRICKS_HOST`/`DATABRICKS_TOKEN`) still exercise the unit tests and
  portability diff instead of failing the workflow.
- **Production examples switched from pandas twins to PySpark tests.** Every
  `transformations.py` under `examples/production/` now exposes a single Spark
  implementation. Tests use a session-scoped `SparkSession` fixture
  (`local[1]`, 512 MB driver, shuffle partitions=1) so the production code is
  the code under test. Eliminates the dual-maintenance burden and the risk of
  silent drift between pandas and Spark paths. Unit-test CI steps now install
  Java 17 + `ubunye-engine[spark,dev]` on the runner.

### Added

- **Production reference example: JHB hourly weather (REST API → Unity Catalog)** —
  end-to-end example at `examples/production/jhb_weather_databricks/` demonstrating
  REST ingestion with the `rest_api` reader against the free Open-Meteo API
  (lat/lon for Johannesburg, no auth required), a Spark transform that explodes
  parallel hourly arrays into a tidy one-row-per-hour DataFrame, and a Unity
  Catalog Delta writer partitioned by `forecast_date`. Ships a Databricks Asset
  Bundle with a scheduled daily job (06:00 `Africa/Johannesburg`), a notebook
  wrapper around `ubunye.run_task()`, seven pandas unit tests over a hand-built
  fixture response, and a CI workflow
  (`.github/workflows/jhb_weather_databricks.yml`) that runs the tests, smoke-checks
  the endpoint, and validates/deploys the bundle when Databricks secrets are
  configured. See the example's `README.md`.

- **Production reference example: Titanic (local runtime)** — end-to-end example at
  `examples/production/titanic_local/` demonstrating a CSV → Parquet pipeline with
  dev/prod profiles, Jinja-templated paths, pandas unit tests (no Spark), a committed
  golden Parquet, and a GitHub Actions workflow (`.github/workflows/local_pipeline.yml`)
  that validates config, runs the pipeline on a real SparkSession, and diffs the output
  against the golden. One half of the portability demo — the Databricks counterpart
  shares `transformations.py` verbatim. See the example's `README.md`.

- **Production reference example: Titanic (Databricks Community Edition)** — the
  Databricks half of the portability demo at `examples/production/titanic_databricks/`.
  Ships a Databricks Asset Bundle (`databricks.yml`) sized for CE's single-node /
  DBFS / no-UC constraints, a notebook entry (`notebooks/run_titanic.py`) that calls
  `ubunye.run_task()` against the active SparkSession, the same pandas unit tests,
  and a CI workflow (`.github/workflows/databricks_deploy.yml`) that installs the
  Go-based Databricks CLI, validates and deploys the bundle, and enforces the
  portability contract by diffing `transformations.py` against the local example.
  Known CE limitations (no service principals, restricted Jobs API, DBFS deprecation)
  are documented honestly rather than worked around.

- **Cross-runtime reference index** — `examples/production/README.md` explains the
  portability contract, provides a side-by-side config comparison of the two examples,
  a decision guide for choosing between the local and Databricks runtimes, and a
  migration table covering what changes when moving from Community Edition to a
  standard Databricks workspace.

- **Hook abstraction for observability** (`ubunye/core/hooks.py`) — `Hook` base class and
  `HookChain` multiplexer. Tasks and steps are now wrapped in hook context managers so the
  Engine no longer imports telemetry modules directly. Built-in hooks shipped under
  `ubunye/telemetry/hooks/`: `EventLoggerHook`, `OTelHook`, `PrometheusHook`, `LegacyMonitorsHook`.
  Third parties can register custom hooks (Slack alerts, audit logs, drift checks) without
  modifying the Engine. See `docs/patterns/hooks.md`.

- **`ubunye.hooks` entry point group** (`pyproject.toml`) — third-party packages can
  register `Hook` subclasses as entry points and have them auto-discovered by the Engine.
  The three built-in telemetry hooks (events, otel, prometheus) are registered via this
  mechanism and gated on `UBUNYE_TELEMETRY=1`.

- **Python API** (`ubunye/api.py`) — `run_task()` and `run_pipeline()` for running Ubunye tasks
  from Python code (Databricks notebooks, scripts, tests) without the CLI.
  Auto-detects and reuses active SparkSessions. Exported from `ubunye.__init__`.

- **DatabricksBackend** (`ubunye/backends/databricks_backend.py`) — backend that wraps an
  existing SparkSession instead of creating one. `stop()` is a no-op since we don't own the session.

- **Dev notebook scaffolding** — `ubunye init` now generates `notebooks/<task>_dev.ipynb`
  alongside `config.yaml` and `transformations.py`. The notebook uses `DatabricksBackend`,
  `dbutils.widgets`, and `display()`. The Load step is commented out by default.

- **Deployment docs** — `docs/deployment.md` covering Databricks Asset Bundles pattern,
  GitHub Actions CI/CD, and Python API on Databricks. DABs belong in the usecase repo,
  not the engine.

- **Deploy workflow** — `.github/workflows/deploy.yml` validates configs on PR and
  runs unit tests. Bundle deployment is handled in the usecase repo.

- **`ubunye test run`** CLI sub-command — runs tasks with a test profile and reports PASS/FAIL.

- **Model Registry** (`ubunye/models/`) — library-independent ML lifecycle management.
  - `UbunyeModel` abstract contract: `train`, `predict`, `save`, `load`, `metadata`, `validate`.
  - `ModelRegistry` — filesystem-backed versioning with stages: development → staging → production → archived.
  - `PromotionGate` — configurable metric thresholds (`min_*`, `max_*`, `require_drift_check`).
  - `load_model_class()` — dynamic model file importer; mirrors the task-dir import pattern.
  - `ModelTransform` plugin (`type: model`) — train and predict from config YAML.
  - `ubunye models` CLI sub-commands: `list`, `info`, `promote`, `demote`, `rollback`, `archive`, `compare`.
  - `RegistryConfig` and `ModelTransformParams` Pydantic schema additions.

- **Lineage tracking** (`ubunye/lineage/`) — automatic run provenance.
  - `RunContext`, `LineageRecorder`, `FileSystemLineageStore`, `hash_dataframe`.
  - `ubunye lineage` CLI sub-commands: `show`, `list`, `compare`, `search`, `trace`.
  - `--lineage` flag on `ubunye run`.

- **REST API connector** — paginated HTTP reader and writer.
  - Pagination strategies: offset, cursor, next_link.
  - Auth: bearer, api_key (header or query param), basic.
  - Rate limiting with configurable `requests_per_second`.
  - Exponential backoff retry on configurable status codes.
  - Optional explicit schema declaration.

- **Config validation** — `ubunye validate` command with full Pydantic v2 schema.
  - Format-specific field validation in `IOConfig`.
  - Jinja2 rendering before Pydantic validation.
  - Semver validation on `VERSION` field.

- **`ubunye export airflow|databricks` CLI** — the `AirflowExporter` and
  `DatabricksExporter` under `ubunye/orchestration/` are now reachable from the
  command line. The command loads the task's `config.yaml`, pulls defaults from
  its `ORCHESTRATION` block, and writes the artifact to `--output`. Airflow emits
  a DAG Python file; Databricks emits a Jobs API `job.json`. Classes are now
  exported from `ubunye.orchestration.__init__`.

- **Test infrastructure** — 288 unit tests, all Spark-free in `tests/unit/`.

### Changed

- `databricks_expoter.py` renamed to `databricks_exporter.py` (typo fix). Not
  previously exported from `ubunye.orchestration`, so external callers are
  unaffected.

- **Unified execution path** (`ubunye/core/task_runner.py`) — `api.py`, `cli/main.py run`
  and `cli/test_cmd.py run` previously each reimplemented the read → transform → write
  loop and called `load_monitors` / `safe_call` directly. They now delegate to
  `execute_user_task()`, which wraps the user's `Task.transform()` as an ephemeral
  Transform plugin and runs it through `Engine`. Single code path, single hook
  lifecycle, `MonitorHook` adapts the legacy lineage recorder to a `Hook`.
  `Engine.__init__` gained `extra_hooks=` (append to defaults) and `manage_backend=`
  (caller-owned vs engine-owned backend lifecycle). `run_task` / `run_pipeline` accept
  `hooks=` for notebook callers who want to swap in custom hook chains.
- **Engine runtime refactored** — `ubunye/core/runtime.py` reduced from 374 to 255 lines.
  `Engine.run()` body shrank from ~220 lines to ~35 by delegating telemetry plumbing to
  hooks. The engine no longer imports from `ubunye.telemetry.*` — only from
  `ubunye.core.hooks`. `UBUNYE_TELEMETRY` and `UBUNYE_PROM_PORT` env vars still honored;
  user monitors in `CONFIG.monitors` continue to work via `LegacyMonitorsHook`.
  `Engine.__init__` gained an optional `hooks=` argument.
- `ubunye/config/schema.py` — added `RegistryConfig`, `ModelTransformParams`, `FormatType.REST_API`.
- `ubunye/__init__.py` — exports `run_task` and `run_pipeline`.
- `ubunye/cli/main.py` — mounted `models_app`, `lineage_app`, `test_app` Typer sub-apps; added notebook scaffolding to `init`.
- `pyproject.toml` — added `model` entry point under `ubunye.transforms`.

### Fixed

- **Unity Catalog writer collapsed plugin dispatch with Spark source format.**
  `UnityTableWriter.write()` was reading `cfg["format"]` and passing it to
  Spark's `DataFrameWriter.format(...)`. But `cfg["format"]` is the Ubunye
  plugin selector and is always `"unity"` by the time the writer runs, so
  Spark raised `[DATA_SOURCE_NOT_FOUND] Failed to find the data source: unity`.
  Switched the writer to read `cfg["file_format"]` (defaulting to `delta`),
  matching the convention already used by the s3 writer. Caught while
  deploying the `jhb_weather_databricks` example to a serverless workspace.

---

## [0.1.0] — 2025-09-11

### Added

- First alpha release of Ubunye Engine.
- Config-first ETL framework built on Apache Spark.
- Plugin system for Readers, Writers, and Transforms via Python entry points.
- Built-in connectors: Hive, JDBC, Delta, Unity Catalog, S3, binary.
- CLI commands: `init`, `run`, `plan`, `config`, `plugins`, `version`.
- Orchestration exporters: Airflow DAG Python file, Databricks Jobs API JSON.
- Internal ML wrappers: `SklearnModel`, `SparkMLModel`, `BatchPredictMixin`, `MLflowLoggingMixin`.
- Telemetry modules: JSON event log, Prometheus, OpenTelemetry.
- Example tasks: `fraud_detection/claims/claim_etl`, `rest_api/customer_sync`.
- `SparkBackend` with context manager and safe multiple-start support.
