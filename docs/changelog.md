# Changelog

All notable changes to Ubunye Engine will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.8] — 2026-05-19

### Added

- **`ubunye deploy databricks` command.** End-to-end deployment from a local
  `config.yaml` to a running Databricks job — handles auth, file upload to
  `/Workspace/`, wrapper notebook generation, and idempotent job
  creation/update via the Databricks SDK. Uses a two-level `targets.yaml`
  lookup (usecase-level defaults, task-level overrides). Supports `--dry-run`
  for previewing the job spec without deploying. New optional dependency:
  `pip install ubunye-engine[databricks]`.

- **Deploy error types.** `DeployError` base class with `AuthNotFoundError`,
  `AuthInvalidError`, `TargetNotFoundError`, `WorkspaceUploadError`, and
  `BundleDeployError` — all follow the dual-inheritance pattern
  (`DeployError(UbunyeError, RuntimeError)`).

- **Structured error messages across the engine.** Every user-facing exception
  now inherits from `UbunyeError` (with optional `context` dict and `hint`
  string) **and** the stdlib type it replaces (dual inheritance for backward
  compatibility). New exception classes: `TaskNotFoundError`,
  `TaskClassMissingError`, `ReaderNotFoundError`, `WriterNotFoundError`,
  `TransformNotFoundError`, `TransformOutputError`, `MonitorNotFoundError`,
  `SourceReadError`, `SinkWriteError`, `SparkSessionError`, `ModelLoadError`,
  `ModelNotFittedError`, `VersionExistsError`, `VersionNotFoundError`,
  `PromotionBlockedError`, `RegistryNotFoundError`,
  `LineageRecordNotFoundError`, and `ConfigProfileError`. Existing config
  errors (`ConfigFieldError`, `ConfigTemplateError`) migrated from
  `ubunye.config.loader` to `ubunye.core.errors`. See the new
  [Error Reference](errors.md) for the full hierarchy and examples.

- **Hook failure logging.** `HookChain` now logs a structured warning when a
  hook's `task()` or `step()` context manager raises, instead of silently
  swallowing the exception.

- **CI workflows link to the Databricks job UI** after `bundle run`, so
  users can find notebook cell output that `databricks bundle run` does not
  stream to stdout.

- **`ubunye init github-actions` command.** Generates a GitHub Actions workflow
  for any pipeline — validates config and runs tests on PRs, deploys to
  Databricks via `ubunye deploy databricks` on merge to main. Supports
  `--no-deploy` for CI-only workflows, `--extras` for pip install extras
  (auto-includes Java setup when `spark` is present), and `--target` for the
  Databricks deploy target. The `init` command is now a sub-app:
  `ubunye init pipeline` (formerly `ubunye init`) and
  `ubunye init github-actions`.

### Fixed

- **Promotion gate failure now propagates.** `PromotionBlockedError` was
  silently caught by the titanic training task's `except ValueError` (via
  dual inheritance). Gate failures now re-raise — CI goes red when a model
  fails its quality gate instead of reporting green.

- **`typer[all]` replaced with plain `typer`.** Typer >= 0.24 dropped
  the `[all]` extra, causing pip warnings on install.

- **`merged_spark_conf()` / `resolved_catalog()` / `resolved_schema()` now
  raise `ConfigProfileError`** when profiles are defined but the requested
  profile doesn't match. Previously these methods silently returned the base
  config, hiding typos in `--mode` / `--profile` flags.

### Changed

- **`ubunye init` is now a sub-app** with two subcommands:
  `ubunye init pipeline` (the previous `ubunye init`) and
  `ubunye init github-actions`. This is a breaking CLI change — update
  any scripts that call `ubunye init -d ...` to `ubunye init pipeline -d ...`.

- **`[ml]` extra no longer installs `torch`.** Use `[ml-torch]` for
  PyTorch workloads. This saves ~1 GB of CUDA wheel downloads in CI for
  sklearn-only pipelines.

- **`CONFIG.transform.type` is now optional.** When omitted, the engine
  defaults to loading the user's `Task` class from `transformations.py` —
  no `type: noop` declaration needed. Existing configs with `type: noop`
  continue to work but emit a `DeprecationWarning` advising removal.
  All scaffolded configs, production examples, test fixtures, and docs
  have been updated to omit the field.

- **Unknown config fields are now rejected at load time.** All Pydantic
  models (except `IOConfig`) use `extra="forbid"`. A typo like `ENGNE`
  raises `ConfigFieldError` with a "Did you mean 'ENGINE'?" suggestion
  via `difflib.get_close_matches`. `IOConfig` retains `extra="allow"`
  so plugin-specific keys (REST API `headers`, `pagination`, etc.) pass
  through to connectors. **Breaking:** configs with unknown top-level or
  nested fields that were previously silently ignored will now fail.

- **Undefined Jinja template variables fail immediately.** The resolver
  now uses `StrictUndefined` instead of `DebugUndefined`. A reference
  to `{{ ds }}` without passing `ds` as a CLI variable raises
  `ConfigTemplateError` listing available variables. The `| default()`
  filter continues to work.

### Added

- **Production reference example: device mapping ETL (Databricks, paid
  workspace)** — `examples/production/device_mapping_etl_databricks/`
  ports a legacy policy-device mapping script onto Ubunye. Three Unity
  Catalog reads, one Unity Catalog Delta sink, a monthly Databricks Jobs
  schedule (2nd of month, 06:00 UTC), and a dedicated GitHub Actions
  workflow (`.github/workflows/device_mapping_etl_databricks.yml`) that
  gates `bundle deploy` on OAuth service-principal secrets plus
  `TELM_CATALOG` / `TELM_SCHEMA` environment secrets. No catalog/schema
  identifiers are committed — values flow through DAB `--var` flags at
  deploy time, keeping confidential Unity Catalog names out of source
  control. Corrects a latent bug in the original script where the
  IMEI-first-detection-adjusted `installation_datetime_final` was
  computed but never selected into the final output.

- **Production reference example: flood-risk (two-task pipeline,
  paid Databricks + Unity Catalog).** A port of the legacy flood-detection
  notebook to Ubunye as `examples/production/flood_risk_databricks/`,
  split into two chained tasks:
  - `geocode_addresses` — reads `(id, address)` rows from a UC source
    table, calls TomTom Search (top-1 per id, three-step parameter
    fallback, 429 retry), writes `address_geocoded`.
  - `flood_risk` — reads `address_geocoded`, calls JBA `floodscores` and
    `flooddepths` in batches of 10, merges the two responses on `id`,
    renames ~60 nested keys to snake-case, writes `address_flood_risk`.

  Quarterly schedule (1st of Jan/Apr/Jul/Oct at 06:00 UTC). TomTom and
  JBA credentials live in a Databricks secret scope (`flood-risk` by
  default) and are read by the notebook wrapper; Unity Catalog
  identifiers and the source-table name come from GitHub environment
  secrets through `--var` at deploy time. Also corrects the legacy
  `"Base "` → `"Basic "` auth-header typo and strips the Zscaler cert
  probing block that belongs on corporate networks, not serverless.
  OAuth-gated GH Actions workflow
  (`.github/workflows/flood_risk_databricks.yml`) runs Spark + fake-
  HTTP unit tests, validates the bundle, and deploys to the `nonprod`
  target when all three UC secrets are present. See the example's
  `README.md`.

### Added

- **Service principal + OAuth auth for Databricks CI.** The four Databricks
  example workflows (`databricks_deploy.yml`, `jhb_weather_databricks.yml`,
  `multitask_databricks.yml`, `titanic_ml_databricks.yml`) now pass
  `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` alongside the existing
  `DATABRICKS_TOKEN` env var. The secrets gate accepts either flow: PAT
  (`HOST` + `TOKEN`) or OAuth (`HOST` + `CLIENT_ID` + `CLIENT_SECRET`). The
  Databricks CLI auto-selects OAuth when both client vars are set. New
  reference page `docs/databricks-auth.md` walks through service principal
  creation, workspace/UC grants, secret rotation, and verification.

---

## [0.1.7] — 2026-04-21

### Changed

- **`MODEL` and `VERSION` are now optional at the top level of `config.yaml`.**
  `MODEL` defaults to `etl` and `VERSION` defaults to `"0.0.0-dev"`. The semver
  validator now accepts an optional pre-release suffix (e.g. `1.0.0-rc1`,
  `0.0.0-dev`) in addition to plain `MAJOR.MINOR.PATCH`. Existing configs that
  set these fields explicitly continue to work unchanged. Rationale: these two
  lines were boilerplate in every scaffolded pipeline; the defaults cover the
  common case. Set them explicitly when job type or version is load-bearing
  (lineage, model registry, orchestrator metadata).

### Added

- **Production reference example: Titanic multi-task pipeline (local)** —
  `examples/production/titanic_multitask_local/` demonstrates sequential
  task chaining via `ubunye run -t clean_data -t aggregate`. Task 1 reads
  the Titanic CSV, cleans it, and writes intermediate Parquet. Task 2 reads
  that Parquet and computes survival rates by class and age group. Exercises
  `run_pipeline()`, sibling-module isolation between tasks, and cross-task
  lineage. CI workflow (`.github/workflows/multitask_local.yml`) runs Spark
  unit tests, validates both configs, runs the full pipeline, and verifies
  output. See the example's `README.md`.

- **Production reference example: Titanic multi-task pipeline (Databricks)** —
  `examples/production/titanic_multitask_databricks/` is the Databricks
  counterpart of the local multi-task example. Same `transformations.py`
  (byte-identical, CI-enforced), but task chaining uses Unity Catalog Delta
  tables instead of Parquet files. Task 1 writes `titanic_cleaned`, task 2
  reads it and writes `survival_summary`. Runs via `ubunye.run_pipeline()`
  on serverless compute. CI workflow enforces portability diff against the
  local example and validates/deploys the Asset Bundle.

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

- **Undefined CLI template variables silently leaked into resolved configs.**
  `resolve_config` pre-checked `{{ env.X }}` references but not bare
  `{{ var }}` identifiers. Jinja2's `DebugUndefined` left unresolved
  expressions verbatim, so `path: "file:///{{ dt }}"` with no `dt`
  provided would pass validation and then hand Spark a literal
  `file:///{{ dt }}` at runtime. Fix adds a post-render residue scan
  that names the offending variable and suggests a CLI flag, env var,
  or `| default()` filter. Regression tests in
  `tests/unit/config/test_resolver.py`. Pre-existing configs under
  `examples/` and `pipelines/` all use `| default()` on CLI-derived
  vars, so no downstream config needs updating.

- **Sibling modules leaked between sequential tasks in `run_pipeline`.**
  `_with_task_dir_on_path` added the task dir to `sys.path` but never
  cleaned up `sys.modules` on exit. Two tasks that each shipped their
  own `model.py` (or `utils.py`, etc.) would silently run the *first*
  task's module when the second task imported it — Python's import
  cache was keying on the shared short name. Fix evicts only modules
  whose source file lives under the exiting task dir; stdlib and
  site-packages are untouched. Regression test in
  `tests/unit/test_task_runner.py`. Caught by offline audit on the
  overnight branch, ahead of the planned multi-task DAG example
  (`tasks/todo/task-04.md`).

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
