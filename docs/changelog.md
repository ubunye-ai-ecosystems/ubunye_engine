# Changelog

All notable changes to Ubunye Engine will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Changed

- **Spark moved out of the engine core, so the hexagon is real rather than
  aspirational.** The founding rule is "the core never depends on the outside
  world," but `core/write_modes.py` and `core/catalog.py` called Spark directly
  (`df.write`, `spark.sql("MERGE INTO ...")`, `USE CATALOG`), so the one place the
  pattern was meant to hold was the one place it leaked. The Spark *mechanism* now
  lives in a new Spark adapter (`ubunye.adapters.spark.write_exec` and
  `ubunye.adapters.spark.catalog`); the backend-agnostic *decision* (validating a
  write mode against what a connector supports) stays in `ubunye.core.write_modes`.
  Behaviour is unchanged and every existing test passes. The old names
  (`write_modes.apply`, `merge_into`, `target_exists`, `dynamic_partition_overwrite`,
  and `core.catalog.set_catalog_and_schema`) still resolve through a deprecation shim,
  so third-party connectors keep working; they will be removed in a future major. This
  is the groundwork for a non-Spark backend (issue #38): the core can be
  backend-agnostic now because it no longer imports one backend's API.

### Added

- **A pandas backend, so `ubunye run --backend pandas` runs a task on a laptop
  with no Spark and no JVM (issue #38).** `Backend` was a port with a single kind
  of adapter (Spark), and a port with one adapter has never really been tested as
  a port. It has a second one now. `PandasBackend` reads and writes the generic
  path formats (csv, parquet, json) with pandas; lakehouse formats and managed
  tables stay Spark's job, and their connectors say so, so an unsupported pairing
  fails with a clear message instead of a stray `AttributeError`. This works
  because the data plane gained a small read/write seam on `Backend`
  (`read_frame` / `execute_write`): a path connector like `s3` asks the backend to
  do the IO instead of naming Spark, so the same task and the same `config.yaml`
  run on either backend. The proof is an integration test that runs one passthrough
  task through the engine on Spark and on pandas and asserts the output is
  identical. Install the extra with `pip install 'ubunye-engine[pandas]'`.

---

## [0.5.0] — 2026-07-14

### Fixed

- **Lineage no longer recomputes your pipeline or risks the driver.** Fingerprinting an
  output used to run the output's entire plan two to three times (a count inside the
  hasher, an unbounded sample collect, then a second count in the recorder on the same
  uncached DataFrame), and had a fallback that collected the **whole table** into the
  driver. At real scale that is a crashed driver describing a job that had already
  succeeded. Now: one `fingerprint_dataframe()` pass, persisted for the duration, one
  count shared by the hash and the row count, and every collect capped at
  `UBUNYE_LINEAGE_SAMPLE_ROWS` (default 1000) whatever the table size.

### Added

- **The model registry writes to object storage now.** `ModelRegistry` was built
  directly on `pathlib`, so models could only be saved to a normal disk — the one
  thing AWS and GCP serverless do not offer. Storage is a port now: the scheme of the
  store path picks the backend. A plain path or `file://` stays local and behaves
  exactly as before. `s3://`, `gs://` and friends go through fsspec
  (`pip install 'ubunye-engine[objectstore]'` plus your cloud's filesystem, e.g.
  `s3fs`). Any other scheme goes to whatever store someone registers under the new
  `ubunye.artifact_stores` entry-point group — one class, one entry point, no engine
  edits, no inheritance, like every other connector. Models still write real files
  (a Keras save needs an actual disk), so remote stores stage locally and upload,
  and `get_model` downloads before load. Closes #28.

- **The annotations are a public API now.** The package ships a `py.typed` marker, so
  type checkers in consuming projects finally see the engine's types (PEP 561 says they
  must ignore packages without it, so until now every annotation was invisible to
  users). mypy runs in CI and is green across all 100 source files, and it earned its
  keep immediately: it caught a Protocol that under-described its own usage, a
  shadowed variable in the REST reader's pagination, unsound stubs in the S3 lineage
  store, and thirteen stale `type: ignore` comments. `Reader.read` now returns
  `DataFramePort` instead of `Any`, so the most important object in the framework is
  typed in its own contract.

- **`DataFramePort` — the data plane finally gets its port.** Specified in the founding
  design notes and never shipped. The model layer always had its port (`UbunyeModel`:
  the engine never imports sklearn or torch); the data layer never did, so every reader
  returned and every transform received a `pyspark.sql.DataFrame` by fiat.
  `ubunye.core.ports.DataFramePort` is a `runtime_checkable` **Protocol** — structural,
  no inheritance — and a Spark DataFrame satisfies it **natively** (verified against a
  real session, not assumed), which is what makes shipping it additive rather than a
  rewrite. `ubunye.adapters.PandasDataFrameAdapter` makes pandas fit in thirty lines,
  exactly as the design promised — including guarding the trap where `df.count()`
  *exists* on pandas and means something else entirely (non-null counts per column).

- **The plugin contract is structural again.** `validate_config`, `SUPPORTED_MODES` and
  `SUPPORTS_MERGE` are read with `getattr` and defaults: a connector that declares
  nothing requires nothing. The previous contract change accidentally demanded
  inheritance — a duck-typed reader, the exact thing docs/interfaces.md promises will
  work ("no inheritance needed"), crashed with `AttributeError`. Declaring is opt-in,
  not a toll.

### Removed

- **Telemetry that could only read zero.** The `ubunye_rows_total` and
  `ubunye_bytes_total` Prometheus counters, and the `rows=` parameter on
  `EventLogger.step_end`, were dead on arrival: nothing in the engine ever fed them.
  A metric that always reads zero is worse than no metric, because somebody builds a
  dashboard on it and trusts the zero. If row metrics return they will arrive fed by
  the engine (the lineage fingerprint now computes row counts in one pass, so the
  plumbing finally exists).

- **`ubunye/compat/analytics_engine_shim.py`** — a placeholder docstring with a folder
  around it, imported by nothing.

- Stale build artifacts (`build/`, an 0.2.0 wheel in `dist/`), `mlflow.db` and a log
  file were removed from tracking and ignored.

### Changed

- **The CLI stops teaching two systems.** `run` gained `--all` (validate had it for
  years, so the two commands taught different habits for the same job). `validate`
  accepts `-m/--mode` as an alias for `--profile`, and now injects the same template
  variables as `run` — a config using `{{ mode }}` used to run perfectly and fail
  validation with an undefined variable, which is exactly backwards for a command
  whose job is to catch problems before the run. Closes #30. The `ubunye init` and
  Databricks notebook scaffolds no longer print `df.count()` per output (a full scan
  per output, teaching the habit the lineage rework just removed); they print the
  column list, which is free.

- The four control-plane Protocols (`AuthBackend`, `DeployAdapter`, `LineageBackend`,
  `RegistryBackend`) are enforced by a conformance test. They were referenced only by
  docstrings before: a contract nobody had signed. The test checks every shipped
  implementation member by member, structurally, with no inheritance added.

- `from ubunye import *` works now. `__all__` advertised five submodule names that the
  module never imported, so the star import raised `NameError` on the package's own
  public surface.

### Breaking

- **The core no longer knows about any connector. It asks.**

  The engine advertised "adding a connector = write the class, register the entry point".
  That was not true. Three places in the core held knowledge of specific implementations:

  * `config/schema.py` carried an `if/elif` chain naming `hive`, `jdbc`, `s3`, `binary`,
    `delta`, `unity` and `rest_api`, spelling out what each one required.
  * `core/write_modes.py` held `_MERGE_FORMATS = {"delta"}` and decided, on the
    connector's behalf, which formats were allowed to `MERGE`.
  * `FormatType` was a closed enum, so a correctly-registered plugin was rejected
    **before the registry was ever consulted**.

  Adding a connector meant editing the engine in three places. That is not open/closed,
  and it is not what the entry-point system is for.

  The plugin contract now carries it:

  ```python
  class Connector(ABC):
      @classmethod
      def validate_config(cls, cfg) -> list[str]: ...   # what I need

  class Writer(Connector):
      SUPPORTED_MODES: frozenset = frozenset({"append", "overwrite"})
      SUPPORTS_MERGE: bool = False                      # what I can do
      MERGE_FILE_FORMATS: frozenset = frozenset({"delta"})
  ```

  An Iceberg or Hudi connector can now declare that it supports `MERGE`, and be believed.
  `tests/unit/config/test_third_party_connector.py` registers a connector the engine has
  never heard of and asserts it works with **zero engine edits**.

  **Breaking:** `FormatType` is deleted. `IOConfig.format` is a plain `str`.

- **The engine no longer changes its behaviour based on where it is running.**

  Two places sniffed the host and acted on the guess:

  * `writers/unity.py::_is_databricks()` string-matched the Spark conf for `"databricks"`
    and used the result to decide whether to run `OPTIMIZE` and `VACUUM`. It was wrong
    twice: a connector should ask the **target** what it can do rather than detect its
    host, and the guess was factually wrong — `OPTIMIZE`, `ZORDER` and `VACUUM` are
    **Delta** features, and open-source Delta has had them for years. Anyone running Delta
    off Databricks silently did not get the maintenance they had explicitly asked for in
    their config, and nothing said so. The statements are now **attempted**, and a target
    that cannot run them is **reported**, not predicted in advance and not swallowed.

  * `_internal/auto_detect.py` picked the model registry backend from
    `DATABRICKS_RUNTIME_VERSION` **with no way to override it**. The same pipeline,
    unchanged, registered models to MLflow on Databricks and to a directory anywhere else
    — and said nothing. `UBUNYE_REGISTRY_BACKEND` now wins, the host is only a fallback,
    and it logs which it chose.

- **Inputs are validated against the READER, outputs against the WRITER.**

  One shared rule used to validate both roles, so it could not tell them apart: `unity`
  as a *source* may be given `sql`, but as a *sink* it must have a `table` — writing to a
  `SELECT` statement is meaningless. The shared rule had to accept `sql` everywhere, so
  it silently allowed it. Each side is now checked against the plugin that will actually
  handle it.

  **Breaking:** `IOConfig(format="s3")` no longer raises on its own. It cannot: an
  `IOConfig` does not know whether it is an input or an output. Validation happens at
  `TaskConfig`, where the role exists.

---

## [0.4.0] — 2026-07-13

### Breaking

- **`ENGINE.spark_conf` is now applied to a session the engine did not create — and
  static keys raise instead of being ignored.**

  Before, the conf was **silently discarded** whenever a SparkSession already existed.
  That is *always* on Databricks, and equally in a notebook, an AWS Glue job, under
  `spark-submit`, or in pytest. `DatabricksBackend` took no `conf` argument at all, and
  `api.py` computed `merged_spark_conf(mode)` and then threw it away. So every
  `ENGINE.spark_conf` and every `ENGINE.profiles` block did **nothing**, and nothing
  said so. The config claimed one thing and the runtime did another.

  Spark divides settings into *runtime* keys (`spark.sql.shuffle.partitions`) and
  *static* ones (`spark.master`, `spark.sql.extensions`, `spark.driver.memory`) that
  are fixed when the JVM starts. Runtime keys are now applied. Static keys are a
  request the engine **cannot honour**, so they raise, naming every offending key at
  once.

  **Action required:** remove static keys from `ENGINE.spark_conf` and set them where
  the session is actually created — a cluster policy, a job conf, `--conf` on
  `spark-submit`. If your pipeline "worked" with them before, it was ignoring them.

- **A config can no longer override a master the platform already chose.**

  Under `spark-submit` — how EMR Serverless and Dataproc Serverless start every job —
  the platform sets `spark.master` in the default SparkConf. A task that also set
  `spark.master` won, and the job ran **entirely in the driver**: it ignored every
  executor, finished, reported success, and billed for a cluster it never touched.
  Nothing warned. The output was correct; there was just far less of it per minute
  than there should have been, forever.

  `SparkBackend` now refuses to start when the config's master disagrees with the
  platform's.

  **Action required:** delete `spark.master` from `ENGINE.spark_conf`. The master
  belongs to whoever launched the session, not to the task.

### Added

- **`python -m ubunye` — an entry point a cloud can actually run.**

  The engine could only be reached through its console script. AWS EMR Serverless and
  GCP Dataproc Serverless do not give you a shell: they hand a **Python file** to
  `spark-submit`. An engine reachable only through its CLI cannot run on either of them
  — and you find that out after wiring up IAM, a bucket and a billing account.

  ```bash
  spark-submit --py-files deps.zip -m ubunye       --task-dir s3a://bucket/code/pipelines/sales/etl/daily --mode PROD
  ```

  It deliberately does not create a SparkSession: `spark-submit` already made one, with
  the platform's master and executors, and the engine attaches to it.

- **A `rest` extra.** `requests` lived only in the `dev` extra, so
  `pip install ubunye-engine[spark]` shipped a `rest_api` reader that **could not make a
  request**. It worked on Databricks by accident, because the runtime preinstalls it —
  the bug was invisible from the one platform most people use, and appeared the moment
  anyone left it, as a bare `ModuleNotFoundError` from inside a Spark job. A missing
  `requests` now says what to install.

- **`format` is now an open string, validated against the plugin registry.**

  The docs said "adding a connector = write the class, register the entry point". That
  was **false**: `format` was a closed enum, so a correctly-registered third-party
  plugin was rejected by config validation *before the registry was ever consulted*.
  The extension story the engine advertises did not work.

  Any name the `ubunye.readers` / `ubunye.writers` entry points can load is now valid.
  An unknown name still fails, and the error lists what is actually installed.

  **Breaking:** `IOConfig.format` is a `str`, not a `FormatType`. Code doing
  `cfg.format.value` must now use `cfg.format`.

- `AmbientSessionBackend` — an alias for `DatabricksBackend`, which contains **no
  Databricks code at all**. It attaches to a session somebody else created and declines
  to stop it, which is equally true of Glue, EMR, Dataproc, a notebook and pytest. The
  name had convinced people the engine has a Databricks dependency here. It does not.

### Fixed

- **`{{ dt | default('latest') }}` never fell back.** `ubunye run` passes
  `{"dt": ..., "dtf": ..., "mode": ...}` unconditionally, and an omitted flag arrives as
  `None`. Jinja treats `None` as *defined*, so `default()` did not fire — the template
  rendered the literal string `"None"` and pipelines quietly wrote to paths like
  `out/dt=None/`. Nothing errored; the data just went somewhere nobody meant. `None`
  values are now dropped, so they are genuinely undefined. `StrictUndefined` still makes
  a real typo fail loudly.

- **`ENGINE.catalog` no longer breaks every non-Databricks Spark.** `set_catalog_and_schema`
  issued `USE CATALOG`, which is a Unity Catalog statement — open-source Spark rejects it
  outright with `PARSE_SYNTAX_ERROR`. It is now attempted and, if unsupported, logged and
  skipped: the configs use three-part names anyway, which Spark resolves without it.

- **`ubunye export airflow` and `ubunye export databricks` emitted artifacts that could
  not run.** Both generated `ubunye run -c <config> --profile <p>` — and there is no `-c`
  and no `--profile` on `ubunye run`. Every DAG and every `job.json` this exporter has
  ever produced would fail on its first task with "no such option". The tests asserted
  the broken flags, which is why nobody noticed: they checked the exporter still emitted
  what it always had, rather than something that works.

---

## [0.3.0] — 2026-07-12

### Breaking

- **The `s3` writer now defaults to `mode: append`, not `overwrite`.** Every other
  writer already defaulted to `append`; the same config key meant "insert" on one
  connector and "destroy and replace" on another. All writers are now consistent,
  and the default is the mode that cannot lose data by accident.

  **Action required:** any `s3` output that omitted `mode` and relied on the
  implicit overwrite must now say `mode: overwrite` explicitly. Nothing fails
  loudly — the write succeeds, it just appends. Grep your configs for `format: s3`
  outputs with no `mode` key before upgrading.

### Added

- **The `delta`, `hive` and `binary` connectors now exist.** All three were
  declared in `FormatType`, accepted by config validation, and documented — but
  had no plugin registered behind them, so any pipeline using them died with
  `ReaderNotFoundError` / `WriterNotFoundError`.

  | Format | Reader | Writer |
  |---|---|---|
  | `delta` | **new** — by path, table, or SQL; time travel via `version_as_of` / `timestamp_as_of` | **new** — all six write modes |
  | `hive` | already existed | **new** — all six write modes, `partitionBy` |
  | `binary` | **new** — Spark's `binaryFile` source, one row per file | none: the source is read-only |

  A test now asserts that **every** format in `FormatType` resolves to a
  registered plugin, so this class of gap fails in CI rather than in a pipeline.

- **`format: binary` in `CONFIG.outputs` is rejected at config load.** Spark's
  `binaryFile` source cannot write. The failure now happens in `ubunye validate`,
  with a reason, instead of after the transform has already run.

- **Four new write modes.** `CONFIG.outputs.*.mode` previously accepted only
  `overwrite`, `append` and `merge` — and of those, `merge` was never
  implemented. The full set is now:

  | Mode | Behaviour |
  |---|---|
  | `errorifexists` (alias `error`) | Fail if the target exists — Spark's own default |
  | `ignore` | No-op if the target exists |
  | `merge` | Delta MERGE (upsert) on `merge_keys`; creates the target on first run |
  | `overwrite_partitions` | Replace only the partitions in the DataFrame — on Delta (dynamic or `replace_where`), or via `INSERT OVERWRITE` on a non-Delta table |

- **Integration tests on a real Spark session.** The write modes are exercised
  against a real JVM Spark (and a real Delta table) in CI, not just asserted
  against mocks — `ignore` really leaves the target alone, `merge` really
  upserts, and `overwrite_partitions` really leaves its neighbouring partitions
  standing.

- **`ubunye.core.write_modes`.** Mode semantics live in one module rather than
  in each connector. Writers declare the modes they support; a mode a connector
  cannot honour raises `SinkWriteError` **before any rows are written** instead
  of being silently downgraded. JDBC accepts Spark's four native modes only;
  `rest_api` is append-only.

- **`partitionBy`, `merge_keys` and `replace_where` on the `s3` writer.**

### Fixed

- **`mode: merge` crashed at runtime.** It passed config validation and was
  documented (with `merge_keys`), but no writer implemented it — the string was
  handed to `df.write.mode("merge")`, which Spark rejects with
  `IllegalArgumentException: Unknown save mode`. It now performs a real Delta
  MERGE.

- **`errorifexists` and `ignore` were unreachable.** The JDBC and Unity writer
  docstrings claimed support, but the `WriteMode` enum rejected both at config
  load, so no `config.yaml` could ever set them.

- **The `s3` writer silently dropped `options`.** `mergeSchema`, `overwriteSchema`
  and friends were documented but never applied to the Spark writer.

- **Stale tests in `tests/test_rest_api_plugin.py`.** Five tests still expected
  `ValueError` where the engine has raised typed `SourceReadError` /
  `SinkWriteError` for some time. They had been failing at HEAD unnoticed because
  CI only runs `tests/unit`.

- **`overwrite_partitions` refuses an unpartitioned target.** Dynamic partition
  overwrite without partitions is a full-table wipe; the engine now fails the
  config instead of quietly destroying data. The session-level
  `spark.sql.sources.partitionOverwriteMode` conf is restored after each write
  rather than leaking into later writes.

- **`overwrite_partitions` now uses the mechanism each target actually supports:**
  Delta's own `partitionOverwriteMode` write option for Delta, `INSERT OVERWRITE`
  (column-aligned, since it is positional) for an existing table, and Spark's
  session-level `partitionOverwriteMode=DYNAMIC` for a path. Every route is
  covered by an integration test against a real Spark session and a real Delta
  table.

---

## [0.2.0] — 2026-05-20

### Added

- **Interactive notebook API (`ubunye.notebook`).** New `ubunye.notebook()`
  factory returns a `NotebookContext` for step-by-step task execution in
  Databricks notebooks. Data scientists can call `ctx.read()`,
  `ctx.transform()`, and `ctx.write()` in separate cells, inspecting
  DataFrames between stages. Environment variables referenced via
  `{{ env.VAR }}` in config.yaml are auto-resolved from Databricks widgets
  and secrets — no manual `os.environ` setup required.

- **Public step methods on `Engine`.** `Engine.read_inputs()`,
  `Engine.apply_transforms()`, and `Engine.write_outputs()` expose the
  pipeline stages individually for interactive and advanced use cases.

- **`extract_env_references()` utility.** Scans raw YAML text for
  `{{ env.VAR }}` patterns before Jinja resolution, enabling automatic
  env-var discovery.

---

## [0.1.9] — 2026-05-19

### Added

- **Formal `typing.Protocol` interfaces for four pluggable seams.**
  `DeployAdapter`, `RegistryBackend`, `LineageBackend`, and `AuthBackend`
  in `ubunye.interfaces` define structural contracts that backends satisfy
  without inheriting from a base class. Cross-boundary dataclasses
  (`DeployContext`, `DeployResult`, `ModelVersionInfo`, `LineageRecord`,
  `Credentials`) accompany each protocol so callers never depend on a
  concrete backend's internal types.

- **Background metadata worker (Decision 1: non-blocking writes).**
  `ubunye._internal.MetadataWorker` dispatches lineage and registry
  metadata writes to a daemon thread with a bounded queue (default 1 000,
  configurable via `UBUNYE_METADATA_QUEUE_SIZE`). Queue overflow drops the
  oldest pending write and logs a warning. Flush timeout configurable via
  `UBUNYE_METADATA_FLUSH_TIMEOUT` (default 30 s). Uses a worker-thread
  pattern — async/await fights Spark's threading model.

- **Graceful degradation with fallback manifests (Decision 2).** When a
  metadata write fails, the record is appended to
  `~/.ubunye/fallback/{run_id}/{kind}.jsonl`. Pipeline execution
  continues. Auth failures are excluded — they propagate immediately.

- **`ubunye sync` CLI command.** Replays fallback manifests against
  configured backends with idempotent deduplication (key:
  `run_id + task + recorded_at`). Sub-commands: `ubunye sync lineage`,
  `ubunye sync registry`. Processed manifests are archived to
  `~/.ubunye/fallback/synced/`.

- **Entry-point discovery for backend groups.** Four new entry-point
  groups in `pyproject.toml`: `ubunye.deploy_adapters`,
  `ubunye.registry_backends`, `ubunye.lineage_backends`,
  `ubunye.auth_backends`. Third-party packages register backends by
  adding entry points under these groups.

- **Environment-based auto-detection** (`ubunye._internal.auto_detect`).
  Registry: MLflow on Databricks, filesystem elsewhere. Lineage: Delta
  when `UBUNYE_LINEAGE_TABLE` is set, filesystem otherwise. Auth: service
  principal when both `DATABRICKS_CLIENT_ID` and
  `DATABRICKS_CLIENT_SECRET` are set, token when `DATABRICKS_TOKEN` is
  set, raises `AuthNotFoundError` otherwise.

- **Schema evolution (Decision 3).** Every cross-boundary dataclass
  separates strict core fields from a flexible `metadata: Dict[str, str]`.
  Core fields never change without a major version bump. Every record
  stamps `engine_version`. Extra metadata keys survive round-trip
  through the filesystem lineage store.

- **Interfaces documentation page** (`docs/interfaces.md`) covering
  protocols, dataclasses, design principles, discovery, auto-detection,
  fallback manifests, and how to write a custom backend.

- **39 conformance and failure-mode tests** covering protocol isinstance
  checks, entry-point discovery, auto-detect logic, lineage/registry
  backend surfaces, non-blocking write timing, queue overflow, fallback
  manifest creation, sync dedup, auth propagation, and schema evolution
  metadata flexibility.

- **`ServicePrincipalAuthBackend`** — OAuth M2M authentication using
  `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`. Entry point
  `ubunye.auth_backends:service_principal`. Takes priority over token
  auth when both are available (via auto-detect).

- **`MLflowRegistryBackend`** — model registry that combines filesystem
  storage with MLflow experiment/run logging. Logs metrics, params, and
  stage transitions to MLflow when installed; falls back gracefully when
  MLflow is unavailable. Entry point `ubunye.registry_backends:mlflow`.

- **`DeltaLineageBackend`** — lineage backend targeting Delta tables via
  Spark SQL on Databricks. Falls back to JSONL-based local storage when
  no active SparkSession is available, enabling unit tests without Spark.
  Entry point `ubunye.lineage_backends:delta`.

- **24 Phase 2 backend tests** covering service principal auth (protocol
  conformance, env var resolution, error cases, entry-point discovery),
  MLflow registry (full CRUD lifecycle, promotion gates, metadata
  round-trip), and Delta lineage (record/get/search, metadata
  preservation, not-found errors).

- **Performance benchmark suite** (`benchmarks/bench_engine.py`). Nine
  benchmarks covering config loading (plain and Jinja), entry-point
  discovery (cached and cold), lineage I/O (write, read, search),
  metadata worker throughput, and registry registration. Reports ops/sec,
  p50/p99 latencies, mean, and stdev. Results saved to
  `benchmarks/results.json` for before/after comparison.

### Changed

- **Filtered entry-point loading.** `_load_group()` in
  `ubunye._internal.discovery` now calls
  `importlib.metadata.entry_points(group=group)` instead of loading all
  groups and filtering. Cached discovery is near-zero cost.

- **O(1) lineage lookup by run ID.** `FileSystemLineageStore.get_run()`
  uses a `run_id→Path` index instead of `rglob("*.json")`. The index is
  built lazily on first lookup and updated incrementally on writes.
  ~34% faster reads.

- **In-memory RunContext cache for lineage search.** Parsed RunContext
  objects are cached on first load and reused by subsequent `search()`
  and `list_runs()` calls, avoiding repeated JSON parsing and disk I/O.
  ~51% faster searches across 100 records.

### Fixed

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
