# Developer Guide

Architecture deep-dive and extension patterns for contributors and advanced users.

---

## Architecture overview

```
Entry points:
    CLI (ubunye run)  --+
    Python API        --+
                        |
                        v
                    load_config()         YAML + Jinja2, then Pydantic v2
                        |
                        v
                    task_runner.execute_user_task()
                        |
                        v
                    Engine.run(cfg)
                        +-- Backend.start()   spark, databricks, pandas or a plugin
                        +-- registry.readers[format]().read()     [per input]
                        +-- Transform.apply()                     [the user's Task]
                        +-- registry.writers[format]().write()    [per output]
                        +-- Backend.stop()
                        |
                        v
                    LineageRecorder (if --lineage), as a hook
```

---

## Config loading pipeline

1. **Read raw YAML**: `load_config(path, variables)` in `ubunye/config/loader.py`.
2. **Jinja2 render**: `resolve_config(raw, cli_vars=...)` renders every string value.
   Variables come from `-dt`, `-dtf`, `-m` (as `dt`, `dtf`, `mode`), `--var` flags,
   `os.environ` (as `env.*`), and `task_dir` (the task's own folder).
3. **Pydantic validation**: the rendered YAML is parsed into `UbunyeConfig`.
   Validation is strict; an unknown field raises `ConfigFieldError` with a suggestion.
4. **Profile merge**: `UbunyeConfig.merged_spark_conf(mode)` merges base and profile Spark conf.
   A mode that names no profile raises `ConfigProfileError` when the config has profiles.

Key files:

- `ubunye/config/loader.py`: `load_config`
- `ubunye/config/resolver.py`: the Jinja resolver
- `ubunye/config/schema.py`: all Pydantic models

---

## Plugin registry

`ubunye.core.runtime.Registry.from_entrypoints()` discovers readers, writers and
transforms via `importlib.metadata.entry_points`. Other groups are loaded where
they are used.

```text
# Entry point groups
ubunye.readers     Reader subclasses
ubunye.writers     Writer subclasses
ubunye.transforms  Transform subclasses
ubunye.backends    Backend subclasses (see Execution backends)
ubunye.hooks       Hook classes (loaded when UBUNYE_TELEMETRY is set)
ubunye.monitors    Monitor implementations (CONFIG.monitors)
ubunye.ml          ML model adapters
ubunye.secrets     secret:// stores
```

`registry.readers["hive"]` returns the class registered as `hive`.
The class is instantiated fresh for each read or write.

---

## Engine runtime

`Engine.run(cfg)` in `ubunye/core/runtime.py`:

1. Check the config before anything runs: inputs and outputs, transforms exist,
   and the backend can do what the task needs (`BackendCapabilityError` if not).
2. Build the hook chain (defaults + `extra_hooks`, or `hooks=` override).
3. Open `chain.task(ctx, cfg, state)`, which wraps the entire run.
4. If `manage_backend=True`, start the backend.
5. Read each input in a `Reader:<format>` step: `Reader.read(io_cfg, backend)`.
6. Apply the transform in a `Transform:<type>` step: `Transform.apply(...)`.
7. Check the output expectations (`ExpectationError` stops the run before any write).
8. Write each output in a `Writer:<format>` step: `Writer.write(...)`.
9. Populate `state["outputs"]` so hooks can inspect them on exit.
10. If `manage_backend=True`, stop the backend.

`task_runner.execute_user_task` puts `task_dir` on `sys.path` for the duration
of the run, so `transformations.py` can do `from model import ...`.

---

## Backends

### SparkBackend

`ubunye/backends/spark_backend.py`:

- Lazily imports `pyspark`: no import error if PySpark is not installed and Spark is not used.
- Implements a context manager (`with SparkBackend(app_name=..., conf={...}) as be:`).
- `be.spark`: the `SparkSession`.
- `be.conf_effective`: dict of active configuration.
- Safe for multiple `start()` calls (idempotent).
- `stop()` stops the session only if `start()` created it. A session that was
  already running belongs to whoever started it.

### DatabricksBackend

`ubunye/backends/databricks_backend.py`:

- Wraps an **existing** SparkSession instead of creating a new one.
- If no session is passed explicitly, retrieves the active session via `SparkSession.getActiveSession()`.
- `start()` attaches to the active session (or no-ops if already attached).
- `stop()` is a **no-op**: we do not own the session, so we never stop it.
- Use this on Databricks where a SparkSession is always available.

### Auto-detection in the Python API

`ubunye.run_task()` and `ubunye.run_pipeline()` auto-detect the backend:

1. If `backend=` is passed (a name such as `"pandas"`, or a `Backend` instance), that backend
2. If `spark=` is passed explicitly, `DatabricksBackend(spark=session)`
3. If an active SparkSession exists, `DatabricksBackend`
4. Otherwise the default backend (`spark`) with config-driven Spark conf

Passing both `backend=` and `spark=` is an error. See
[ADR 003](architecture/adr-003-backend-resolution.md).

---

## Python API

`ubunye/api.py` exposes `run_task()` and `run_pipeline()`: the same execution
pipeline as the CLI but callable from Python code (notebooks, scripts, tests).

Key differences from the CLI path:

- **Backend auto-detection**: picks `DatabricksBackend` if an active session exists, otherwise the default backend, unless `backend=` names one.
- **No subprocess**: runs in-process, so the caller can inspect returned DataFrames directly.
- **Shared session lifecycle**: when using `DatabricksBackend`, the session stays alive after the run.

Both functions (and `notebook()`) are re-exported from `ubunye.__init__`:

```python
import ubunye
outputs = ubunye.run_task(task_dir="...", mode="DEV")

# Inject custom hooks (notebook/script use only)
from my_pkg.hooks import SlackAlertHook
outputs = ubunye.run_task(task_dir="...", hooks=[SlackAlertHook("#alerts")])
```

### Unified execution: `task_runner.execute_user_task`

`api.py`, `cli/main.py run`, and `cli/test_cmd.py run` all delegate to
`ubunye.core.task_runner.execute_user_task()`. It loads the user's `Task`
subclass from `transformations.py`, wraps it as an ephemeral Transform plugin
(`_ubunye_user_task`), and runs the resulting config through `Engine`. One code
path means hooks, lineage, and lifecycle behave identically across all three
entry points.

The `Engine` exposes two flags that this helper relies on:

- `manage_backend=False`: the caller owns `backend.start()` / `backend.stop()`,
  letting the CLI share one Spark session across many tasks.
- `extra_hooks=[...]`: appended to the default hook set. Used to attach a
  `MonitorHook` wrapping the optional `LineageRecorder` without replacing the
  built-in telemetry hooks.

---

## Lineage system

`ubunye/lineage/`:

| Module | Responsibility |
|---|---|
| `context.py` | `RunContext` and `StepRecord`: run ID, task metadata, timestamps, facts per input and output |
| `recorder.py` | `LineageRecorder`: `task_start` / `task_end`, run as a hook through `MonitorHook` |
| `storage.py` | `FileSystemLineageStore`: reads and writes JSON under `.ubunye/lineage/` |
| `content_hash.py` | The content hash (`rows-v1`): every row, any order, the same on Spark and pandas |
| `source_version.py` | The version of each input source when it is read and when it is hashed |
| `openlineage.py` | OpenLineage events from run records |
| `focus.py` | FOCUS cost rows for a run's model calls |

The `LineageRecorder` is wrapped in a `MonitorHook` and called at task start and end.
On task end it writes the run record to
`.ubunye/lineage/<usecase>/<package>/<task>/<run_id>.json`. See
[ADR 006](architecture/adr-006-run-record.md).

---

## Observability (hooks)

The Engine observes task runs through **hooks**: a single abstraction that
wraps every task and every step in a context manager. The Engine itself does
not know about Prometheus, OpenTelemetry, MLflow, or event logs.

```python
class Hook:
    @contextmanager
    def task(self, ctx, cfg, state): yield

    @contextmanager
    def step(self, ctx, name, meta): yield
```

`ubunye/core/hooks.py` defines `Hook` and `HookChain` (multiplexer).
`ubunye/telemetry/hooks/` ships four built-in hooks (plus `MonitorHook`, which wraps one monitor such as the `LineageRecorder`):

| File | Hook | Wraps |
|---|---|---|
| `hooks/events.py` | `EventLoggerHook` | JSON Lines event log |
| `hooks/otel.py` | `OTelHook` | OpenTelemetry spans |
| `hooks/prometheus.py` | `PrometheusHook` | Prometheus counters/histograms |
| `hooks/monitors.py` | `LegacyMonitorsHook` | User monitors from `CONFIG.monitors` (MLflow etc.) |

Environment flags still apply:

- `UBUNYE_TELEMETRY=1`: enable every hook registered under `ubunye.hooks` (the three
  built-in telemetry hooks, and any a third party ships)
- `UBUNYE_PROM_PORT=8000`: start a Prometheus HTTP endpoint

Legacy user monitors (`CONFIG.monitors`) run independently of the telemetry
flag, as before.

Override the hook set directly:

```python
from ubunye.core.runtime import Engine
from my_package.hooks import SlackAlertHook

engine = Engine(hooks=[SlackAlertHook(channel="#data-alerts")])
engine.run(cfg)
```

See the [Hooks guide](patterns/hooks.md) for writing custom hooks.

---

## ML architecture

Two separate ML systems coexist:

| System | Location | Purpose |
|---|---|---|
| Internal wrappers | `ubunye/plugins/ml/` | `SklearnModel`, `SparkMLModel`: engine-owned adapters |
| User contract | `ubunye/models/` | `UbunyeModel` ABC: user-implemented; the engine never imports ML libs |

**`UbunyeModel`** (`ubunye/models/base.py`) is the only interface the engine calls.
**`ModelTransform`** (`ubunye/plugins/transforms/model_transform.py`) loads the user's class
dynamically via `load_model_class()` and calls `train()` or `predict()`.

The `ModelRegistry` (`ubunye/models/registry.py`) stores artifacts and metadata on the
filesystem. JSON serialization uses `dataclasses.asdict()`.

---

## Writing tests

### Unit tests (Spark-free)

Use `MockDF` for any test that would otherwise require PySpark:

```python
class MockDF:
    def __init__(self, rows=None):
        self._rows = rows or [{"id": 1, "val": 2.0}]
    def count(self): return len(self._rows)
    def toPandas(self):
        import pandas as pd; return pd.DataFrame(self._rows)
```

All tests in `tests/unit/` must pass without `pyspark` installed.

### Integration tests

Mark with `@pytest.mark.integration` and use `SparkBackend`:

```python
import pytest
from ubunye.backends.spark_backend import SparkBackend

@pytest.fixture(scope="session")
def spark():
    with SparkBackend(app_name="test", conf={"spark.sql.shuffle.partitions": "1"}) as be:
        yield be.spark

@pytest.mark.integration
def test_hive_reader(spark):
    ...
```

Run:

```bash
pytest tests/ -m integration
```

---

## Orchestration exporters

`ubunye/orchestration/`:

- `base.py`: `OrchestratorExporter` ABC with
  `export(config_path, *, output_path, options) -> Path`.
- `airflow_exporter.py`: `AirflowExporter`, generates a DAG Python file (Airflow
  2.4+ and 3) whose single `BashOperator` runs `ubunye run ...`.
- `databricks_exporter.py`: `DatabricksExporter`, generates a Databricks Jobs
  API spec (`job.json`).
- `spark_pipeline_exporter.py`: `SparkPipelineExporter`, writes the task as a
  Spark Declarative Pipeline (Spark 4.1+).

Exporters are reached via `ubunye export airflow|databricks|spark-pipeline`. The CLI
(`ubunye/cli/export.py`) loads the task's `config.yaml`, dumps the
`ORCHESTRATION` block to a plain dict, and hands it to the exporter as
``options``. For the Databricks exporter the nested ``databricks`` cluster block
is flattened into top-level options so the exporter can splat it into
``new_cluster``. They do **not** interact with the running cluster: they only
produce configuration artifacts.

---

## Adding a new orchestration target

1. Subclass `OrchestratorExporter` and implement `export(config_path, *,
   output_path, options)`.
2. Add a new value to the `OrchestrationType` enum in `ubunye/config/schema.py`
   (it already lists `prefect` and `dagster`, which have no exporter yet).
3. Export the class from `ubunye/orchestration/__init__.py`.
4. Add a new Typer command in `ubunye/cli/export.py` that maps
   `OrchestrationConfig` fields to the exporter's `options`.
