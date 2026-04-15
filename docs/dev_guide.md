# Developer Guide

Architecture deep-dive and extension patterns for contributors and advanced users.

---

## Architecture overview

```
Entry points:
    CLI (ubunye run)  ──┐
    Python API        ──┤
                        ↓
                    ConfigLoader          ← YAML + Jinja2 → Pydantic v2
                        ↓
                    Engine.run(cfg)
                        ├── Backend.start()     ← SparkBackend or DatabricksBackend
                        ├── Registry.get_reader(format) → Reader.read()  [per input]
                        ├── Registry.get_transform(type) → Transform.apply()
                        ├── Registry.get_writer(format) → Writer.write() [per output]
                        └── Backend.stop()
                                ↓
                        LineageRecorder (if --lineage)
```

---

## Config loading pipeline

1. **Read raw YAML** — `ConfigLoader.load(path, variables)`.
2. **Jinja2 render** — `ConfigResolver.resolve(raw_yaml, variables)` renders all string values.
   Variables come from: `--var` flags, `os.environ` (as `env.*`), and any extra context.
3. **Pydantic validation** — rendered YAML is parsed into `UbunyeConfig`.
   Strict validation; unknown top-level keys raise an error.
4. **Profile merge** — `UbunyeConfig.merged_spark_conf(profile)` merges base + profile Spark conf.

Key files:

- `ubunye/config/loader.py` — `ConfigLoader`
- `ubunye/config/resolver.py` — Jinja resolver
- `ubunye/config/schema.py` — all Pydantic models

---

## Plugin registry

`ubunye.core.registry.PluginRegistry` discovers plugins at startup via `importlib.metadata.entry_points`.

```python
# Entry point groups
ubunye.readers     →  Reader subclasses
ubunye.writers     →  Writer subclasses
ubunye.transforms  →  Transform subclasses
ubunye.monitors    →  Monitor implementations
```

`registry.get_reader("hive")` looks up the `hive` key and returns the class.
The class is instantiated fresh for each task run.

---

## Engine runtime

`Engine.run(cfg)` in `ubunye/core/runtime.py`:

1. Build the hook chain (defaults + `extra_hooks`, or `hooks=` override).
2. Open `chain.task(ctx, cfg, state)` — wraps the entire run.
3. If `manage_backend=True`, start the backend.
4. Read each input: `chain.step("read.<name>")` → `Reader.read(io_cfg, backend)`.
5. Apply the transform: `chain.step("transform")` → `Transform.apply(...)`.
6. Write each output: `chain.step("write.<name>")` → `Writer.write(...)`.
7. Populate `state["outputs"]` so hooks can inspect them on exit.
8. If `manage_backend=True`, stop the backend.

`task_runner.execute_user_task` adds `task_dir` to `sys.path` for the duration
of step 5 so `transformations.py` can do `from model import ...`.

---

## Backends

### SparkBackend

`ubunye/backends/spark_backend.py`:

- Lazily imports `pyspark` — no import error if PySpark is not installed and Spark isn't used.
- Implements context manager (`with SparkBackend(...) as be:`).
- `be.spark` — the `SparkSession`.
- `be.conf_effective` — dict of active configuration.
- Safe for multiple `start()` calls (idempotent).
- `stop()` terminates the session.

### DatabricksBackend

`ubunye/backends/databricks_backend.py`:

- Wraps an **existing** SparkSession instead of creating a new one.
- If no session is passed explicitly, retrieves the active session via `SparkSession.getActiveSession()`.
- `start()` attaches to the active session (or no-ops if already attached).
- `stop()` is a **no-op** — we don't own the session, so we never stop it.
- Use this on Databricks where a SparkSession is always available.

### Auto-detection in the Python API

`ubunye.run_task()` and `ubunye.run_pipeline()` auto-detect the backend:

1. If `spark=` is passed explicitly → `DatabricksBackend(spark=session)`
2. If an active SparkSession exists → `DatabricksBackend`
3. Otherwise → `SparkBackend` with config-driven Spark conf

---

## Python API

`ubunye/api.py` exposes `run_task()` and `run_pipeline()` — the same execution
pipeline as the CLI but callable from Python code (notebooks, scripts, tests).

Key differences from the CLI path:

- **Backend auto-detection**: picks `DatabricksBackend` if an active session exists, otherwise `SparkBackend`.
- **No subprocess**: runs in-process, so the caller can inspect returned DataFrames directly.
- **Shared session lifecycle**: when using `DatabricksBackend`, the session stays alive after the run.

Both functions are re-exported from `ubunye.__init__`:

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

- `manage_backend=False` — caller owns `backend.start()` / `backend.stop()`,
  letting the CLI share one Spark session across many tasks.
- `extra_hooks=[...]` — appended to the default hook set. Used to attach a
  `MonitorHook` wrapping the optional `LineageRecorder` without replacing the
  built-in telemetry hooks.

---

## Lineage system

`ubunye/lineage/`:

| Module | Responsibility |
|---|---|
| `context.py` | `RunContext` frozen dataclass — run ID, task metadata, timestamps |
| `recorder.py` | `LineageRecorder` — implements `Monitor` protocol; writes step records |
| `storage.py` | `FileSystemLineageStore` — reads/writes JSON under `.ubunye/lineage/` |
| `hasher.py` | `hash_dataframe()` — SHA-256 of sampled rows + schema (Spark-optional) |

The `LineageRecorder` is attached as a monitor and called at task start and end.
On task end it writes a `RunRecord` JSON file keyed by `run_id`.

---

## Observability (hooks)

The Engine observes task runs through **hooks** — a single abstraction that
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
`ubunye/telemetry/hooks/` ships four built-in hooks:

| File | Hook | Wraps |
|---|---|---|
| `hooks/events.py` | `EventLoggerHook` | JSON Lines event log |
| `hooks/otel.py` | `OTelHook` | OpenTelemetry spans |
| `hooks/prometheus.py` | `PrometheusHook` | Prometheus counters/histograms |
| `hooks/monitors.py` | `LegacyMonitorsHook` | User monitors from `CONFIG.monitors` (MLflow etc.) |

Environment flags still apply:

- `UBUNYE_TELEMETRY=1` — enable the three built-in telemetry hooks
- `UBUNYE_PROM_PORT=8000` — start Prometheus HTTP endpoint

Legacy user monitors (`CONFIG.monitors`) run independently of the telemetry
flag — same behavior as before.

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
| Internal wrappers | `ubunye/plugins/ml/` | `SklearnModel`, `SparkMLModel` — engine-owned adapters |
| User contract | `ubunye/models/` | `UbunyeModel` ABC — user-implemented; engine never imports ML libs |

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
    with SparkBackend(app_name="test", spark_conf={"spark.sql.shuffle.partitions": "1"}) as be:
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

- `base.py` — `OrchestrationExporter` ABC with `export(cfg, output_path)`.
- `airflow.py` — generates an Airflow DAG Python file.
- `databricks.py` — generates a Databricks Jobs API JSON.

Exporters read from `cfg.ORCHESTRATION` and `cfg.ENGINE` (for profile-specific cluster settings).
They do **not** interact with the running cluster — they only produce configuration artifacts.

---

## Adding a new orchestration target

1. Subclass `OrchestrationExporter`.
2. Implement `export(cfg: UbunyeConfig, output_path: str, profile: str)`.
3. Add a new value to `OrchestrationType` enum in `schema.py`.
4. Register in the CLI `export` command.
