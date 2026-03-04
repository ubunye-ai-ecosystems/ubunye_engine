# Developer Guide

Architecture deep-dive and extension patterns for contributors and advanced users.

---

## Architecture overview

```
CLI (Typer)
    └── ubunye run
            ↓
        ConfigLoader          ← YAML + Jinja2 → Pydantic v2
            ↓
        Engine.run(cfg)
            ├── SparkBackend.start()
            ├── PluginRegistry.get_reader(format) → Reader.read()  [per input]
            ├── PluginRegistry.get_transform(type) → Transform.apply()
            ├── PluginRegistry.get_writer(format) → Writer.write() [per output]
            └── SparkBackend.stop()
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

1. Merge Spark conf (base + profile).
2. Start `SparkBackend`.
3. For each input in `cfg.CONFIG.inputs`: call `Reader.read(io_cfg_dict, backend)`.
4. Call `Transform.apply(inputs_dict, transform_cfg_params, backend)`.
5. For each output in `cfg.CONFIG.outputs`: call `Writer.write(df, io_cfg_dict, backend)`.
6. Stop `SparkBackend`.
7. If `LineageRecorder` is attached (via monitors): write run record.

The `task_dir` is added to `sys.path` before step 4, so `transformations.py` and
`model.py` can be imported without explicit path configuration.

---

## SparkBackend

`ubunye/backends/spark_backend.py`:

- Lazily imports `pyspark` — no import error if PySpark is not installed and Spark isn't used.
- Implements context manager (`with SparkBackend(...) as be:`).
- `be.spark` — the `SparkSession`.
- `be.spark_conf` — dict of active configuration.
- Safe for multiple `start()` calls (idempotent).

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

## Telemetry

`ubunye/telemetry/` — all telemetry is opt-in via `UBUNYE_TELEMETRY=1`:

| Module | Backend |
|---|---|
| `events.py` | JSON Lines log file |
| `prometheus.py` | Prometheus counters and histograms (port `UBUNYE_PROM_PORT`) |
| `otel.py` | OpenTelemetry spans (OTLP or console exporter) |
| `mlflow.py` | MLflow run logging (experiment, metrics, params) |

All monitors implement the `Monitor` protocol:

```python
class Monitor(Protocol):
    def on_task_start(self, ctx: RunContext) -> None: ...
    def on_task_end(self, ctx: RunContext, success: bool) -> None: ...
```

Monitors are wrapped with `safe_call()` — a failing monitor never crashes the task.

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
