# API Reference

Run Ubunye tasks from Python instead of the CLI: in a notebook, a job, or a test.
This page lists the public classes and functions, generated from their docstrings.

## Which API do I use?

| You want to | Use | It returns |
|---|---|---|
| Run one task, end to end | `ubunye.run_task(task_dir, ...)` | `{output name: frame}`: what the task wrote, as the backend's own frames (a pandas DataFrame on pandas, a Spark DataFrame on Spark) |
| Run several tasks of one pipeline, in order | `ubunye.run_pipeline(usecase_dir, usecase, pipeline, tasks, ...)` | `{task name: {output name: frame}}`, without the tasks `resume=True` skipped |
| Step through a task in a notebook: read, look, transform, look, write | `ubunye.notebook(task_dir, ...)` | A `NotebookContext`: `read()` gives `{input name: frame}`, `transform()` gives `{output name: frame}`, `write()` writes them, `run()` does all three |
| Drive the engine yourself from a parsed config | `ubunye.core.runtime.Engine` | `Engine.run(cfg)` gives `{output name: frame}` (`None` with `dry_run=True`) |

Most people want `run_task`. All of them take `mode` (the profile: `DEV`,
`PROD`, ...; default `DEV`) and `backend` (`"pandas"`, `"spark"`, ...). See
[Execution backends](backends.md) and [Profiles](config/engine.md).

---

## Python API

The main use: Databricks notebooks and jobs, where a SparkSession already exists
and the engine attaches to it.

```python
import ubunye

# Run a single task
outputs = ubunye.run_task(
    task_dir="pipelines/fraud_detection/ingestion/claim_etl",
    mode="PROD",
    dt="202510",
)

# The same task with no Spark and no Java
outputs = ubunye.run_task("pipelines/sales/etl/daily", backend="pandas")

# Run multiple tasks in order
results = ubunye.run_pipeline(
    usecase_dir="pipelines",
    usecase="fraud_detection",
    pipeline="ingestion",
    tasks=["claim_etl", "feature_engineering"],
    mode="PROD",
    dt="202510",
)
```

::: ubunye.api.run_task
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.api.run_pipeline
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

### Job health and upstream checks

Queries over run records ([ADR 013](architecture/adr-013-receipt-queries.md)).

::: ubunye.api.job_health
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

::: ubunye.api.wait_for_upstream
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

---

## Notebook API

A Databricks job notebook is three steps inside one `with` block:

```python
import ubunye

with ubunye.notebook(task_dir, dt=dt) as run:
    sources = run.read()
    outputs = run.transform(sources)
    run.write(outputs)
```

The block is one run ([ADR 016](architecture/adr-016-notebook-as-a-run.md)):

- **Run records** are on when a location is configured: `run_records=`, else the
  job parameter `ubunye_run_records`, else `$UBUNYE_RUN_RECORDS`. A table name
  (`main.ops.ubunye_runs`) goes to Delta, a path to files. The first save creates
  the table; if the job's user may not, the warning says to create it once or
  grant CREATE TABLE. `lineage=False` turns records off.
- **Every failure is recorded** with the stage it failed in, as `failed_stage`:
  `setup`, `read`, `expectations`, `transform`, `write`, `notebook` (your code
  between the steps), or a name you give with `with run.step("score"):`.
- **One line per step** is printed (`quiet=True` stops it):

```text
ubunye: read 1 input (claims) in 2.10s
ubunye: transform 1 output (bronze_claims) in 0.40s
ubunye: write 1 output (bronze_claims) in 5.02s
ubunye: run 3a1b7ed8 success in 7.60s; run record: delta main.ops.ubunye_runs
```

- `dt=""` (an empty job parameter) counts as not given; then the job parameter
  `dt`, else `$UBUNYE_DT`. `mode` not given is the job parameter `ubunye_mode`,
  else `$UBUNYE_MODE`, else `DEV`.
- A backup copy of a record the store could not save goes to the folder in
  `ubunye_run_records_fallback` (a Unity Catalog Volume), only when one is set;
  otherwise there is a warning and the job goes on.
- `{{ env.VAR }}` values in config.yaml come from widgets and secrets, with no
  manual `os.environ` setup.

`run_task` and `run_pipeline` read the same settings (`run_records=`, the job
parameter, the environment) and record the same stage names.

Without `with`, the notebook works as before, for stepping through a task in
cells; each `write()` leaves a run record when records are on:

```python
ctx = ubunye.notebook(
    "/Workspace/pipelines/claims/claim_etl",
    mode="PROD",
    dt="2026-01-01",
)

sources = ctx.read()
sources["raw_claims"].display()

outputs = ctx.transform(sources)
outputs["bronze_claims"].show()

ctx.write(outputs)
```

::: ubunye.notebook.notebook
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.notebook.NotebookContext
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3
      members:
        - read
        - transform
        - write
        - run
        - step
        - run_id
        - run_records
        - config
        - config_dict
        - env_vars
        - spark
        - task_name
        - close

---

## Core Engine

::: ubunye.core.runtime.Engine
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Interfaces (Protocols)

See the full [Interfaces](interfaces.md) page for design principles,
discovery, and auto-detection.

::: ubunye.interfaces
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Config

::: ubunye.config.schema.UbunyeConfig
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.config.schema.TaskConfig
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.config.schema.IOConfig
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.config.schema.EngineConfig
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.config.schema.RegistryConfig
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.config.schema.ModelTransformParams
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Backends

Backends are plugins, chosen by name. See [Execution backends](backends.md) for
how to choose one and write your own.

::: ubunye.core.backends
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3
      members: [resolve, create, load_class, available, DEFAULT_BACKEND]

::: ubunye.core.capabilities.Capabilities
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

The shipped backends (`spark`, `databricks`, `pandas`): what each is for and
how each reads and writes is on [Execution backends](backends.md).

::: ubunye.backends.spark_backend.SparkBackend
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.backends.databricks_backend.DatabricksBackend
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.backends.pandas_backend.PandasBackend
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Models (UbunyeModel contract)

::: ubunye.models.base.UbunyeModel
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.models.loader.load_model_class
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Model Registry

::: ubunye.models.registry.ModelRegistry
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.models.registry.ModelVersion
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.models.registry.ModelStage
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Promotion Gates

::: ubunye.models.gates.PromotionGate
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.models.gates.GateResult
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Lineage

::: ubunye.lineage.recorder.LineageRecorder
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.lineage.context.RunContext
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.lineage.storage.FileSystemLineageStore
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Plugins: Readers

::: ubunye.plugins.readers.rest_api.RestApiReader
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Plugins: Writers

::: ubunye.plugins.writers.rest_api.RestApiWriter
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Plugins: Transforms

::: ubunye.plugins.transforms.model_transform.ModelTransform
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Internal ML wrappers

!!! note
    These are internal wrappers used by Ubunye's own sklearn/Spark ML adapters.
    User-defined models should implement `UbunyeModel`, not these classes.

::: ubunye.plugins.ml.base.BaseModel
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.plugins.ml.sklearn.SklearnModel
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

::: ubunye.plugins.ml.pysparkml.SparkMLModel
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3
