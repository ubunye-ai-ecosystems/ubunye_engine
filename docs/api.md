# API Reference

Auto-generated from docstrings via [mkdocstrings](https://mkdocstrings.github.io/).

---

## Python API

The public Python API for running Ubunye tasks without the CLI.
Primary use case: Databricks notebooks and jobs where a SparkSession already exists.

```python
import ubunye

# Run a single task
outputs = ubunye.run_task(
    task_dir="pipelines/fraud_detection/ingestion/claim_etl",
    mode="nonprod",
    dt="202510",
)

# The same task with no Spark and no Java
outputs = ubunye.run_task("pipelines/sales/etl/daily", backend="pandas")

# Run multiple tasks sequentially
results = ubunye.run_pipeline(
    usecase_dir="pipelines",
    usecase="fraud_detection",
    package="ingestion",
    tasks=["claim_etl", "feature_engineering"],
    mode="nonprod",
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

---

## Notebook API (Interactive)

Step-by-step execution for prototyping in Databricks notebooks.
Environment variables referenced via `{{ env.VAR }}` in config.yaml are
auto-resolved from widgets and secrets — no manual `os.environ` setup.

```python
import ubunye

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

### SparkBackend

Creates and manages a new SparkSession. Use for local development, CI, and non-Databricks environments.

::: ubunye.backends.spark_backend.SparkBackend
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

### DatabricksBackend

Reuses an active SparkSession instead of creating one. Use on Databricks where a session already exists.

::: ubunye.backends.databricks_backend.DatabricksBackend
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

### PandasBackend

Runs a task with pandas and pyarrow: no Spark and no Java. Reads and writes local
csv, json and parquet exactly as Spark does.

::: ubunye.backends.pandas_backend.PandasBackend
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

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

## Plugins — Readers

::: ubunye.plugins.readers.rest_api.RestApiReader
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Plugins — Writers

::: ubunye.plugins.writers.rest_api.RestApiWriter
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Plugins — Transforms

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
