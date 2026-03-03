# API Reference

Auto-generated from docstrings via [mkdocstrings](https://mkdocstrings.github.io/).

---

## Core Engine

::: ubunye.core.runtime.Engine
    options:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

## Interfaces

::: ubunye.core.interfaces
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

## Spark Backend

::: ubunye.backends.spark_backend.SparkBackend
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
