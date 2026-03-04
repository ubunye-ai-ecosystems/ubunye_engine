# Changelog

All notable changes to Ubunye Engine will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

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

- **Test infrastructure** — 288 unit tests, all Spark-free in `tests/unit/`.

### Changed

- `ubunye/config/schema.py` — added `RegistryConfig`, `ModelTransformParams`, `FormatType.REST_API`.
- `ubunye/cli/main.py` — mounted `models_app` and `lineage_app` Typer sub-apps.
- `pyproject.toml` — added `model` entry point under `ubunye.transforms`.

### Fixed

- N/A

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
