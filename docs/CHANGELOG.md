# Changelog

All notable changes to **Ubunye Engine** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),  
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Initial ML base classes (`BaseModel`, `SklearnModel`, `SparkMLModel`).
- JDBC, Delta, and Unity Catalog readers/writers.
- Telemetry modules (`events`, `prometheus`, `otel`).
- Orchestration exporters for Airflow & Databricks.
- CLI commands: `init`, `run`, `plan`, `export`, `config`, `plugins`, `doctor`, `version`.

### Changed
- Runtime engine now supports **transform chains** (list of transforms).
- `SparkBackend` improved with context manager and safe multiple starts.

### Fixed
- N/A

---

## [0.1.0] – 2025-09-11
### Added
- First alpha release of Ubunye Engine.
- Config-first ETL framework built on Spark.
- Plugin system for Readers, Writers, Transforms.
- CLI scaffolding (`ubunye init`).
- Example `fraud_detection/claims/claim_etl` task.
- Basic orchestration scaffolding.
