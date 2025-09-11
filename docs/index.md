# Ubunye Engine

Ubunye Engine is a **config-first, Spark-native ETL + ML framework**.

- Define jobs using **YAML configs** + optional **Python transforms**.
- Run locally, on-prem, or in the cloud (Databricks, EMR, Glue).
- Extend easily with plugins: Readers, Writers, Transforms, ML backends.
- Orchestrate with Airflow, Databricks, Prefect, or Dagster using exporters.

---

## ✨ Features
- **Config-first**: all I/O, compute, orchestration is YAML-driven.
- **Plugin system**: extend via entrypoints (`ubunye.readers`, `writers`, `transforms`).
- **Multi-backend**: works with Spark, Pandas, or custom.
- **Telemetry-ready**: hooks for Prometheus, OpenTelemetry, JSON event logs.
- **ML-friendly**: unified BaseModel API for sklearn, PyTorch, Spark ML.
- **Orchestration**: export jobs for Airflow, Databricks, Prefect, Dagster.

---

## 📚 Documentation structure
- [Installation](installation.md) – how to install & verify
- [Overview](overview.md) – concepts & architecture
- [CLI](cli.md) – all commands
- [Config Reference](config_reference.md) – YAML schema
- [Plugins](plugins.md) – built-in and how to extend
