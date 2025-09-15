# Overview

Ubunye Engine simplifies ETL + ML by combining:

- **Configs** – YAML files define sources, transforms, and outputs.
- **Feature Classes** – optional Python code for custom transforms.
- **Plugins** – reusable building blocks for reading, writing, ML, orchestration.

---

## 🔄 Execution flow
1. **Inputs** – read from Hive, JDBC, Delta, Unity Catalog, etc.
2. **Transforms** – apply transforms (built-in or custom).
3. **Outputs** – write results to S3, JDBC, Delta, Unity Catalog.
4. **Telemetry** – events, metrics, traces (optional).
5. **Orchestration** – export configs to Airflow DAGs, Databricks Jobs, Prefect flows.

---

## 🏗 Architecture
- **Core**: engine runtime, config loader, registry.
- **Backends**: Spark (default), Pandas.
- **Plugins**: Readers, Writers, Transforms, ML models.
- **CLI**: `ubunye` commands for init, run, export, plan.
- **Orchestration**: exporters for Airflow, Databricks, etc.
- **Telemetry**: Prometheus, OpenTelemetry, event logs.
