# Installation

Ubunye Engine requires **Python 3.9 – 3.11**.

---

## Core install

```bash
pip install ubunye-engine
```

This installs the CLI, config loader, plugin registry, and lineage tracker.
PySpark and ML backends are **optional** extras.

---

## Install extras

=== "Spark"

    ```bash
    pip install "ubunye-engine[spark]"
    ```

    Adds `pyspark`. Required to run `format: hive`, `format: delta`, and `format: unity` connectors.

=== "ML"

    ```bash
    pip install "ubunye-engine[ml]"
    ```

    Adds `scikit-learn`, `mlflow`, and the ML plugin wrappers.

=== "All extras"

    ```bash
    pip install "ubunye-engine[spark,ml]"
    ```

=== "Dev (contributors)"

    ```bash
    git clone https://github.com/ubunye-ai-ecosystems/ubunye_engine.git
    cd ubunye_engine
    pip install -e ".[dev,spark,ml]"
    pre-commit install
    ```

---

## Verify the install

```bash
ubunye version
```

Expected output:

```
Ubunye Engine v0.1.0
```

List all discovered plugins:

```bash
ubunye plugins
```

---

## System requirements

| Requirement | Minimum |
|---|---|
| Python | 3.9 |
| Java (for Spark) | 11 |
| Apache Spark | 3.3 |
| PySpark (optional) | 3.3 |

!!! tip "Databricks"
    On Databricks the cluster already has PySpark installed.
    Install only the core package on the driver:
    ```bash
    %pip install ubunye-engine
    ```

---

## Next steps

- [Quickstart →](quickstart.md)
- [Project structure →](structure.md)
