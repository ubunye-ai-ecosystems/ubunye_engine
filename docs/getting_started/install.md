# Installation

Ubunye Engine runs on **Python 3.10 to 3.13**, on Linux, Windows and macOS.
Every one of those is tested on every change.

---

## Core install

```bash
pip install ubunye-engine
```

This installs the CLI, config loader, plugin registry, and lineage tracker.
The engines that move data are **optional** extras: pick the one you need.

---

## Install extras

=== "pandas (no Java)"

    ```bash
    pip install "ubunye-engine[pandas]"
    ```

    Adds pandas and pyarrow. Runs a task on your laptop or in CI with no Spark
    and no Java, reading and writing exactly as Spark does. See
    [Execution backends](../backends.md).

=== "Spark"

    ```bash
    pip install "ubunye-engine[spark]"
    ```

    Adds `pyspark`. Required for `format: hive`, `format: delta`, and
    `format: unity`. Add `delta` for Delta Lake outside Databricks:
    `pip install "ubunye-engine[spark,delta]"`.

=== "ML"

    ```bash
    pip install "ubunye-engine[ml]"
    ```

    Adds `scikit-learn`, `mlflow`, and the ML plugin wrappers.

=== "Dev (contributors)"

    ```bash
    git clone https://github.com/ubunye-ai-ecosystems/ubunye_engine.git
    cd ubunye_engine
    pip install -e ".[dev]"
    pre-commit install
    ```

    See [Contributing](../contributing.md) for the test tiers.

---

## Verify the install

```bash
ubunye version
```

prints the version you installed, for example:

```
Ubunye Engine v0.6.0
```

See which engines you can run on, and what each can do:

```bash
ubunye backends
```

---

## System requirements

| Requirement | Tested |
|---|---|
| Python | 3.10, 3.11, 3.12, 3.13 |
| Operating system | Linux, Windows, macOS |
| pandas backend | pandas 2.2 or newer with pyarrow 14 or newer (24 or newer on Windows) |
| Spark backend | Spark 3.5 with Java 11, Spark 4 with Java 17 or 21 |

The oldest versions listed are the ones the package accepts, and CI installs
exactly those and runs the tests on them.

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
