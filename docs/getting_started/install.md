# Installation

How to install Ubunye Engine, pick the extras you need, and check it works. This is
the one page that lists the Python, Java and pandas versions it needs.

Ubunye Engine runs on **Python 3.10 or newer** (3.10 to 3.13 are tested on every
change), on Linux, Windows and macOS.

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

    Adds `scikit-learn` and `mlflow`, which the built-in ML wrappers and the MLflow
    registry use. `ml-torch` adds `torch` as well.

=== "Dev (contributors)"

    ```bash
    git clone https://github.com/ubunye-ai-ecosystems/ubunye_engine.git
    cd ubunye_engine
    pip install -e ".[dev]"
    pre-commit install
    ```

    See [Contributing](../contributing.md) for the test tiers.

Extras combine: `pip install "ubunye-engine[pandas,spark]"`.

!!! note "A warning you can ignore"
    With the `pandas` and `spark` extras together, pip may install pandas 3. PySpark
    then prints this when Spark starts:

    ```
    FutureWarning: PySpark does not yet fully support pandas >= 3.0.0. Some features may not work correctly. It is recommended to use pandas < 3.0.0 for now.
    ```

    This is expected: the run carries on and gives the same result (checked with
    pandas 3.1 and Spark 4.2).
    To silence it, install `"pandas<3"` alongside.

The other extras:

| Extra | Adds | For |
|---|---|---|
| `delta` | `delta-spark` 4 | Delta Lake on Spark outside Databricks |
| `rest` | `requests` | the `rest_api` connector |
| `objectstore` | `fsspec` | model artifacts on `s3://`, `gs://` (add `s3fs` or `gcsfs`) |
| `databricks` | `databricks-sdk` | `ubunye deploy databricks`, `secret://databricks` |
| `otel` | OpenTelemetry SDK and OTLP exporter | traces and metrics |
| `aws`, `gcp`, `azure` | each cloud's secret store client | `secret://aws-sm`, `secret://gcp-sm`, `secret://azure-kv` |
| `mcp` | `mcp` | `ubunye mcp`, the engine as an MCP server for agents |

---

## Verify the install

```bash
ubunye version
```

prints the version you installed, for example:

```
Ubunye Engine v0.9.0
```

See which engines you can run on, and what each can do:

```bash
ubunye backends
```

With only the `pandas` extra, part of it looks like this:

```
pandas
  features:     partitioned_writes, path_io, records
  file formats: csv, json, parquet
  write modes:  append, errorifexists, ignore, overwrite, overwrite_partitions
  distributed:  no   needs Java: no
```

`ubunye doctor` goes further: it checks the backends, the plugins and, given a task,
the environment variables its config needs, and says what would fail and why.

---

## System requirements

| Requirement | Tested |
|---|---|
| Python | 3.10, 3.11, 3.12, 3.13 |
| Operating system | Linux, Windows, macOS |
| pandas backend | pandas 2.2 or newer (pandas 3 works) with pyarrow 14 or newer (24 or newer on Windows) |
| Spark 4 backend | Java 17 or 21 |
| Spark 3.5 backend | Java 8, 11 or 17 |

The pandas backend needs no Java at all. To check for Java, run `java -version`; Spark
also finds it through `JAVA_HOME`. `ubunye doctor` warns when PySpark is installed and
no Java is found.

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
- [Concepts →](concepts.md)
- [Project structure →](structure.md)
- Something failed? [Troubleshooting →](../troubleshooting.md)
