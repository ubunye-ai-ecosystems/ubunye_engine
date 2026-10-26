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

Extras combine: `pip install "ubunye-engine[pandas,spark]"`. The others:

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
Ubunye Engine v0.8.0
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
