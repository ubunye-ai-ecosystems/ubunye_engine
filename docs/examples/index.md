# Examples: start here

This section is organised by what you want to do: ingest data, build machine
learning, keep models honest, work with language models. Inside every example
there is a small toggle showing how to run it on each environment.

The toggle remembers your choice. Click Databricks once and every example on
every page shows you the Databricks way. Click Laptop and everything switches.
The pipelines themselves never change between tabs. That is the whole point of
the engine.

The examples on these pages live in a companion repository. It installs the
engine from PyPI and pins the release it was tested with, `ubunye-engine==0.7.0`:

[**ubunye-ai-ecosystems/ubunye-examples** on GitHub](https://github.com/ubunye-ai-ecosystems/ubunye-examples){ .md-button .md-button--primary }

More examples ship inside the engine repository, under
[`examples/`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples).
Each has its own README with the commands to run it:

| Folder | What it shows | Runs on |
|---|---|---|
| `proving/c01_portable_etl` | one Narwhals transform, the same rows on pandas and Spark, checked with `ubunye prove` | laptop, no Java with pandas |
| `real-world/food_prices_africa` | WFP food prices: clean, then flag price jumps | laptop, no Java |
| `real-world/olist_ecommerce` | nine raw tables to an order fact table, with input contracts, quarantine and reconcile | laptop, no Java |
| `production/titanic_local` | the Titanic task on Spark and pandas, same run record hashes | laptop |
| `production/titanic_multitask_local` | two chained tasks on Spark and pandas | laptop |
| `production/llm_replay` | model calls replayed from a committed file: no key, no network, no cost | laptop, no Java |
| `production/titanic_databricks`, `titanic_multitask_databricks`, `jhb_weather_databricks`, `titanic_ml_databricks` | the same tasks as Asset Bundles on serverless with Unity Catalog | Databricks |
| `python_api` | `ubunye.run_task()` and `ubunye.run_pipeline()` from Python | laptop with Spark, or Databricks |
| `titanic_end_to_end.ipynb` | the model lifecycle: train, gates, registry, rollback, lineage | a notebook, no Spark |

## Before you start, once per environment

=== "Databricks"

    Sign up for Databricks Free Edition (no card). In the workspace choose
    Create, then Git folder, and paste the examples repository URL. It is
    public, so you need no token. Every example is then a notebook you open
    and run.

=== "Laptop"

    You need Python, Java 17 and an internet connection.

    ```bash
    git clone https://github.com/ubunye-ai-ecosystems/ubunye-examples
    cd ubunye-examples
    pip install "pyspark==3.5.3" "delta-spark==3.2.0" "ubunye-engine[spark,ml]==0.7.0" "requests"
    ```

    The scripts under `platforms/` handle Spark, Delta and a local metastore.
    Examples that read the Databricks sample tables need seeded stand-ins with
    the same shape, created once:

    ```bash
    . platforms/spark_env.sh && python platforms/seed.py
    ```

=== "Docker"

    You need Docker only. Build the image once:

    ```bash
    docker build -f platforms/docker/Dockerfile -t ubunye-portable:ci .
    ```

    The machine learning examples use a second image with the open source
    model weights baked in, so nothing is downloaded at run time:

    ```bash
    docker build -f platforms/docker/Dockerfile.ml --build-arg BASE=ubunye-portable:ci -t ubunye-ml:ci .
    ```

=== "Kubernetes"

    Any cluster works. For a disposable local one:

    ```bash
    kind create cluster --name ubunye
    kind load docker-image ubunye-portable:ci --name ubunye
    ```

    Job manifests live under `platforms/k8s/`.

## Why you can trust these pages

Every example has been run for real, with its output inspected. One test goes
further: it runs the same pipeline on the laptop, in Docker, on Kubernetes,
against object storage and on Databricks, then fails the build unless all of
them produced the same output, byte for byte. See [Run anywhere](run_anywhere.md).
