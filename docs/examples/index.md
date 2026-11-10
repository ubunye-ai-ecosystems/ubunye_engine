# Examples: start here

These examples live in the companion repository
[ubunye-ai-ecosystems/ubunye-examples](https://github.com/ubunye-ai-ecosystems/ubunye-examples).
This section is organised by what you want to do: ingest data, build machine
learning, keep models honest, work with language models.

Each example on these pages has a row of tabs: **Databricks**, **Laptop**,
**Docker** and **Kubernetes**. Each tab shows how to run that example in that
place. The tabs remember your choice: click Databricks once and every example
on every page shows the Databricks way. The pipelines themselves never change
between tabs. That is the whole point of the engine.

[**ubunye-ai-ecosystems/ubunye-examples** on GitHub](https://github.com/ubunye-ai-ecosystems/ubunye-examples){ .md-button .md-button--primary }

**Which engine version?** The examples repo installs the engine from PyPI and
pins the release its CI last tested: `ubunye-engine==0.7.0` (in its
`requirements.txt` and Docker image). The current engine is 0.9.0. The commands
below keep the tested pin. To try the examples on 0.9.0, install
`"ubunye-engine[spark,ml]==0.9.0"` instead; that pairing has not been through
the examples' CI yet. What changed between versions: [Upgrading](../upgrading.md).

## More examples in the engine repo

More examples ship inside the engine repository, under
[`examples/`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples).
Each runnable one has its own README with the commands:

| Folder | What it shows | Runs on |
|---|---|---|
| [`proving/c01_portable_etl`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/proving/c01_portable_etl) | one Narwhals transform, the same rows on pandas and Spark, checked with `ubunye prove` | laptop, no Java with pandas |
| [`real-world/food_prices_africa`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/real-world/food_prices_africa) | WFP food prices: clean, then flag price jumps | laptop, no Java |
| [`real-world/olist_ecommerce`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/real-world/olist_ecommerce) | nine raw tables to an order fact table, with input contracts, quarantine and reconcile | laptop, no Java |
| [`production/titanic_local`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/titanic_local) | the Titanic task on Spark and pandas, same run record hashes | laptop |
| [`production/titanic_multitask_local`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/titanic_multitask_local) | two chained tasks on Spark and pandas | laptop |
| [`production/llm_replay`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/llm_replay) | model calls replayed from a committed file: no key, no network, no cost | laptop, no Java |
| [`production/titanic_databricks`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/titanic_databricks), [`titanic_multitask_databricks`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/titanic_multitask_databricks), [`jhb_weather_databricks`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/jhb_weather_databricks), [`titanic_ml_databricks`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/production/titanic_ml_databricks) | the same tasks as [Databricks Asset Bundles](../deployment/databricks.md#databricks-asset-bundles) on serverless compute, writing to Unity Catalog | Databricks |
| [`python_api`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/python_api) | `ubunye.run_task()` and `ubunye.run_pipeline()` from Python | laptop with Spark, or Databricks |
| [`titanic_end_to_end.ipynb`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/blob/main/examples/titanic_end_to_end.ipynb) | the model lifecycle: train, gates, registry, rollback, lineage | a notebook, no Spark |
| [`fraud_detection/claims/claim_etl`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/fraud_detection/claims/claim_etl) | a sample config and transform: read a Hive table, write to S3. A shape to copy, not a runnable demo (it needs your own `fraud_db.raw_claims` table) | your Spark cluster |
| [`rest_api/customer_sync`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples/rest_api/customer_sync) | a sample `config.yaml` for the REST connector: bearer token, paging, rate limits. A shape to copy; it points at a placeholder API | reference only |

## Before you start, once per environment

=== "Databricks"

    Sign up for Databricks Free Edition (no card). In the workspace choose
    Create, then Git folder, and paste the examples repository URL. It is
    public, so you need no token. Every example is then a notebook you open
    and run.

=== "Laptop"

    You need Python, Java 17 and an internet connection. (This setup uses
    Spark 3.5, which runs on Java 8, 11 or 17; see
    [Installation](../getting_started/install.md).)

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
them produced the same rows (the hash ignores row order). See [Run anywhere](run_anywhere.md).
