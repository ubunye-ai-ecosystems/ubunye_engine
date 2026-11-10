---
title: Ubunye Engine
description: Write a data or ML task once, run it on pandas, Spark or Databricks, and prove the results match.
hide:
  - navigation
  - toc
---

<div class="ue-hero" markdown>

<div class="ue-hero__text" markdown>

# Ubunye Engine

<p class="ue-hero__lead">Ubunye runs your data and ML jobs from a small folder of YAML and Python, the same way on a laptop, a cloud or Databricks.</p>

<ul class="ue-promises">
  <li class="ue-promise ue-promise--once"><strong>Write it once.</strong> A task is one folder: a <code>config.yaml</code> and a <code>transformations.py</code>.</li>
  <li class="ue-promise ue-promise--anywhere"><strong>Run it anywhere.</strong> pandas with no Java, or Spark on your laptop, a cluster, AWS, GCP, Azure or Databricks.</li>
  <li class="ue-promise ue-promise--same"><strong>Prove it's the same.</strong> Every run can leave a run record with a hash of every row, so two runs can be compared exactly.</li>
</ul>

[Get started](getting_started/quickstart.md){ .md-button .md-button--primary }
[GitHub](https://github.com/ubunye-ai-ecosystems/ubunye_engine){ .md-button }

</div>

<figure class="ue-film">
  <video class="ue-film__video" controls preload="metadata" playsinline poster="assets/video/ubunye-intro-poster.jpg" aria-label="Ubunye Engine intro film, 64 seconds: write a task once, run it on pandas, Spark and the clouds, and prove the rows are the same">
    <source src="assets/video/ubunye-intro.mp4" type="video/mp4">
  </video>
</figure>

</div>

## Try it in 60 seconds

On pandas, so all you need is Python ([which version](getting_started/install.md)):
no Java, no cloud account.

```bash
pip install "ubunye-engine[pandas]"
ubunye init -d pipelines -u demo -p starter -t filter_adults
ubunye run -d pipelines -u demo -p starter -t filter_adults --backend pandas --lineage
ubunye lineage list -d pipelines -u demo -p starter -t filter_adults
```

`init` makes a task folder with eight sample people in a CSV. `run` keeps the
adults and writes them as Parquet. `lineage list` shows the run record the run left:

```
RUN_ID     STARTED              STATUS        DUR  ROWS
------------------------------------------------------------
a2da9fbd  2026-11-10T10:34:34  success      0.3s  in:8  out:5
```

Next: the [Quickstart](getting_started/quickstart.md) explains each step, then
runs the same folder on Spark.

---

## What you get

The same task, unchanged, has given the same rows on pandas, local Spark,
Kubernetes, AWS Glue, GCP Dataproc, Azure Container Apps and Databricks, checked
from the run records by the [proving ground](proving-ground/index.md)
([latest results](proving-ground/latest.md)).

New to the words below? [Concepts](getting_started/concepts.md) explains use case,
pipeline, task, profile, write mode, run record and backend in plain words.

- **Config first.** Inputs, outputs and Spark settings live in YAML, so no
  credentials or paths are hard coded. [Config overview](config/overview.md)
- **One config for every environment.** A **profile** (`DEV`, `PROD`) changes the
  Spark settings per environment, chosen with `-m`. Values can be filled in with
  **Jinja2**, a small template language: `{{ dt }}` becomes the run date.
  [Profiles](config/engine.md), [Jinja](config/jinja.md)
- **Connectors you declare, not code.** Hive, JDBC, files on S3 or local disk,
  Delta, REST APIs, binary files and **Unity Catalog** (Databricks' catalog of
  tables). [Connectors](connectors/overview.md)
- **Backends.** Run on Spark, Databricks, or pandas with no Java (`--backend`).
  `ubunye backends` lists what each can do. [Execution backends](backends.md)
- **Checks before a run.** `ubunye validate` (the config), `ubunye plan` (a dry
  run: what it will read and write) and `ubunye doctor` (this machine).
  [Which command catches what](config/overview.md)
- **Bad data stopped early.** Expectations check inputs, put bad rows aside
  (quarantine) and reconcile input with output. [Expectations](config/expectations.md)
- **Run records.** `--lineage` writes one **run record** per run to
  `.ubunye/lineage/`: what ran, on what data, with a hash of every row.
  `ubunye gate` and `ubunye prove` compare them. [Run records](guides/run-records.md)
- **Safe reruns.** A batch that was already appended is refused unless you pass
  `--rerun`; `--resume` finishes a pipeline that stopped half way.
  [Safe reruns](guides/safe-reruns.md)
- **Models.** A library-independent `UbunyeModel` contract and a built-in
  registry to version, promote, roll back and gate models.
  [Model contract](ml/model_contract.md), [Registry](ml/registry.md)
- **Observability.** Prometheus, OpenTelemetry, MLflow and JSON event logs
  through hooks (`ubunye.hooks`), and export of run records to **OpenLineage** (an open standard
  for sharing what a job read and wrote). [Hooks](patterns/hooks.md),
  [OpenLineage](patterns/openlineage.md)
- **Orchestration export.** One command writes an Airflow DAG, a Databricks job
  or a Spark Declarative Pipeline from the same config.
  [Orchestration](config/orchestration.md)
- **Deployment.** `ubunye deploy` to Databricks, AWS Glue, GCP Dataproc, EMR
  Serverless, Kubernetes and Azure Container Apps. [Deployment](deployment.md)
- **Plugins.** Add your own readers, writers, transforms and backends through
  **entry points** (a standard Python way for an installed package to announce
  plugins): `ubunye.readers`, `ubunye.writers`, `ubunye.transforms`,
  `ubunye.backends` and more. `ubunye plugins` lists what is installed.
  [Writing your own](connectors/plugin_guide.md)
- **Agents.** `ubunye mcp` serves the engine over **MCP** (Model Context
  Protocol, the way AI agents call tools). It only reads unless you pass
  `--allow-run`. [Agents over MCP](patterns/mcp.md)
- **Tested three ways.** A unit tier with no Java, a Spark tier, and a parity
  tier that checks Spark and pandas agree. [Contributing](contributing.md)

## What Ubunye is not

- **Not an agent framework.** Use LangChain or CrewAI for that.
- **Not an orchestrator.** Use Airflow, Prefect or Dagster. (`ubunye export`
  writes an Airflow DAG or a Databricks job for you.)
- **Not a compute engine.** It runs on Spark, or on pandas for laptop-sized data.

Ubunye is the layer between your data sources and your applications that makes
the plumbing the same everywhere, so you can focus on the logic.

## Two ways to run a task

=== "CLI"

    ```bash
    ubunye run -d pipelines -u fraud -p etl -t claims -m PROD --lineage
    ```

=== "Python API (Databricks)"

    ```python
    import ubunye

    outputs = ubunye.run_task(
        task_dir="pipelines/fraud/etl/claims",
        mode="PROD",
        dt="2024-06-01",
    )
    ```

The Python API finds and reuses an active SparkSession on Databricks. When no
session exists, it creates one, the same as the CLI. Pass `backend="pandas"` to
run with no Spark at all.

## A Spark example with profiles

This task reads a Hive table and writes Delta, so it **needs the Spark backend**
(Spark on your machine, a cluster or Databricks). For a pandas example, see
[Try it in 60 seconds](#try-it-in-60-seconds) above.

```yaml
# pipelines/fraud/etl/claims/config.yaml
MODEL: etl
VERSION: "1.0.0"

ENGINE:
  profiles:
    DEV:
      spark_conf:
        spark.sql.shuffle.partitions: "4"
    PROD:
      spark_conf:
        spark.sql.shuffle.partitions: "200"

CONFIG:
  inputs:
    raw_claims:
      format: hive
      db_name: raw
      tbl_name: claims
  transform: {}
  outputs:
    clean_claims:
      format: delta
      path: s3://data-lake/clean/claims
      mode: overwrite
```

```bash
ubunye plan -d pipelines -u fraud -p etl -t claims -m PROD   # what it will read and write
ubunye run  -d pipelines -u fraud -p etl -t claims -m PROD
```

`-m PROD` picks the `PROD` profile. Leave `-m` out and `run` uses `DEV`. Profile
names must match exactly, case included. [Profiles](config/engine.md)

## Documentation map

| Section | What's in it |
|---|---|
| [Installation](getting_started/install.md) | Install, check it works, extras, Python and Java versions |
| [Quickstart](getting_started/quickstart.md) | Your first task, end to end, in 5 minutes |
| [Concepts](getting_started/concepts.md) | The words the docs use, in plain language |
| [Tutorials](tutorials/01-local-parity.md) | Same result on pandas and Spark; a nine-table pipeline with data checks |
| [Writing tasks](config/overview.md) | The full `config.yaml` reference |
| [Connectors](connectors/overview.md) | Every built-in reader and writer |
| [Machine learning](ml/model_contract.md) | The `UbunyeModel` contract and the [registry](ml/registry.md) |
| [Examples](examples/index.md) | Working examples, grouped by what you want to do |
| [Deployment](deployment.md) | Databricks, AWS, GCP, Azure, Kubernetes |
| [How-to guides](patterns/notebook.md) | Notebooks, RAG, drift, REST ingestion, hooks, gates and more |
| [CLI reference](cli.md) | Every command and flag |
| [Python API](api.md) | `run_task` and `run_pipeline` |
| [Execution backends](backends.md) | Spark, Databricks, pandas |
| [Errors](errors.md) | Every error, what it means and what to do |
| [Proving ground](proving-ground/index.md) | One workload compared across environments |
| [Developer guide](dev_guide.md) | Architecture, plugins, testing |
