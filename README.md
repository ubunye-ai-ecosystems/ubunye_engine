<p align="center">
  <img src="docs/assets/ubunye-logo-white-v2.png" alt="Ubunye Engine" width="400"/>
</p>

<p align="center">
  <em>Ubunye (oo-BOON-yeh) — isiZulu for "unity"</em>
</p>

<h3 align="center">One framework. Every pipeline. Any environment.</h3>

<p align="center">
  <a href="https://ubunye-ai-ecosystems.github.io/ubunye_engine">Docs</a> •
  <a href="#quickstart">Quickstart</a> •
  <a href="#why-ubunye">Why Ubunye</a> •
  <a href="https://github.com/ubunye-ai-ecosystems/ubunye_engine/discussions">Community</a>
</p>

---

## Hey there 👋

If you've ever spent more time wiring up Spark boilerplate than writing actual data logic — this is for you.

**Ubunye Engine** lets you define your entire data pipeline in a simple YAML config and a Python file. No Spark session setup. No connection management. No environment-specific scripts. Just tell the engine *what* you want, and it handles the *how*.

It works on your laptop, on YARN, on Kubernetes, on Databricks — same config, same code, everywhere.

---

## Quickstart

```bash
pip install ubunye-engine
```

Scaffold your first pipeline:

```bash
ubunye init -d ./pipelines -u fraud_detection -p ingestion -t claim_etl
```

You'll get:

```
pipelines/
  fraud_detection/
    ingestion/
      claim_etl/
        config.yaml           ← tell the engine what to do
        transformations.py    ← your logic goes here
```

Open `config.yaml` and describe your pipeline:

```yaml
MODEL: "etl"
VERSION: "0.1.0"

ENGINE:
  profiles:
    dev:
      spark_conf:
        spark.master: "local[*]"
    prod:
      spark_conf:
        spark.master: "yarn"

CONFIG:
  inputs:
    raw_claims:
      format: hive
      db_name: fraud_db
      tbl_name: raw_claims

  transform:
    type: noop

  outputs:
    bronze:
      format: delta
      table: main.fraud.bronze_claims
      mode: overwrite
```

Add your logic in `transformations.py`:

```python
def transform(df):
    return df.filter("claim_amount > 0").dropDuplicates(["claim_id"])
```

Run it:

```bash
ubunye run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --profile dev
```

That's it. You just built and ran a pipeline. Same config runs in production — just swap `--profile prod`.

---

## Why Ubunye

We've all been there. You join a new team, open the repo, and find five Spark projects — each structured differently, each with its own way of handling configs, credentials, and deployment. One uses a JSON file, another has everything hardcoded, a third has a 300-line bash script that "Dave wrote and it just works."

Ubunye says: **let's agree on how pipelines look.** One folder structure. One config format. One CLI. Whether you're building an ETL job, a feature pipeline, or an ML training run.

| Without Ubunye | With Ubunye |
|---|---|
| Every project looks different | One standard: `use_case / pipeline / task` |
| Spark setup scattered everywhere | Engine handles it from YAML config |
| Credentials hardcoded or inconsistent | `{{ env.DB_PASSWORD }}` everywhere |
| "Works on my machine" | Same config runs local, YARN, K8s, Databricks |
| New teammate needs a week to onboard | `ubunye init` and they're running in minutes |

---

## How It Works

Three simple ideas:

**Config over code.** Your pipeline is a YAML file. Inputs, outputs, Spark settings, orchestration — all declared, not coded.

**Plugins for everything.** The `format` field in your config selects a connector — `hive`, `jdbc`, `delta`, `s3`, `unity`, `rest_api`, and more. Need a new data source? Add a plugin.

**Folders as architecture.** Pipelines are organized as `project / use_case / pipeline / task`. The CLI uses this for scaffolding, execution, and discovery:

```
pipelines/
  fraud_detection/
    ingestion/
      claim_etl/
      policy_etl/
    feature_engineering/
      claim_features/
    risk_scoring/
      train_model/
      score_claims/
```

---

## What Can You Build With It

**ETL pipelines** — move data between Hive, JDBC databases, Delta Lake, S3, REST APIs. Config-driven, scheduled, reproducible.

**ML training and inference** — define your model behind a simple contract, let the engine handle versioning, storage, and deployment.

**RAG document pipelines** — ingest documents, extract text, chunk, compute embeddings, load into a vector store. All from YAML.

**Feature engineering** — compute features once, write to a shared table, reuse across use cases.

**Data drift detection** — monitor feature distributions between runs, flag when things shift.

Check out the [Patterns](https://ubunye-ai-ecosystems.github.io/ubunye_engine) section in our docs for full examples.

---

## Connectors

| Format | Read | Write | Description |
|---|:---:|:---:|---|
| `hive` | ✓ | ✓ | Apache Hive tables |
| `jdbc` | ✓ | ✓ | PostgreSQL, MySQL, Teradata, and more |
| `delta` | ✓ | ✓ | Delta Lake (standalone or Unity Catalog) |
| `s3` | ✓ | ✓ | S3, HDFS, or local filesystem |
| `unity` | ✓ | ✓ | Databricks Unity Catalog |
| `binary` | ✓ | | Binary files (images, PDFs) |
| `rest_api` | ✓ | ✓ | REST APIs with pagination and auth |

Want to add one? See the [plugin guide](https://ubunye-ai-ecosystems.github.io/ubunye_engine).

---

## Run Anywhere

Same pipeline, no changes:

| Environment | Just set |
|---|---|
| Local | `spark.master: "local[*]"` |
| YARN / Hadoop | `spark.master: "yarn"` |
| Kubernetes | `spark.master: "k8s://..."` |
| Databricks | Via ORCHESTRATION config |
| AWS EMR | Via EMR Steps |

---

## Jinja Templating

All config values support Jinja2:

```yaml
# Environment variables
password: "{{ env.DB_PASSWORD }}"

# CLI variables (--var ds=2025-01-01)
path: "s3a://bucket/{{ ds }}/"

# Defaults
path: "s3a://bucket/{{ ds | default('2025-01-01') }}/"
```

---

## CLI

```bash
ubunye init     -d ./pipelines -u <use_case> -p <pipeline> -t <task>   # scaffold
ubunye run      -d ./pipelines -u <use_case> -p <pipeline> -t <task>   # execute
ubunye validate -d ./pipelines -u <use_case> -p <pipeline> -t <task>   # check config
```

---

## What Ubunye Is Not

It's not an agent framework — use LangChain or CrewAI for that.
It's not an orchestrator — use Airflow, Prefect, or Dagster.
It's not a compute engine — it runs on Spark.

Ubunye is the **standardization layer** between your data sources and your applications. It makes the plumbing boring so you can focus on what matters.

---

## Roadmap

- [x] Config-driven ETL pipelines
- [x] Multi-environment profiles
- [x] Jinja templating
- [x] Plugin-based connectors
- [x] CLI scaffolding and execution
- [ ] Pydantic config validation
- [ ] ML model contract
- [ ] Model registry with versioning
- [ ] Data drift detection
- [ ] Lineage tracking

---

## Get Involved

We'd love your help. Whether it's a new connector, a bug fix, a typo, or just telling us what you're building — all contributions matter.

- 🐛 [Report a bug](https://github.com/ubunye-ai-ecosystems/ubunye_engine/issues)
- 💡 [Request a feature](https://github.com/ubunye-ai-ecosystems/ubunye_engine/discussions)
- 📖 [Read the contributing guide](CONTRIBUTING.md)
- ⭐ Star the repo if you find it useful — it helps more than you'd think

---

## License

[MIT License](LICENSE)

---

<p align="center">
  Built with 🇿🇦 by <a href="https://github.com/ubunye-ai-ecosystems">Ubunye AI Ecosystems</a>
</p>
