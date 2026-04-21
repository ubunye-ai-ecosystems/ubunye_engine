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

A **data pipeline** is a program that moves data from one place to another — a database to a file, a REST API to a data warehouse — and usually reshapes the data along the way. Building one from scratch is mostly plumbing: wire up the connection, juggle credentials, learn a framework's quirks, write the same *"read → transform → write"* scaffold for the tenth time this year. It's a lot of glue code standing between you and the three lines that actually matter.

**Ubunye Engine writes that plumbing for you.** You describe the pipeline in a short YAML file and put your transformation in a normal Python class. Ubunye takes care of connections, the compute engine (Apache Spark), and the read/write loop.

Same pipeline runs on your laptop today and on a production cluster tomorrow — no code changes.

---

## Quickstart

Install it:

```bash
pip install ubunye-engine
```

Scaffold a new pipeline folder:

```bash
ubunye init -d ./pipelines -u demo -p starter -t filter_adults
```

You get:

```
pipelines/demo/starter/filter_adults/
  config.yaml              ← describes the pipeline (inputs, outputs, settings)
  transformations.py       ← your code goes here
  notebooks/               ← an interactive dev notebook for exploring
```

`ubunye init` gives you a working starting point you can customise. For a minimal run-it-on-your-laptop example, edit `config.yaml` to read a local CSV and write Parquet:

```yaml
CONFIG:
  inputs:
    people:
      format: s3              # generic file reader; "file://" paths work too
      file_format: csv
      path: "file:///tmp/people.csv"
      options:
        header: "true"
        inferSchema: "true"

  outputs:
    adults:
      format: s3
      file_format: parquet
      path: "file:///tmp/adults/"
      mode: overwrite
```

Then open `transformations.py` and write your logic:

```python
from typing import Any, Dict
from ubunye.core.interfaces import Task


class FilterAdults(Task):
    """Keep only rows where age is 18 or older."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        people = sources["people"]
        return {"adults": people.filter("age >= 18")}
```

Two things to notice:

- `sources["people"]` matches the `inputs.people` name from the YAML.
- The return key `"adults"` matches the `outputs.adults` name.

Run it:

```bash
ubunye run -d ./pipelines -u demo -p starter -t filter_adults
```

That's the whole loop. Ubunye reads `/tmp/people.csv`, hands you a Spark DataFrame, and writes whatever you return to `/tmp/adults/`.

**Running on Databricks?** Call it from a notebook instead:

```python
import ubunye
outputs = ubunye.run_task(task_dir="./pipelines/demo/starter/filter_adults")
```

Ubunye detects Databricks' active Spark session and reuses it — same pipeline, no code change.

Want to see a realistic end-to-end example — Kaggle Titanic CSV → survival-rate Parquet, with tests and CI? See [`examples/production/titanic_local/`](examples/production/titanic_local/).

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

**Config over code.** Your pipeline is a YAML file. Inputs, outputs, Spark settings, scheduling — all declared, not coded.

**Plugins for everything.** The `format` field in your config picks which *connector* to use. A connector is a small Python class that knows how to read from or write to one specific place (a database, a REST API, a cloud bucket). Built-ins include `hive`, `jdbc`, `delta`, `s3`, `unity`, and `rest_api`. Need a new data source? Write one and register it — Ubunye discovers plugins automatically.

**Folders as architecture.** Pipelines are organized as `project / use_case / pipeline / task`. The CLI uses this structure for scaffolding, execution, and discovery:

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

The same pipeline runs on every Spark-compatible environment. You only change the `spark.master` setting — the rest is identical:

| Where you run it                       | What to set                                   |
|---------------------------------------|-----------------------------------------------|
| **Your laptop**                       | `spark.master: "local[*]"`                    |
| **Hadoop / YARN cluster**             | `spark.master: "yarn"`                        |
| **Kubernetes**                        | `spark.master: "k8s://..."`                   |
| **Databricks notebooks or jobs**      | Call `ubunye.run_task()` from Python — Ubunye picks up the active session |
| **AWS EMR**                           | Runs as an EMR Step                           |

Don't recognise some of these? That's fine — you only need one. If you're starting out, `local[*]` runs Spark on your own machine with no setup.

---

## Jinja Templating

Anywhere a string appears in your YAML, you can plug in a variable using `{{ … }}` syntax (this is called **Jinja templating**). That's how you keep secrets out of your config, change paths per environment, and inject the run date from the CLI:

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
ubunye validate -d ./pipelines -u <use_case> -p <pipeline> -t <task>   # check config
ubunye plan     -d ./pipelines -u <use_case> -p <pipeline> -t <task>   # preview plan
ubunye run      -d ./pipelines -u <use_case> -p <pipeline> -t <task>   # execute
ubunye test run -d ./pipelines -u <use_case> -p <pipeline> -t <task>   # test mode
ubunye lineage list -d ./pipelines -u <use_case> -p <pipeline> -t <task>  # run history
ubunye models list -u <use_case> -m <model> -s <store>                 # model versions
```

## Python API

```python
import ubunye

# Run from Databricks or any Python environment
outputs = ubunye.run_task(task_dir="./pipelines/...", mode="DEV", dt="2024-06-01")

# Multiple tasks
results = ubunye.run_pipeline(
    usecase_dir="./pipelines", usecase="fraud", package="etl",
    tasks=["claim_etl", "features"], mode="DEV",
)
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
- [x] Pydantic config validation
- [x] ML model contract
- [x] Model registry with versioning
- [x] Lineage tracking
- [x] Python API for Databricks
- [x] Databricks Asset Bundles deployment
- [x] Dev notebook scaffolding
- [ ] Data drift detection
- [ ] `ubunye deploy` CLI command

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
