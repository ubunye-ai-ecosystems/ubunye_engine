# Ubunye Engine: production reference examples

Worked pipelines, each with its own README, tests and CI workflow. Most of
them compute survival rate by passenger class on the Kaggle Titanic training
set, on a laptop and on Databricks. They answer one question:

> *How much code changes when I move a Ubunye pipeline from my laptop to
> Databricks?*

The short answer: the business logic does not change. The config swaps a
local Parquet output for a Unity Catalog table, and a notebook plus an Asset
Bundle wrap the run. That is all.

---

## The examples

| Example                                           | Runtime                               | Input                        | Output                       |
|---------------------------------------------------|---------------------------------------|------------------------------|------------------------------|
| [`titanic_local/`](./titanic_local/)              | Local Spark, or pandas with no Java   | CSV on disk                  | Parquet on disk              |
| [`titanic_databricks/`](./titanic_databricks/)    | Databricks serverless + Unity Catalog | CSV on a UC volume           | Unity Catalog Delta table    |
| [`jhb_weather_databricks/`](./jhb_weather_databricks/) | Databricks serverless + Unity Catalog | Open-Meteo REST API (no auth) | Unity Catalog Delta table |
| [`titanic_ml_databricks/`](./titanic_ml_databricks/) | Databricks serverless + Unity Catalog (ML) | CSV on a UC volume   | Training audit log + predictions (UC Delta); MLflow + model registry |
| [`titanic_multitask_local/`](./titanic_multitask_local/) | Local Spark, or pandas with no Java | CSV on disk              | Intermediate + summary Parquet on disk |
| [`titanic_multitask_databricks/`](./titanic_multitask_databricks/) | Databricks serverless + Unity Catalog | CSV on a UC volume | Unity Catalog Delta tables (intermediate + summary) |
| [`llm_replay/`](./llm_replay/)                    | pandas, no Java                       | CSV on disk                  | Parquet on disk, with replayed model calls |

The first two answer "how much code changes when I move from laptop to
Databricks?": their `transformations.py` is byte-identical. The third shows
REST ingestion into a Unity Catalog table on a scheduled Databricks job. The
fourth is the ML lifecycle: `UbunyeModel`, MLflow logging and a
filesystem-backed model registry on a UC volume, with a promotion gate. The
two multitask examples chain two tasks: `ubunye run -t clean_data -t aggregate`
locally, `ubunye.run_pipeline()` on Databricks. Their `transformations.py`
files are byte-identical too; only the configs change (`s3` paths to `unity`
tables). The last one calls a language model through `ubunye.llm` and replays
the committed answers, so it runs with no key, no network and no cost.

Each example is self-contained. Start with the one that matches your runtime.

---

## The portability contract

The file that holds the business logic,
`pipelines/titanic/analytics/survival_by_class/transformations.py`,
is byte-identical in `titanic_local` and `titanic_databricks`.

CI enforces this. `.github/workflows/databricks_deploy.yml` has a step that
runs:

```bash
diff -q \
  examples/production/titanic_local/pipelines/titanic/analytics/survival_by_class/transformations.py \
  examples/production/titanic_databricks/pipelines/titanic/analytics/survival_by_class/transformations.py
```

If the two files drift, the workflow fails with a unified diff and an
`::error` annotation that points at the file. So the portability claim is
checked on every build. `.github/workflows/multitask_databricks.yml` does the
same for the two multitask examples.

Nothing else is shared. `config.yaml`, the run wrappers and the CI workflows
are free to differ per runtime.

---

## Side-by-side config comparison

Both configs share `MODEL`, `VERSION`, the `ENGINE` profiles and the input.
They differ in the PROD partition count and in the output.

| Section / key                             | `titanic_local`                                     | `titanic_databricks`                                      |
|-------------------------------------------|-----------------------------------------------------|-----------------------------------------------------------|
| `ENGINE.spark_conf`                       | `shuffle.partitions: 4`, dynamic partition overwrite | same                                                     |
| `ENGINE.profiles.DEV.spark_conf`          | `shuffle.partitions: 2`                             | `shuffle.partitions: 2`                                   |
| `ENGINE.profiles.PROD.spark_conf`         | `shuffle.partitions: 50`                            | `shuffle.partitions: 8`                                   |
| `CONFIG.inputs.titanic`                   | `format: s3`, `file_format: csv`, `{{ env.TITANIC_INPUT_PATH }}` | same                                         |
| Input path *(resolved)*                   | `file:///.../data/titanic.csv`                      | `/Volumes/workspace/titanic/data/titanic.csv`             |
| `CONFIG.outputs.survival_by_class.format` | `s3` with `file_format: parquet`                    | `unity`                                                   |
| Output location                           | `{{ env.TITANIC_OUTPUT_PATH }}/mode={{ mode }}/dt={{ dt }}` | `table: {{ env.TITANIC_CATALOG }}.{{ env.TITANIC_SCHEMA }}.survival_by_class` (defaults `workspace.titanic`) |
| Output *(resolved)*                       | `file:///.../output/mode=DEV/dt=2026-04-15`         | `workspace.titanic.survival_by_class`                     |
| `mode` used by the run                    | `DEV` (the CLI default)                             | `PROD` (set by the job)                                   |

### What the runtime wrappers do

| Concern                          | `titanic_local`                                        | `titanic_databricks`                                            |
|----------------------------------|--------------------------------------------------------|-----------------------------------------------------------------|
| Engine session                   | Created by `SparkBackend`, or none with `--backend pandas` | The notebook's session, reused by `DatabricksBackend`       |
| How the CSV gets staged          | `scripts/fetch_titanic.sh` (curl; local or CI)         | `notebooks/run_titanic.py` downloads it into a UC volume with `urllib.request.urlretrieve` |
| How env vars are set             | `export TITANIC_INPUT_PATH=...` before `ubunye run`    | `os.environ[...]` in the notebook, from job widgets             |
| How the pipeline is invoked      | `ubunye run -d ... -t survival_by_class`               | `ubunye.run_task(task_dir=..., dt=..., mode=..., lineage=True)` |
| Output validation                | `scripts/validate_output.py` diffs against the golden parquet; `scripts/same_receipt.sh` compares the Spark and pandas run records | PySpark unit tests check the golden parquet; query the table after a run |

---

## Decision guide: which runtime to use when

Pick the runtime that matches where the *data* lives. Ubunye does not care;
the bottleneck is I/O locality.

| Scenario                                                           | Use                  | Why                                                                                       |
|--------------------------------------------------------------------|----------------------|-------------------------------------------------------------------------------------------|
| Developing and iterating locally, data fits on a laptop            | `titanic_local`      | Fast feedback; no auth, no workspace, no cluster start. `--backend pandas` needs no Java. |
| CI checks of config and logic on pull requests                     | `titanic_local`      | GitHub runners can run a local Spark session, or pandas, in about a minute.               |
| Unit tests of the aggregation itself                               | Either               | Both use a session-scoped local SparkSession fixture, so the same code runs in CI.        |
| Data lives in Unity Catalog or a Databricks Delta table            | `titanic_databricks` | Reading UC tables and volumes needs a Databricks runtime; elsewhere you pay to move data. |
| Production batch job on a schedule with Databricks Jobs            | `titanic_databricks` | Asset Bundles give you the job as code and versioned deploys.                             |
| Ad-hoc analysis in a notebook                                      | `titanic_databricks` | `ubunye.run_task()` reuses the active SparkSession, so the notebook just works.           |

Rule of thumb: if your input is a `/Volumes/...` path or a
`<catalog>.<schema>.<table>` name, use the Databricks example. Otherwise the
local one is faster and cheaper to iterate on.

---

## Free Edition and paid workspaces

The Databricks examples were run end to end on Databricks Free Edition. Free
Edition is serverless only and gives you a `workspace` catalog. The same
bundles work on a paid workspace with small changes.

| Aspect               | Free Edition                                             | Paid workspace                                                        |
|----------------------|----------------------------------------------------------|-----------------------------------------------------------------------|
| **Authentication**   | Personal access token (`DATABRICKS_TOKEN`).              | Prefer a service principal. The CI workflows also accept `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`. |
| **Compute**          | Serverless only. The bundles have no cluster block, so tasks run on serverless. | Same bundles work. Add a cluster block only if you need classic compute. |
| **Catalog**          | `workspace` (the bundle default).                        | Usually `main`: `databricks bundle deploy --target nonprod --var="titanic_catalog=main"`. |
| **Input files**      | UC volumes (`/Volumes/...`). Serverless blocks `file:///tmp/...`. | Same.                                                        |
| **Engine install**   | Each notebook runs `%pip install ubunye-engine==0.7.0`.  | Same. Pin a newer version when you upgrade.                           |

The `databricks.yml` in each example is a reference bundle. `ubunye deploy
databricks` can build an equivalent job for one task instead; add `--dry-run`
to print the job spec without deploying.

If something fails in CI, the workflow fails with the underlying tool's own
error message. Nothing is worked around silently.
