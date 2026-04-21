# Ubunye Engine — Production Reference Examples

Two fully worked, CI-deployed pipelines that compute the same thing — survival
rate by passenger class on the canonical Kaggle Titanic training set — on two
different runtimes. They exist to answer one specific question:

> *How much code changes when I move a Ubunye pipeline from my laptop to
> Databricks?*

The short answer: the business logic does not change. The config shifts from
`file://` to `dbfs:/` and gains a deployment wrapper. That is all.

---

## The examples

| Example                                           | Runtime                               | Input                        | Output                       |
|---------------------------------------------------|---------------------------------------|------------------------------|------------------------------|
| [`titanic_local/`](./titanic_local/)              | Local SparkSession                    | CSV on disk                  | Parquet on disk              |
| [`titanic_databricks/`](./titanic_databricks/)    | Databricks serverless + UC            | CSV on UC volume             | Unity Catalog Delta table    |
| [`jhb_weather_databricks/`](./jhb_weather_databricks/) | Databricks + Unity Catalog        | Open-Meteo REST API (no auth)| Unity Catalog Delta table    |
| [`titanic_ml_databricks/`](./titanic_ml_databricks/) | Databricks serverless + UC (ML)     | CSV on UC volume             | Training audit log + predictions (UC Delta); MLflow + Model Registry |
| [`titanic_multitask_local/`](./titanic_multitask_local/) | Local SparkSession             | CSV on disk                  | Intermediate + summary Parquet on disk |
| [`titanic_multitask_databricks/`](./titanic_multitask_databricks/) | Databricks serverless + UC | CSV on UC volume | Unity Catalog Delta tables (intermediate + summary) |
| [`absa_flood_risk_databricks/`](./absa_flood_risk_databricks/) | Databricks serverless + UC (paid) | UC table with `(id, address)` | Unity Catalog Delta tables (`address_geocoded` → `address_flood_risk`) |

The first two answer *"how much code changes when I move from laptop to
Databricks?"* — the business logic is byte-identical across them. The third
shows REST ingestion + Unity Catalog sinks with a scheduled Databricks job.
The fourth is the end-to-end ML lifecycle counterpart: `UbunyeModel` +
MLflow logging + filesystem-backed Model Registry on a UC volume, with
promotion gates. The fifth and sixth are **multi-task chaining** pairs: two
tasks run sequentially via `ubunye run -t task1 -t task2` (local) or
`ubunye.run_pipeline()` (Databricks), validating that the engine's
sibling-module isolation works correctly. Their `transformations.py` files
are byte-identical — only the configs change (s3 → unity).

Each example is self-contained: its own `README.md`, tests, and CI workflow.
Start with the one that matches your runtime.

---

## The portability contract

The file that contains the business logic —
`pipelines/titanic/analytics/survival_by_class/transformations.py` —
is **byte-identical** between the two examples.

This is enforced in CI. The Databricks workflow includes a step that runs:

```bash
diff -q \
  examples/production/titanic_local/pipelines/titanic/analytics/survival_by_class/transformations.py \
  examples/production/titanic_databricks/pipelines/titanic/analytics/survival_by_class/transformations.py
```

If the two files drift, the workflow fails with a unified diff and an
`::error` annotation pointing at the file. The portability claim is therefore
a build-time invariant, not a documentation promise.

Nothing else is shared — `config.yaml`, deployment wrappers, and CI workflows
are free to diverge per runtime.

---

## Side-by-side config comparison

Both configs share `MODEL`, `VERSION`, the `ENGINE.profiles` shape, and the
`CONFIG.inputs` / `CONFIG.outputs` structure. Only the highlighted lines
differ.

| Section / key                             | `titanic_local`                                                | `titanic_databricks`                                      |
|-------------------------------------------|----------------------------------------------------------------|-----------------------------------------------------------|
| `ENGINE.spark_conf`                       | `shuffle.partitions: 4`                                        | `shuffle.partitions: 4`                                   |
| `ENGINE.profiles.DEV.spark_conf`          | `shuffle.partitions: 2`                                        | `shuffle.partitions: 2`                                   |
| `ENGINE.profiles.PROD.spark_conf`         | `shuffle.partitions: 50`                                       | `shuffle.partitions: 8` *(CE is single-node)*             |
| `CONFIG.inputs.titanic.format`            | `s3`                                                           | `s3`                                                      |
| `CONFIG.inputs.titanic.file_format`       | `csv`                                                          | `csv`                                                     |
| `CONFIG.inputs.titanic.path` *(resolved)* | `file:///.../data/titanic.csv`                                 | `dbfs:/FileStore/titanic/titanic.csv`                     |
| `CONFIG.outputs.*.format`                 | `s3`                                                           | `s3`                                                      |
| `CONFIG.outputs.*.file_format`            | `parquet`                                                      | `parquet`                                                 |
| `CONFIG.outputs.*.path` *(resolved)*      | `file:///.../output/mode=DEV/dt=2026-04-15`                    | `dbfs:/FileStore/titanic/output/mode=PROD/dt=2026-04-15`  |
| Default `mode` in output path Jinja        | `DEV`                                                          | `PROD`                                                    |

Everything that differs is isolated to the two `env.TITANIC_*` values and the
CE-appropriate partition count. The config *schema* is the same; only its
concrete bindings move.

### What the runtime wrappers do

| Concern                          | `titanic_local`                                        | `titanic_databricks`                                            |
|----------------------------------|--------------------------------------------------------|-----------------------------------------------------------------|
| SparkSession lifecycle           | Created by `SparkBackend.start()`                      | Reused from the notebook context via `DatabricksBackend`        |
| How the CSV gets staged          | `scripts/fetch_titanic.sh` (curl; local or CI)         | `notebooks/run_titanic.py` does `urllib.urlretrieve` into DBFS  |
| How env vars are set             | `export TITANIC_INPUT_PATH=...` before `ubunye run`    | `os.environ["TITANIC_INPUT_PATH"] = dbutils.widgets.get(...)`   |
| How the pipeline is invoked      | `ubunye run -d ... -t survival_by_class`               | `ubunye.run_task(task_dir=..., dt=..., mode=...)`               |
| Output validation                | `scripts/validate_output.py` diffs vs. golden parquet  | PySpark unit tests read the golden parquet via the SparkSession fixture |

---

## Decision guide — which runtime to use when

Pick the runtime that matches where the *data* lives. Ubunye does not care;
the bottleneck is always I/O locality.

| Scenario                                                           | Use                  | Why                                                                                       |
|--------------------------------------------------------------------|----------------------|-------------------------------------------------------------------------------------------|
| Developing and iterating locally, data fits on a laptop            | `titanic_local`      | Fast feedback loop; no auth, no workspace, no cluster startup cost.                       |
| CI validation of config + logic on pull requests                   | `titanic_local`      | GitHub runners have JDK and can run a local Spark session deterministically in ~1 minute. |
| Unit tests of the aggregation itself                               | Either               | Both use a session-scoped local SparkSession fixture — same code runs in CI.              |
| Data lives in DBFS, Unity Catalog, or a Databricks Delta table     | `titanic_databricks` | Reading from DBFS/UC requires a Databricks runtime; running elsewhere pays a transfer cost. |
| Production batch job on a schedule with Databricks Jobs orchestration | `titanic_databricks` | Asset Bundles give you job-as-code, versioned deploys, and lineage in one workflow.       |
| Ad-hoc analysis in a notebook by a data scientist                  | `titanic_databricks` | `ubunye.run_task()` reuses the active SparkSession so the notebook "just works".          |
| Training run where the input has already been prepared locally     | `titanic_local`      | Avoids a round-trip through DBFS for ephemeral inputs.                                    |

Heuristic: if your input path starts with `dbfs:/` or `<catalog>.<schema>.<table>`,
you want the Databricks example. Otherwise the local one is almost certainly
faster and cheaper to iterate on.

---

## Community Edition vs. a standard Databricks workspace

The Databricks example is targeted at **Community Edition** so it works on a
free tier. Every decision in `databricks.yml` that looks unusual exists
because of a specific CE constraint. The table below tells you what to do
differently if you are on a paid workspace.

| Aspect                                    | On Community Edition                                                                 | On a standard Databricks workspace                                                                  |
|-------------------------------------------|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| **Authentication**                        | Personal access token (PAT) only — no service principals.                            | Prefer service principal + OAuth (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`).             |
| **Cluster sizing**                        | Single-node (`num_workers: 0`, `spark.master: local[*]`).                            | Multi-node autoscaling. Delete the `spark.databricks.cluster.profile: singleNode` conf.             |
| **Storage layer**                         | DBFS (`dbfs:/FileStore/...`). Deprecated for new workspaces.                         | Unity Catalog (`<catalog>.<schema>.<table>`). Swap `format: s3` for `format: unity`.                |
| **Asset Bundle `jobs.create` API**        | Often restricted. `bundle deploy` may 403; fall back to `existing_cluster_id`.       | Works out of the box. `new_cluster` is the right default.                                           |
| **Cluster lifecycle**                     | Auto-terminates; no warm pools.                                                      | Use instance pools for faster cold starts.                                                          |
| **Observability**                         | Notebook `print()` + `ubunye lineage` records copied out of DBFS.                    | MLflow, Ubunye lineage written to UC volumes, OTel hooks shipped to your APM.                        |
| **Secrets**                               | `DATABRICKS_TOKEN` in GitHub Actions secrets.                                        | Same shape; consider using GitHub OIDC + Databricks federated identity instead of a long-lived PAT. |
| **Cost model**                            | Free; 15 GB memory single-node; workspace idle-expires.                              | Pay per DBU + cloud compute. Size clusters to your SLA, not to CE limits.                           |
| **Runtime availability**                  | `13.3.x-scala2.12` is the safe pin; newer runtimes may not be offered.               | Track the latest LTS. Update `spark_version` in `databricks.yml` during each LTS bump.              |
| **Delta / Iceberg**                       | Delta works; Iceberg typically not provisioned.                                      | Both. Unity Catalog governs Iceberg tables as of DBR 14.                                            |

**Net:** the *shape* of `databricks.yml` and the entry notebook is the same
on both. If you copy this example to a paid workspace you will delete the
single-node cluster conf, change the storage format, and swap PAT for OAuth
— roughly a ten-line diff. The pipeline and `transformations.py` do not move.

### Community Edition-specific risks you should know about

These are not fixable from inside the example — they are constraints of the
platform itself and are documented honestly in
[`titanic_databricks/README.md`](./titanic_databricks/README.md):

1. New CE signups have been closed. Existing workspaces still work but new
   ones may need to use the Databricks "Free Edition" successor, which has
   different defaults.
2. `databricks bundle deploy` against CE can fail on the `jobs.create` API
   depending on your tier; the README documents the `existing_cluster_id`
   fallback.
3. DBFS is deprecated for net-new workspaces. The example uses it because CE
   has no Unity Catalog alternative; on a paid workspace you should migrate
   to UC volumes.
4. Workspaces time out. Expect to re-authenticate and possibly recreate the
   job if you do not return for months.

None of these are worked around silently. If any of them bite in CI, the
workflow fails with the underlying tool's own error message.
