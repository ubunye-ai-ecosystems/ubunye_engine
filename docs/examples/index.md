# Examples

Fully worked, CI-deployed pipelines. Every one is self-contained — its own
`config.yaml`, `transformations.py`, tests and GitHub Actions workflow — and every
one runs in CI, so what you read here is what actually executes.

They exist to answer one question:

> *How much code changes when I move a Ubunye pipeline from my laptop to Databricks?*

The short answer: **the business logic does not change.** The config shifts from
`file://` to `dbfs:/` and gains a deployment wrapper. That is all. The
`transformations.py` in `titanic_local` and `titanic_databricks` is *byte-identical*,
and CI enforces it with `diff -q` — if they ever drift, the build fails.

---

## The examples

| Example | Runtime | Input | Output |
|---|---|---|---|
| [Titanic (Local)](titanic-local.md) | Local SparkSession | CSV on disk | Parquet on disk |
| [Titanic (Databricks)](titanic-databricks.md) | Databricks serverless + UC | CSV on UC volume | Unity Catalog Delta table |
| [Titanic ML (Databricks)](titanic-ml-databricks.md) | Databricks serverless + UC | CSV on UC volume | Predictions + training audit log; MLflow + Model Registry |
| [Titanic Multi-Task (Local)](titanic-multitask-local.md) | Local SparkSession | CSV on disk | Intermediate + summary Parquet |
| [Titanic Multi-Task (Databricks)](titanic-multitask-databricks.md) | Databricks serverless + UC | CSV on UC volume | UC Delta tables (intermediate + summary) |
| [JHB Weather (Databricks)](jhb-weather.md) | Databricks + UC, scheduled | Open-Meteo REST API (no auth) | Unity Catalog Delta table |
| [Python API](python-api.md) | Notebook / library | — | Driving the engine from Python instead of the CLI |

---

## Where to start

Pick the one that matches your runtime.

- **Never used the engine?** → [Titanic (Local)](titanic-local.md). Runs on a laptop,
  no cloud account needed.
- **Moving to Databricks?** → Read [Titanic (Local)](titanic-local.md) and
  [Titanic (Databricks)](titanic-databricks.md) side by side. The diff between them
  *is* the migration guide.
- **Chaining tasks?** → the Multi-Task pair. Two tasks run in sequence via
  `ubunye run -t task1 -t task2` (local) or `ubunye.run_pipeline()` (Databricks) —
  which is what proves the engine's sibling-module isolation works.
- **Ingesting from an API?** → [JHB Weather](jhb-weather.md): REST in, Unity Catalog
  out, on a schedule.
- **Doing ML?** → [Titanic ML](titanic-ml-databricks.md): `UbunyeModel`, MLflow
  logging, a filesystem-backed Model Registry on a UC volume, and promotion gates.

!!! note "Community Edition vs paid workspace"

    Every Databricks example here runs on a Databricks **Community Edition**
    workspace — free, no cloud spend. What CE forces you to do differently, and what
    to undo when you move to a paid workspace, is on the
    [Databricks Community Edition](community-edition.md) page.

---

--8<-- "examples/production/README.md:portability"

[:material-github: Browse all examples on GitHub](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/examples){ .md-button }
