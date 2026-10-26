# Developer Guide

For people working on the engine itself. Users want [README.md](README.md) and the
[docs](https://ubunye-ai-ecosystems.github.io/ubunye_engine/).

## Environment
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -U pip
pip install -e ".[dev]"
pre-commit install
```

`[dev]` includes pandas and pyarrow, so the unit tier runs with no Java. For the Spark
tier add `pip install -e ".[spark,delta]"` and Java 17 or 21.

## Test
```bash
pytest tests/unit -m "not integration"   # unit tier, no Java, what CI runs on every OS
pytest tests/integration                 # Spark tier, includes the pandas/Spark parity suite
python -m mypy ubunye                    # types
pre-commit run --all-files               # black + ruff
```

[CONTRIBUTING.md](CONTRIBUTING.md) lists every tier and what each needs.

## Local docs
The docs are MkDocs (`mkdocs.yml`, pages in `docs/`):
```bash
pip install -r docs/requirements.txt
mkdocs serve           # preview
mkdocs build --strict  # what CI runs
```


## Framework Directory Tree

The main folders (not every file):

```bash
ubunye_engine/
├── pyproject.toml         # version, extras, entry points (plugins are registered here)
├── README.md              # user-facing docs (how to install and use)
├── DEV_README.md          # this file
├── CONTRIBUTING.md        # PR rules, style, test tiers
├── mkdocs.yml
├── .github/workflows/     # tests.yml (unit + Spark matrix), ci.yml, docs.yml, ...
│
├── docs/                  # MkDocs site
│   ├── getting_started/   # install, quickstart, structure
│   ├── tutorials/
│   ├── config/  connectors/  deployment/  ml/  patterns/
│   ├── architecture/      # ADRs
│   ├── cli.md  api.md  backends.md  errors.md
│   └── proving-ground/    # results of `ubunye prove`
│
├── examples/              # proving/, real-world/, production/, fraud_detection/, rest_api/, python_api/
├── benchmarks/            # guard.py, the performance guard
│
├── tests/
│   ├── unit/              # no Java
│   ├── integration/       # needs Spark
│   ├── fixtures/  data/
│   └── conftest.py
│
└── ubunye/
    ├── api.py             # run_task, run_pipeline
    ├── notebook.py        # notebook helpers
    ├── mcp_server.py      # `ubunye mcp`
    ├── core/              # runtime, task_runner, interfaces, expectations, planning, runs, hooks, errors
    ├── config/            # loader, schema (Pydantic), resolver (Jinja), variables
    ├── backends/          # spark_backend, databricks_backend, pandas_backend
    ├── cli/               # main plus one module per command group
    ├── plugins/           # readers/, writers/, transforms/, ml/, secrets, llm
    ├── models/            # UbunyeModel contract, registry, gates, artifact stores
    ├── lineage/           # run records, hashing, OpenLineage, FOCUS
    ├── proving/           # the proving ground behind `ubunye prove`
    ├── deploy/            # Databricks, Glue, Dataproc, EMR Serverless, Kubernetes, Container Apps
    ├── orchestration/     # Airflow, Databricks job and Spark pipeline exporters
    ├── telemetry/         # events, OpenTelemetry, Prometheus, MLflow, hooks/
    └── llm/               # language model steps: replay, budgets, cost estimates
```

## Ubunye CLI Commands

`ubunye <command> --help` is the reference; the [CLI page](docs/cli.md) has every flag.

```bash
ubunye --help
```

When working on the engine, make sure `ubunye` is your checkout, not an older install:
`pip install -e .` in the checkout, then `ubunye version`. (`python -m ubunye
--task-dir <folder>` is something else: it runs one task, for platforms with no shell.)

### Commands

- **`init`**
  Scaffold a task folder with config, transformations module, a sample CSV and a dev notebook.
  ```bash
  ubunye init -d <dir> -u <usecase> -p <package> -t <task>
  ```
  - Options: `--template local|databricks` (default `local`), `--overwrite`
  - Creates: `<dir>/<usecase>/<package>/<task>/config.yaml`, `transformations.py`,
    `data/people.csv` (local template) and `notebooks/<task>_dev.ipynb`
  - `ubunye init github-actions ...` writes a CI workflow instead.

- **`run`**
  Run one or more tasks.
  ```bash
  ubunye run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl -m DEV
  ```
  - Options: `-d/--usecase-dir`, `-u/--usecase`, `-p/--package`, `-t/--task-list`
    (repeatable), `--all`, `-m/--mode` (default `DEV`), `-dt/--data-timestamp`,
    `--var key=value`, `--backend spark|databricks|pandas`, `--lineage`,
    `--rerun`, `--resume`

- **`plan`**
  Dry run: what each task will read and write, and what will stop it. Moves no data.
  ```bash
  ubunye plan -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --backend pandas
  ```
  - Example output (the `ubunye init` sample task):
    ```
    Task demo/starter/filter_adults   (backend: pandas)
      Inputs
        people       s3 csv         .../filter_adults/data/people.csv
                     found: 1 file(s), 172 bytes
      Transform  FilterAdults  (transformations.py)
      Outputs
        adults       s3 parquet     .../filter_adults/output/adults
                     mode overwrite

    Plan OK: 1 task(s) can run.
    ```

- **`validate`** and **`doctor`**
  `validate` checks configs without running them. `doctor` checks the machine (backends,
  plugins, environment variables) and, with `-d -u -p -t`, the tasks too.

- **`export`**
  Generate orchestrator artifacts from a task's config.
  ```bash
  ubunye export airflow        -c pipelines/fraud_detection/ingestion/claim_etl/config.yaml -o dag.py
  ubunye export databricks     -c pipelines/fraud_detection/ingestion/claim_etl/config.yaml -o job.json
  ubunye export spark-pipeline -c pipelines/fraud_detection/ingestion/claim_etl/config.yaml -o sdp/
  ```

- **`config`**
  Load and validate one task's config.
  ```bash
  ubunye config -d pipelines -u fraud_detection -p ingestion -t claim_etl
  ```

- **`plugins`**
  List the readers, writers and transforms that are installed.
  ```bash
  ubunye plugins
  ```
  - Example output:
    ```
    Readers:   binary, delta, hive, jdbc, rest_api, s3, unity
    Writers:   delta, hive, jdbc, rest_api, s3, unity
    Transforms:model, noop
    ```
  - `ubunye backends` lists the execution backends and what each can do.

- **`version`**
  Show Ubunye version.
  ```bash
  ubunye version
  ```
  - Example output:
    ```
    Ubunye Engine v0.8.0
    ```

Other command groups: `test run`, `lineage`, `models`, `deploy`, `prove`, `gate`,
`mcp`, `sync`. See the [CLI page](docs/cli.md).

## Features

- **Scaffolding**: a runnable task with `init`.
- **Execution**: `run` on Spark, Databricks or pandas (no Java), with safe reruns (`--rerun`, `--resume`).
- **Checks before a run**: `validate`, `plan` and `doctor`.
- **Run records**: `--lineage` keeps one per run; `lineage`, `gate` and `prove` read them.
- **Orchestration**: export to Airflow, a Databricks job or a Spark Declarative Pipeline.
- **Deployment**: `deploy` to Databricks, Glue, Dataproc, EMR Serverless, Kubernetes or Container Apps.
- **Extensibility**: plugins are entry points in `pyproject.toml`; list them with `plugins`.
