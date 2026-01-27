# Developer Guide

## Environment
```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -e .[dev]
```

## Test
```bash
pytest -q
```

## Local docs
You can document with MkDocs or Sphinx (choose one). Placeholder docs are in `/docs`.


## Framework Directory Tree

```bash
ubunye-engine/
├── pyproject.toml
├── README.md              # User-facing docs (how to install & use)
├── DEV_README.md          # For developers (running tests, dev env)
├── CONTRIBUTING.md        # PR rules, style, testing
├── LICENSE
├── .gitignore
├── .github/workflows/ci.yml
│
├── docs/                  # MkDocs or Sphinx
│   ├── index.md
│   ├── quickstart.md
│   ├── cli.md
│   ├── config_reference.md
│   └── plugins.md
│
├── examples/
│   ├── pipelines/fraud_detection/ingestion/claim_etl/
│   │   ├── config.yaml
│   │   └── transformations.py
│   └── README.md
│
├── tests/
│   ├── test_config.py
│   ├── test_runtime.py
│   ├── test_plugins.py
│   └── resources/tiny_parquet.parquet
│
└── ubunye/
    ├── __init__.py
    ├── core/
    │   ├── __init__.py
    │   ├── interfaces.py
    │   ├── runtime.py
    │   ├── graph.py
    │   └── errors.py
    ├── config/
    │   ├── __init__.py
    │   ├── loader.py
    │   ├── schema.py
    │   └── variables.py
    ├── backends/
    │   ├── __init__.py
    │   ├── spark_backend.py
    │   └── pandas_backend.py
    ├── cli/
    │   ├── __init__.py
    │   └── main.py
    ├── orchestration/
    │   ├── __init__.py
    │   ├── base.py
    │   ├── airflow_exporter.py
    │   ├── dagster_exporter.py
    │   └── prefect_exporter.py
    ├── telemetry/
    │   ├── __init__.py
    │   ├── otel.py
    │   ├── prometheus.py
    │   └── events.py
    ├── plugins/
    │   ├── __init__.py
    │   ├── readers/hive.py
    │   ├── writers/s3.py
    │   ├── transforms/noop.py
    │   └── ml/sklearn.py
    └── compat/
        └── analytics_engine_shim.py
```

## Ubunye CLI Commands


```bash
ubunye --help
```

### Commands

- **`init`**  
  Scaffold a new usecase/pipeline/task with config and transformations module.  
  ```bash
  ubunye init -d <dir> -u <usecase> -p <pipeline> -t <task>
  ```
  - Options: `--with-ml sklearn|xgboost|h2o`, `--overwrite`
  - Creates: `<dir>/<usecase>/<pipeline>/<task>/config.yaml`, `<dir>/<usecase>/<pipeline>/<task>/transformations.py`

- **`run`**  
  Execute tasks locally, on-prem, or in the cloud.  
  ```bash
  ubunye run -d ./pipelines -u fraudetection -p ingestion -t claim_etl --profile dev
  ```
  - Options: `-d, --dir`, `-u, --usecase`, `-p, --pipeline`, `-t, --task`, `--all-packages`, `--all-tasks`, `--profile dev|prod|staging`, `--spark-conf key=value`, `--dry-run`, `--backend spark|pandas`

- **`plan`**  
  Visualize the resolved DAG (inputs → transform → outputs).  
  ```bash
  ubunye plan -d ./pipelines -u fraudetection -p ingestion -t claim_etl
  ```
  - Example output:
    ```
    Task: pipelines/fraudetection/ingestion/claim_etl
    Inputs:
      - hive: fraud_db.raw_claims
      - jdbc: insurance.policy_dim
    Transform: transformations.ClaimEtl
    Outputs:
      - s3: s3a://fraud-bronze/claims/{{ ds }}
      - iceberg: cur_fraud.claim_curated
    ```

- **`export`**  
  Generate orchestrator artifacts.  
  ```bash
  ubunye export airflow   -d ./pipelines -u fraudetection -p ingestion -t claim_etl -o dag.py
  ubunye export dagster   -d ./pipelines -u fraudetection -p ingestion --all-tasks -o job.py
  ubunye export prefect   -d ./pipelines -u fraudetection --all-packages --all-tasks -o flow.py
  ubunye export databricks -d ./pipelines -u fraudetection -p ingestion -t claim_etl -o job.json
  ```

- **`config`**  
  Inspect or validate configs.  
  ```bash
  ubunye config show pipelines/fraudetection/ingestion/claim_etl/config.yaml
  ubunye config validate pipelines/fraudetection/ingestion/claim_etl/config.yaml
  ```

- **`plugins`**  
  List available plugins (readers, writers, transforms, ML).  
  ```bash
  ubunye plugins
  ```
  - Example output:
    ```
    Readers: hive, jdbc, api, mongo
    Writers: hive, s3, iceberg, opensearch
    Transforms: noop, sql
    ML: sklearn, xgboost, h2o, mlflow
    ```

- **`doctor`**  
  Run environment and connection checks.  
  ```bash
  ubunye doctor
  ```
  - Checks: Spark availability, Python deps, JDBC connectivity, S3/HDFS/Iceberg access, plugin registration

- **`version`**  
  Show Ubunye version.  
  ```bash
  ubunye version
  ```
  - Example output:
    ```
    Ubunye Engine v0.1.0
    Python 3.11.8
    Spark 3.5.1
    ```

## Features

- **Scaffolding**: Quick start with `init`.
- **Execution**: Flexible `run` with local/dev/prod support.
- **Orchestration**: Export to Airflow, Dagster, Prefect, or Databricks.
- **Config Management**: Validate and inspect with `config`.
- **Diagnostics**: Ensure setup with `doctor`.
- **Extensibility**: Discover plugins with `plugins`.
