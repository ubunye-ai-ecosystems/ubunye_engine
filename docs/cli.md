
# CLI Reference

Commands: init, run, plan, export, config, plugins, doctor, version.


The Ubunye CLI (`ubunye`) is built with [Typer](https://typer.tiangolo.com/).

---

## 🔨 `init`
Scaffold a new usecase/pipeline/task with config + transformations.py.

```bash
ubunye init -d ./pipelines -u fraud_detection -p ingestion -t claim_etl
### Creates:
pipelines/fraud_detection/ingestion/claim_etl/
    config.yaml
    transformations.py
```

## Commands

```bash
# Run a task locally or in Spark cluster
    ubunye run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --profile dev

# Visualize the DAG of inputs → transforms → outputs.
    ubunye plan -c pipelines/fraud_detection/ingestion/claim_etl/config.yaml

# Export orchestration artifacts.
    ubunye export airflow -c path/to/config.yaml -o dags/claim_etl.py
    ubunye export databricks -c path/to/config.yaml -o job.json

# Validate or show expanded configs.
    ubunye config validate -c config.yaml

# List all discovered plugins
    ubunye plugins

#Run environment checks.
    ubunye doctor

#Show Ubunye Engine version.
ubunye version

```
