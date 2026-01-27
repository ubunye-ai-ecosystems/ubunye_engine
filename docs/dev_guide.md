
# Developer Guide

This guide covers local setup, architecture deep-dive, and how to extend Ubunye with plugins and ML components.

---

## 1) Local setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -e .[dev,spark,ml]
pre-commit install
````

Run checks:

```bash
pytest -q
ruff check .
black --check .
```

---

## 2) Architecture deep-dive

### Engine runtime

* `Engine.run(cfg)`:

  1. Start backend (Spark).
  2. Read inputs via **Reader** plugins.
  3. Apply one or more **Transform** plugins.
  4. Write outputs via **Writer** plugins.
  5. Stop backend.
* Deterministic ordering of inputs/outputs.
* Transform pipelines supported (list of transforms).

### Registry

* Discovers plugins via entry points:

  * `ubunye.readers`, `ubunye.writers`, `ubunye.transforms`, `ubunye.ml`.

### Backends

* `SparkBackend` lazily imports `pyspark`, supports context manager, multiple starts, and effective conf inspection.

### Configs

* YAML with `ENGINE`, `CONFIG`, optional `ORCHESTRATION`.
* Pydantic models validate and merge profiles.
* Jinja templating supported (env vars, dates).

### Telemetry

* Feature-flagged with `UBUNYE_TELEMETRY=1`.
* `telemetry/events.py`: JSONL logs.
* `telemetry/prometheus.py`: counters/histograms.
* `telemetry/otel.py`: optional OpenTelemetry spans.

### Orchestration

* Exporters generate artifacts:

  * Airflow: DAG `.py`
  * Databricks: Jobs `job.json`
* CLI: `ubunye export <airflow|databricks> -c config.yaml -o <out>`

---

## 3) Creating plugins

### Reader

```python
from ubunye.core.interfaces import Reader
class MyReader(Reader):
    def read(self, cfg, backend):
        spark = backend.spark
        return spark.read.format("...").options(**cfg.get("options", {})).load(...)
```

Register in `pyproject.toml`:

```toml
[project.entry-points."ubunye.readers"]
myreader = "my_pkg.my_reader:MyReader"
```

### Writer

```python
from ubunye.core.interfaces import Writer
class MyWriter(Writer):
    def write(self, df, cfg, backend):
        df.write.format("...").mode(cfg.get("mode","append")).save(...)
```

### Transform

```python
from ubunye.core.interfaces import Transform
class MyTransform(Transform):
    def apply(self, inputs, cfg, backend):
        df = inputs["in"]
        out = df.filter("...").select("...")
        return {"out": out}
```

### Testing plugins

* Use Spark local mode:

  ```python
  from ubunye.backends.spark_backend import SparkBackend
  with SparkBackend(app_name="test") as be:
      spark = be.spark
      # create tiny DataFrames and assert behavior
  ```

---

## 4) ML integration

### Base API

* `BaseModel` (`plugins/ml/base.py`) defines:

  * `fit(X,y)`, `predict(X)`, `save(path)`, `load(path)`, `metrics()`, `params`.
  * `FeatureSchema(features=[...], target="...")`.
* Wrappers:

  * `SklearnModel`, `TorchModel`, `SparkMLModel`.

### Example (sklearn)

```python
from sklearn.linear_model import LogisticRegression
from ubunye.plugins.ml.base import FeatureSchema
from ubunye.plugins.ml.sklearn import SklearnModel

schema = FeatureSchema(features=["f1","f2","f3"], target="label")
model = SklearnModel(LogisticRegression(max_iter=200), schema=schema)
model.fit(pdf)                      # pandas df or spark.toPandas()
preds, probs = model.predict(pdf, proba=True)
model.save("models/logreg")
```

### Batch scoring on Spark

* Use `BatchPredictMixin.predict_on_spark(sdf)` for UDF-based inference.
* For high throughput: port to pandas UDF / Arrow or native Spark ML when possible.

### MLflow (optional)

* Use `MLflowLoggingMixin.mlflow_log_all(...)` to log params/metrics/artifacts when MLflow is installed and configured.

---

## 5) Monitoring hooks (MLflow, drift, etc.)

You can attach monitoring backends via `CONFIG.monitors`. Each monitor is a plugin loaded
from the `ubunye.monitors` entry point group.

```yaml
CONFIG:
  monitors:
    - type: mlflow
      params:
        experiment: "ubunye"
        run_name: "claim_etl"
        metrics_path: "CONFIG.monitoring.metrics"
  monitoring:
    metrics:
      drift_psi: 0.12
      precision_at_10: 0.91
```

The MLflow monitor will log task params/metrics at run completion (when MLflow is installed).

---

## 6) Orchestration exporters

### Airflow

* Command:

  ```bash
  ubunye export airflow -c path/to/config.yaml -o dags/claim_etl.py --profile prod
  ```
* The exporter reads `ORCHESTRATION.airflow` (schedule, retries, owner) and emits a DAG that shells:

  ```
  ubunye run -c path/to/config.yaml --profile prod
  ```

### Databricks

* Command:

  ```bash
  ubunye export databricks -c path/to/config.yaml -o job.json --profile prod
  ```
* Upload your wheel to DBFS, then:

  ```bash
  databricks jobs create --json-file job.json
  databricks jobs run-now --job-id <ID>
  ```

---

## 7) Telemetry in local runs

```bash
UBUNYE_TELEMETRY=1 UBUNYE_PROM_PORT=8000 ubunye run -c config.yaml --profile dev
# scrape http://localhost:8000/metrics (Prometheus)
# spans go to console by default unless OTLP is configured
```

---

## 8) Tips & best practices

* Keep configs small; compose via includes/templates if needed.
* Avoid calling `count()` just for metrics; piggyback on existing actions.
* Treat **plugins** as deployment boundaries: keep dependencies optional.
* Prefer **Delta + Unity Catalog** on Databricks for governance and performance.
* Use **partitioned** JDBC reads for large tables.

````

---

# 🔧 Add to your MkDocs nav

Update `mkdocs.yml`:

```yaml
nav:
  - Home: index.md
  - Getting Started:
      - Installation: installation.md
      - Overview: overview.md
  - Usage:
      - CLI: cli.md
      - Config Reference: config_reference.md
      - Plugins: plugins.md
  - API: api.md
  - Contributing: contributing.md
  - Developer Guide: dev_guide.md
````

