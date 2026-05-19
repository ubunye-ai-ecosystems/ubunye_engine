# Quickstart

Build and run your first Ubunye pipeline in under 5 minutes.

---

## 1. Install

```bash
pip install ubunye-engine
```

---

## 2. Scaffold a task

```bash
ubunye init -d pipelines -u demo -p etl -t hello_world
```

This creates:

```
pipelines/demo/etl/hello_world/
    config.yaml              ← I/O and compute config
    transformations.py       ← your Python transform
    notebooks/
        hello_world_dev.ipynb  ← interactive dev notebook
```

---

## 3. Edit the config

Open `pipelines/demo/etl/hello_world/config.yaml`:

```yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    source:
      format: hive
      db_name: default
      tbl_name: sample_data

  transform: {}

  outputs:
    sink:
      format: delta
      path: /tmp/ubunye_demo/output
      mode: overwrite
```

!!! tip "No Spark handy?"
    Swap the connectors for REST API or JDBC to run without a Hive metastore.
    See the [Connectors overview](../connectors/overview.md).

---

## 4. (Optional) Add a transform

Edit `transformations.py`:

```python
from ubunye.core.interfaces import Task

class HelloWorldTask(Task):
    def transform(self, sources: dict) -> dict:
        df = sources["source"]
        return {"sink": df.filter("value IS NOT NULL")}
```

Then reference it in `config.yaml`:

```yaml
  transform:
    type: task          # loads transformations.py automatically
```

---

## 5. Validate the config

```bash
ubunye validate -d pipelines -u demo -p etl -t hello_world
```

Expected output:

```
[OK] Config is valid.
```

---

## 6. Preview the execution plan

```bash
ubunye plan -d pipelines -u demo -p etl -t hello_world
```

Prints a DAG: inputs → transform → outputs. Nothing is executed.

---

## 7. Run

```bash
ubunye run -d pipelines -u demo -p etl -t hello_world --profile dev
```

Optionally capture lineage:

```bash
ubunye run -d pipelines -u demo -p etl -t hello_world --profile dev --lineage
```

View recorded runs:

```bash
ubunye lineage list
```

---

---

## 8. (Optional) Run from Python

On Databricks or in a notebook, use the Python API instead of the CLI:

```python
import ubunye

outputs = ubunye.run_task(
    task_dir="pipelines/demo/etl/hello_world",
    mode="DEV",
)
```

The Python API auto-detects an active SparkSession (Databricks) and reuses it.

---

## Common errors

The engine validates configs strictly and gives structured, actionable error
messages across all subsystems. Every error includes context and a hint.

**Unknown field (typo)**

```
Unknown fields in pipelines/fraud/ingestion/claim_etl/config.yaml:

  (top level):
    Unknown field 'ENGNE'
    Did you mean 'ENGINE'?
```

**Undefined template variable**

```
Template resolution failed for config.yaml:
  Undefined variable 'ds' in config value 's3://bucket/{{ ds }}/'.
  Available variables: ['env']. Use '| default(...)' for optional values.
```

Fix: pass the variable via CLI (`--var ds=2025-01-01`) or add a default
(`{{ ds | default('1970-01-01') }}`).

**Missing transformations.py**

```
TaskNotFoundError: Missing transformations.py at .../hello_world/transformations.py.

  Task dir:      pipelines/demo/etl/hello_world
  Expected file: pipelines/demo/etl/hello_world/transformations.py
  Hint: Run 'ubunye init' to scaffold a new task, or check the directory path.
```

**Unknown reader plugin**

```
ReaderNotFoundError: Reader plugin 'hve' not found.

  Format:    hve
  Input:     source
  Installed: ['hive', 'jdbc', 'rest_api', 's3', 'unity']
  Hint: Check the 'format' field in CONFIG.inputs.source.
```

**Invalid profile**

```
ConfigProfileError: Profile 'staging' not found.

  Profile:   staging
  Available: ['dev', 'prod']
  Hint: Valid profiles: dev, prod
```

See the full [Error Reference](../errors.md) for the complete hierarchy and
catching patterns.

---

## What's next?

| Topic | Link |
|---|---|
| Full YAML schema | [Config Reference](../config/overview.md) |
| All built-in connectors | [Connectors](../connectors/overview.md) |
| Python API reference | [API Reference](../api.md) |
| Deploying to Databricks | [Deployment](../deployment.md) |
| Training and versioning ML models | [Model Contract](../ml/model_contract.md) |
| CLI flags and sub-commands | [CLI Reference](../cli.md) |
| Writing custom plugins | [Plugin Guide](../connectors/plugin_guide.md) |
