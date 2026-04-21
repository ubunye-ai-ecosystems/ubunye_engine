# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Classifier — Training (Databricks serverless)
# MAGIC
# MAGIC Invoked by the ``titanic_train`` job defined in ``databricks.yml``. All
# MAGIC parameters are supplied by the job at runtime via widgets.
# MAGIC
# MAGIC This notebook wraps the portable ``ubunye.run_task()`` entry point —
# MAGIC the actual training logic lives in
# MAGIC ``pipelines/titanic/ml/train_classifier/transformations.py``.

# COMMAND ----------

dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("titanic_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("titanic_schema", "titanic_ml", "Unity Catalog schema")
dbutils.widgets.text("min_auc", "0.80", "Promotion gate: minimum AUC")

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")
min_auc = dbutils.widgets.get("min_auc")

assert task_dir, "task_dir must be supplied by the job (see databricks.yml)"

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[spark,ml]==0.1.7"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import urllib.request

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")
min_auc = dbutils.widgets.get("min_auc")

# COMMAND ----------

# Provision the schema and two UC volumes: one for the input CSV and one for
# the model registry filesystem store. UC volumes are the only serverless-
# compatible local-path destination (file:///tmp/... is blocked).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {titanic_catalog}.{titanic_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.model_store")

# COMMAND ----------

volume_dir = f"/Volumes/{titanic_catalog}/{titanic_schema}/data"
csv_path = f"{volume_dir}/titanic.csv"

if not os.path.exists(csv_path):
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
        csv_path,
    )
    print(f"Fetched Titanic CSV -> {csv_path}")
else:
    print(f"Titanic CSV already present at {csv_path}")

os.environ["TITANIC_INPUT_PATH"] = csv_path
os.environ["TITANIC_CATALOG"] = titanic_catalog
os.environ["TITANIC_SCHEMA"] = titanic_schema
os.environ["TITANIC_MODEL_STORE"] = (
    f"/Volumes/{titanic_catalog}/{titanic_schema}/model_store"
)
os.environ["TITANIC_MIN_AUC"] = min_auc
os.environ["MLFLOW_EXPERIMENT_NAME"] = "/Shared/titanic_ml"

# COMMAND ----------

import ubunye

outputs = ubunye.run_task(
    task_dir=task_dir,
    dt=dt,
    mode=mode,
    lineage=True,
)

print(f"Outputs written: {list(outputs.keys())}")

# COMMAND ----------

# Show the one-row training audit log so the job-run summary surfaces the
# promotion outcome and headline metrics without opening the UC table.
for name, df in outputs.items():
    print(f"--- {name} ---")
    df.show(truncate=False)
