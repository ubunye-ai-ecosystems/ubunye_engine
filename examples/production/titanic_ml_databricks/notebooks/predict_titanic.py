# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Classifier — Batch Prediction (Databricks serverless)
# MAGIC
# MAGIC Invoked by the ``titanic_predict`` job defined in ``databricks.yml``.
# MAGIC Loads the current production (or staging) model from the Ubunye Model
# MAGIC Registry via ``ubunye.run_task()`` and writes per-passenger predictions
# MAGIC to a Unity Catalog Delta table.

# COMMAND ----------

dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("titanic_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("titanic_schema", "titanic_ml", "Unity Catalog schema")

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")

assert task_dir, "task_dir must be supplied by the job (see databricks.yml)"

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[spark,ml]==0.1.7"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")

# COMMAND ----------

# The train job owns schema + volume creation; this job assumes they exist.
# CREATE IF NOT EXISTS is still cheap insurance against predict-before-train.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {titanic_catalog}.{titanic_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.model_store")

# COMMAND ----------

csv_path = f"/Volumes/{titanic_catalog}/{titanic_schema}/data/titanic.csv"
assert os.path.exists(csv_path), (
    f"Input CSV missing at {csv_path}. Run the train job at least once to seed it."
)

os.environ["TITANIC_INPUT_PATH"] = csv_path
os.environ["TITANIC_CATALOG"] = titanic_catalog
os.environ["TITANIC_SCHEMA"] = titanic_schema
os.environ["TITANIC_MODEL_STORE"] = (
    f"/Volumes/{titanic_catalog}/{titanic_schema}/model_store"
)

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

for name, df in outputs.items():
    print(f"--- {name} (first 20 rows) ---")
    df.show(20, truncate=False)
    print(f"Total predictions: {df.count()}")
