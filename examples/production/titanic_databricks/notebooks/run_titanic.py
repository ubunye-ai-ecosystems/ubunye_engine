# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Survival — Databricks (serverless)
# MAGIC
# MAGIC Invoked by the `titanic_survival` job defined in `databricks.yml`. All
# MAGIC parameters are supplied by the job at runtime via widgets.
# MAGIC
# MAGIC The notebook wraps the portable `ubunye.run_task()` entry point — the
# MAGIC exact same transformation that runs in the local example. No framework
# MAGIC changes: the existing `DatabricksBackend` auto-detects the active
# MAGIC SparkSession.

# COMMAND ----------

dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("titanic_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("titanic_schema", "titanic", "Unity Catalog schema")

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")

assert task_dir, "task_dir must be supplied by the job (see databricks.yml)"

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[spark] @ git+https://github.com/ubunye-ai-ecosystems/ubunye_engine.git@main"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Re-read widgets after the Python restart and expose env vars consumed by
# config.yaml via Jinja.
import os
import urllib.request

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")

# Bootstrap the Titanic CSV into /tmp at runtime - serverless can read
# file:///tmp/... and the file is gone when the executor releases. This
# replaces the old DBFS bootstrap, which is deprecated on Free Edition.
csv_path = "/tmp/titanic.csv"
if not os.path.exists(csv_path):
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
        csv_path,
    )
    print(f"Fetched Titanic CSV -> {csv_path}")

os.environ["TITANIC_INPUT_PATH"] = f"file://{csv_path}"
os.environ["TITANIC_CATALOG"] = titanic_catalog
os.environ["TITANIC_SCHEMA"] = titanic_schema

# COMMAND ----------

# Ensure the target schema exists. Unity Catalog raises on a missing schema
# when the writer issues CREATE TABLE.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {titanic_catalog}.{titanic_schema}")

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

# Surface the result in the notebook for job-run inspection.
for name, df in outputs.items():
    print(f"--- {name} ---")
    df.show(truncate=False)
    print(f"Row count: {df.count()}")
