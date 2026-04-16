# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Multi-Task Pipeline — Databricks (serverless)
# MAGIC
# MAGIC Two-task pipeline: **clean_data** writes a cleaned Delta table, then
# MAGIC **aggregate** reads it and produces a survival-summary table.
# MAGIC
# MAGIC Invoked by the `titanic_multitask` job defined in `databricks.yml`.
# MAGIC Uses the same `transformations.py` as the local example (build once,
# MAGIC run everywhere).

# COMMAND ----------

dbutils.widgets.text("usecase_dir", "", "Workspace pipelines directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("titanic_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("titanic_schema", "titanic", "Unity Catalog schema")

usecase_dir = dbutils.widgets.get("usecase_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")

assert usecase_dir, "usecase_dir must be supplied by the job (see databricks.yml)"

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[spark]==0.1.6"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import urllib.request

usecase_dir = dbutils.widgets.get("usecase_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {titanic_catalog}.{titanic_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.data")

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

# COMMAND ----------

import ubunye

results = ubunye.run_pipeline(
    usecase_dir=usecase_dir,
    usecase="titanic",
    package="pipeline",
    tasks=["clean_data", "aggregate"],
    dt=dt,
    mode=mode,
    lineage=True,
)

print(f"Pipeline complete. Tasks: {list(results.keys())}")

# COMMAND ----------

for task_name, outputs in results.items():
    print(f"\n=== {task_name} ===")
    for name, df in outputs.items():
        print(f"  {name}: {df.count()} rows")
        df.show(truncate=False)
