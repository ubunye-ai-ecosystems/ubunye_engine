# Databricks notebook source
# MAGIC %md
# MAGIC # JHB Hourly Weather - Open-Meteo -> Unity Catalog
# MAGIC
# MAGIC Invoked by the `jhb_weather_forecast` job defined in `databricks.yml`.
# MAGIC Parameters are supplied by the job at runtime via widgets.
# MAGIC
# MAGIC The notebook is a thin wrapper around `ubunye.run_task()` - the same
# MAGIC portable entry point used by the titanic examples. No framework
# MAGIC changes: `DatabricksBackend` auto-detects the active SparkSession.

# COMMAND ----------

dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("weather_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("weather_schema", "weather", "Unity Catalog schema")

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
weather_catalog = dbutils.widgets.get("weather_catalog")
weather_schema = dbutils.widgets.get("weather_schema")

assert task_dir, "task_dir must be supplied by the job (see databricks.yml)"

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[spark]==0.1.7"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Re-read widgets after the Python restart and expose the env vars that
# config.yaml resolves via Jinja.
import os

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
weather_catalog = dbutils.widgets.get("weather_catalog")
weather_schema = dbutils.widgets.get("weather_schema")

os.environ["WEATHER_CATALOG"] = weather_catalog
os.environ["WEATHER_SCHEMA"] = weather_schema

# COMMAND ----------

# Ensure the target schema exists. Unity Catalog raises on a missing schema
# when the writer issues CREATE TABLE, and provisioning the schema from the
# job run keeps the example self-contained.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {weather_catalog}.{weather_schema}")

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
    df.show(10, truncate=False)
    print(f"Row count: {df.count()}")
