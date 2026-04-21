# Databricks notebook source
# MAGIC %md
# MAGIC # Telematics Policy / Device Mapping ETL - Databricks
# MAGIC
# MAGIC Invoked by the `policy_device_mapping_etl` job in `databricks.yml`.
# MAGIC All parameters (task_dir, dt, mode, catalog, schema) are supplied by
# MAGIC the job as widgets at runtime. No identifiers are hard-coded; the
# MAGIC DAB targets pull real catalog/schema values from GitHub environment
# MAGIC secrets and pass them through `--var` at deploy time.

# COMMAND ----------

dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-21", "Data timestamp")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("telm_catalog", "", "Unity Catalog catalog (telematics)")
dbutils.widgets.text("telm_schema", "", "Unity Catalog schema (telematics)")

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
telm_catalog = dbutils.widgets.get("telm_catalog")
telm_schema = dbutils.widgets.get("telm_schema")

assert task_dir, "task_dir must be supplied by the job (see databricks.yml)"
assert telm_catalog, "telm_catalog must be supplied by the job (DAB variable)"
assert telm_schema, "telm_schema must be supplied by the job (DAB variable)"

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[spark]==0.1.7"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
telm_catalog = dbutils.widgets.get("telm_catalog")
telm_schema = dbutils.widgets.get("telm_schema")

os.environ["TELM_CATALOG"] = telm_catalog
os.environ["TELM_SCHEMA"] = telm_schema

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
    print(f"--- {name} ---")
    print(f"Row count: {df.count()}")
    df.printSchema()
