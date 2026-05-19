# Databricks notebook source
# MAGIC %md
# MAGIC # ABSA Flood Risk - two-task pipeline (Databricks, Unity Catalog)
# MAGIC
# MAGIC Runs two chained Ubunye tasks:
# MAGIC 1. **geocode_addresses** - calls TomTom for each row in the address
# MAGIC    source table and writes the top-1 candidate per id to
# MAGIC    `address_geocoded`.
# MAGIC 2. **flood_risk** - reads `address_geocoded`, calls JBA
# MAGIC    floodscores and flooddepths, merges the two, and writes the
# MAGIC    per-address flood metrics to `address_flood_risk`.
# MAGIC
# MAGIC Credentials come from the Databricks secret scope named by the
# MAGIC `secret_scope` widget (default `absa-flood`). Unity Catalog
# MAGIC identifiers come from DAB variables supplied by the GitHub Actions
# MAGIC workflow; nothing confidential is committed to source.

# COMMAND ----------

dbutils.widgets.text("usecase_dir", "", "Workspace pipelines directory (absolute)")
dbutils.widgets.text("dt", "2026-04-21", "Data timestamp")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("telm_catalog", "", "Unity Catalog catalog (telematics)")
dbutils.widgets.text("telm_schema", "", "Unity Catalog schema (telematics)")
dbutils.widgets.text("address_source_table", "", "Unqualified source table name (id, address)")
dbutils.widgets.text("secret_scope", "absa-flood", "Databricks secret scope for API credentials")
dbutils.widgets.dropdown(
    "tasks_to_run",
    "both",
    ["both", "geocode_only", "flood_only"],
    "Which tasks to run",
)

usecase_dir = dbutils.widgets.get("usecase_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
telm_catalog = dbutils.widgets.get("telm_catalog")
telm_schema = dbutils.widgets.get("telm_schema")
address_source_table = dbutils.widgets.get("address_source_table")
secret_scope = dbutils.widgets.get("secret_scope")
tasks_to_run = dbutils.widgets.get("tasks_to_run")

assert usecase_dir, "usecase_dir must be supplied by the job"
assert telm_catalog and telm_schema, "telm_catalog / telm_schema must be supplied by the job"
assert address_source_table, "address_source_table must be supplied by the job"

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[spark]==0.1.7"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

# Re-read widgets after the Python restart.
usecase_dir = dbutils.widgets.get("usecase_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
telm_catalog = dbutils.widgets.get("telm_catalog")
telm_schema = dbutils.widgets.get("telm_schema")
address_source_table = dbutils.widgets.get("address_source_table")
secret_scope = dbutils.widgets.get("secret_scope")
tasks_to_run = dbutils.widgets.get("tasks_to_run")

# Unity Catalog identifiers flow through the Jinja env context.
os.environ["TELM_CATALOG"] = telm_catalog
os.environ["TELM_SCHEMA"] = telm_schema
os.environ["ADDRESS_SOURCE_TABLE"] = address_source_table

# API credentials from the Databricks secret scope - never persisted.
os.environ["TOMTOM_API_KEY"] = dbutils.secrets.get(scope=secret_scope, key="tomtom_api_key")
os.environ["JBA_BASIC_AUTH"] = dbutils.secrets.get(scope=secret_scope, key="jba_basic_auth")

# COMMAND ----------

import ubunye

if tasks_to_run == "both":
    tasks = ["geocode_addresses", "flood_risk"]
elif tasks_to_run == "geocode_only":
    tasks = ["geocode_addresses"]
else:
    tasks = ["flood_risk"]

results = ubunye.run_pipeline(
    usecase_dir=usecase_dir,
    usecase="flood",
    package="etl",
    tasks=tasks,
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
        df.printSchema()
