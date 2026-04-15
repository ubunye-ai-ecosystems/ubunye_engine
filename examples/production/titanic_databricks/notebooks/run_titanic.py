# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Survival — Databricks Community Edition
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
dbutils.widgets.text(
    "titanic_input_path",
    "dbfs:/FileStore/titanic/titanic.csv",
    "DBFS path to the Titanic CSV",
)
dbutils.widgets.text(
    "titanic_output_path",
    "dbfs:/FileStore/titanic/output",
    "DBFS output directory",
)

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
input_path = dbutils.widgets.get("titanic_input_path")
output_path = dbutils.widgets.get("titanic_output_path")

assert task_dir, "task_dir must be supplied by the job (see databricks.yml)"

# COMMAND ----------

# MAGIC %pip install ubunye-engine==0.1.5

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Re-read widgets after the Python restart and expose the env vars that
# config.yaml resolves via Jinja.
import os

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
input_path = dbutils.widgets.get("titanic_input_path")
output_path = dbutils.widgets.get("titanic_output_path")

os.environ["TITANIC_INPUT_PATH"] = input_path
os.environ["TITANIC_OUTPUT_PATH"] = output_path

# COMMAND ----------

# Bootstrap the Titanic CSV into DBFS if it is not already present.
# Idempotent: skips the download on every subsequent run.
import urllib.request

local_mirror = input_path.replace("dbfs:/", "/dbfs/")
if not os.path.exists(local_mirror):
    os.makedirs(os.path.dirname(local_mirror), exist_ok=True)
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
        local_mirror,
    )
    print(f"Fetched Titanic CSV -> {local_mirror}")
else:
    print(f"Titanic CSV already present at {local_mirror}")

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
