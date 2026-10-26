# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Classifier: batch prediction (Databricks serverless)
# MAGIC
# MAGIC Invoked by the ``titanic_predict`` job defined in ``databricks.yml``.
# MAGIC Loads the current production (or staging) model from the Ubunye Model
# MAGIC Registry and writes a prediction per passenger to a Unity Catalog
# MAGIC Delta table, one step at a time: ``ctx.read()``, ``ctx.transform()``,
# MAGIC ``ctx.write()``.

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[ml]==0.8.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Widgets come after the restart, so they are read once. A job run fills them
# from base_parameters; the defaults are for running the notebook by hand.
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

# The train job creates the schema and volumes. CREATE IF NOT EXISTS is cheap
# insurance if predict runs first.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {titanic_catalog}.{titanic_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.model_store")

# COMMAND ----------

import os

csv_path = f"/Volumes/{titanic_catalog}/{titanic_schema}/data/titanic.csv"
assert os.path.exists(
    csv_path
), f"Input CSV missing at {csv_path}. Run the train job at least once to seed it."

# transformations.py reads the model store path straight from os.environ.
os.environ["TITANIC_MODEL_STORE"] = f"/Volumes/{titanic_catalog}/{titanic_schema}/model_store"

# COMMAND ----------

import ubunye

# config.yaml uses {{ env.TITANIC_CATALOG }} and {{ env.TITANIC_SCHEMA }}:
# ubunye.notebook() fills them from the widgets of the same name in lower case.
# TITANIC_INPUT_PATH has no widget, so it is passed in with env=.
ctx = ubunye.notebook(
    task_dir,
    mode=mode,
    dt=dt,
    env={"TITANIC_INPUT_PATH": csv_path},
    lineage=True,
)
print(f"Values resolved: {ctx.env_vars}")

# COMMAND ----------

sources = ctx.read()

# Loads the model from the registry and scores every passenger.
outputs = ctx.transform(sources)
for name, df in outputs.items():
    print(f"--- {name} (first 20 rows) ---")
    df.show(20, truncate=False)
    print(f"Total predictions: {df.count()}")

# COMMAND ----------

ctx.write(outputs)
print(f"Outputs written: {list(outputs.keys())}")
ctx.close()
