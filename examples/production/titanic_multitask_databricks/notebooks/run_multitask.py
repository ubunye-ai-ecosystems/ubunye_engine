# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Multi-Task Pipeline: Databricks (serverless)
# MAGIC
# MAGIC Two-task pipeline: **clean_data** writes a cleaned Delta table, then
# MAGIC **aggregate** reads it and produces a survival-summary table.
# MAGIC
# MAGIC Invoked by the `titanic_multitask` job defined in `databricks.yml`.
# MAGIC Uses the same `transformations.py` as the local example (build once,
# MAGIC run everywhere). Each task gets its own notebook context, and each step
# MAGIC (`ctx.read()`, `ctx.transform()`, `ctx.write()`) is its own cell.

# COMMAND ----------

# MAGIC %pip install "ubunye-engine==0.8.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Widgets come after the restart, so they are read once. A job run fills them
# from base_parameters; the defaults are for running the notebook by hand.
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

# The schema, and a volume for the input CSV. UC volumes are the only local
# file path serverless allows (file:///tmp/... is blocked).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {titanic_catalog}.{titanic_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.data")

# COMMAND ----------

# Put the Titanic CSV in the volume once. Later runs reuse it.
import os
import urllib.request

csv_path = f"/Volumes/{titanic_catalog}/{titanic_schema}/data/titanic.csv"

if not os.path.exists(csv_path):
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
        csv_path,
    )
    print(f"Fetched Titanic CSV -> {csv_path}")
else:
    print(f"Titanic CSV already present at {csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: clean_data

# COMMAND ----------

import ubunye

# config.yaml uses {{ env.TITANIC_CATALOG }} and {{ env.TITANIC_SCHEMA }}:
# ubunye.notebook() fills them from the widgets of the same name in lower case.
# TITANIC_INPUT_PATH has no widget, so it is passed in with env=.
clean = ubunye.notebook(
    f"{usecase_dir}/titanic/pipeline/clean_data",
    mode=mode,
    dt=dt,
    env={"TITANIC_INPUT_PATH": csv_path},
    lineage=True,
)

sources = clean.read()
outputs = clean.transform(sources)
for name, df in outputs.items():
    print(f"--- {name}: {df.count()} rows ---")
    df.show(5, truncate=False)

# COMMAND ----------

# aggregate reads the table this write creates, so it must finish first.
clean.write(outputs)
clean.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: aggregate

# COMMAND ----------

aggregate = ubunye.notebook(
    f"{usecase_dir}/titanic/pipeline/aggregate",
    mode=mode,
    dt=dt,
    lineage=True,
)

sources = aggregate.read()
outputs = aggregate.transform(sources)
for name, df in outputs.items():
    print(f"--- {name}: {df.count()} rows ---")
    df.show(truncate=False)

# COMMAND ----------

aggregate.write(outputs)
aggregate.close()
print("Pipeline complete: clean_data, aggregate")
