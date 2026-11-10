# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Survival: Databricks (serverless)
# MAGIC
# MAGIC Run by the `titanic_survival` job in `databricks.yml`. The job gives this
# MAGIC notebook everything it needs as parameters: the task folder, the run date,
# MAGIC the catalog and schema, where to save run records, and the git commit.
# MAGIC
# MAGIC **No logic here.** The input and output are in `config.yaml`; the logic is in
# MAGIC `transformations.py` next to it, byte-identical to the local example.
# MAGIC
# MAGIC The job installs the engine (`environments` in `databricks.yml`). To run this
# MAGIC notebook by hand, give it an environment with `ubunye-engine==0.9.3`, or run
# MAGIC `%pip install "ubunye-engine==0.9.3"` once in a cell above this one.

# COMMAND ----------

# For a run by hand. A job run fills these from its parameters.
dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.text("titanic_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("titanic_schema", "titanic", "Unity Catalog schema")

# COMMAND ----------

# Setup, not part of the task: the schema, a volume for the input CSV, and the
# CSV itself (fetched once, later runs reuse it). UC volumes are the only local
# file path serverless allows (file:///tmp/... is blocked).
import os
import urllib.request

catalog = dbutils.widgets.get("titanic_catalog")
schema = dbutils.widgets.get("titanic_schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.data")

csv_path = f"/Volumes/{catalog}/{schema}/data/titanic.csv"
if not os.path.exists(csv_path):
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
        csv_path,
    )

# COMMAND ----------

import ubunye

# COMMAND ----------

# config.yaml's {{ env.TITANIC_CATALOG }} and {{ env.TITANIC_SCHEMA }} come from
# the parameters of the same name in lower case. The CSV path has no parameter,
# so it is passed with env=.
with ubunye.notebook(
    dbutils.widgets.get("task_dir"),
    dt=dbutils.widgets.get("dt"),
    env={"TITANIC_INPUT_PATH": csv_path},
) as run:
    # READ: load every input under CONFIG.inputs in config.yaml (the Titanic CSV),
    # one DataFrame per input. Nothing is changed yet.
    sources = run.read()

    # TRANSFORM: run transformations.py. One DataFrame per output.
    # All the business logic is in that file.
    outputs = run.transform(sources)

    # WRITE: check the data-quality rules first. If one fails, nothing is written
    # and the run is recorded as failed. Otherwise each output is written to its
    # table.
    run.write(outputs)
