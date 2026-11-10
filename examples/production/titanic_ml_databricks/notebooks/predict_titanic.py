# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Classifier: batch prediction (Databricks serverless)
# MAGIC
# MAGIC Run by the `titanic_predict` job in `databricks.yml`. The job gives this
# MAGIC notebook everything it needs as parameters: the task folder, the run date,
# MAGIC the catalog and schema, where to save run records, and the git commit.
# MAGIC Run the train job at least once first: it fetches the CSV and registers a
# MAGIC model.
# MAGIC
# MAGIC **No logic here.** Loading the model and scoring are in
# MAGIC `pipelines/titanic/ml/predict_classifier/transformations.py`.
# MAGIC
# MAGIC The job installs the engine with its `ml` extra (`environments` in
# MAGIC `databricks.yml`). To run this notebook by hand, give it an environment with
# MAGIC `ubunye-engine[ml]==0.9.3`, or run `%pip install "ubunye-engine[ml]==0.9.3"`
# MAGIC once in a cell above this one.

# COMMAND ----------

# For a run by hand. A job run fills these from its parameters.
dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.text("titanic_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("titanic_schema", "titanic_ml", "Unity Catalog schema")

# COMMAND ----------

# Setup, not part of the task. The train job creates the schema and volumes;
# CREATE IF NOT EXISTS is cheap insurance if predict runs first.
import os

catalog = dbutils.widgets.get("titanic_catalog")
schema = dbutils.widgets.get("titanic_schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.model_store")

csv_path = f"/Volumes/{catalog}/{schema}/data/titanic.csv"
assert os.path.exists(
    csv_path
), f"Input CSV missing at {csv_path}. Run the train job at least once to seed it."

# transformations.py reads the model store path straight from os.environ.
os.environ["TITANIC_MODEL_STORE"] = f"/Volumes/{catalog}/{schema}/model_store"

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

    # TRANSFORM: run transformations.py. It loads the model from the registry and
    # scores every passenger. One DataFrame per output (the predictions).
    outputs = run.transform(sources)

    # WRITE: check the data-quality rules first. If one fails, nothing is written
    # and the run is recorded as failed. Otherwise each output is written to its
    # table.
    run.write(outputs)
