# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Classifier: training (Databricks serverless)
# MAGIC
# MAGIC Run by the `titanic_train` job in `databricks.yml`. The job gives this
# MAGIC notebook everything it needs as parameters: the task folder, the run date,
# MAGIC the catalog and schema, the promotion gate, where to save run records, and
# MAGIC the git commit.
# MAGIC
# MAGIC **No logic here.** The training, registration and promotion gate are in
# MAGIC `pipelines/titanic/ml/train_classifier/transformations.py` and `model.py`.
# MAGIC
# MAGIC The job installs the engine with its `ml` extra (`environments` in
# MAGIC `databricks.yml`). To run this notebook by hand, give it an environment with
# MAGIC `ubunye-engine[ml]==0.9.2`, or run `%pip install "ubunye-engine[ml]==0.9.2"`
# MAGIC once in a cell above this one.

# COMMAND ----------

# For a run by hand. A job run fills these from its parameters.
dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.text("titanic_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("titanic_schema", "titanic_ml", "Unity Catalog schema")
dbutils.widgets.text("min_auc", "0.80", "Promotion gate: minimum AUC")

# COMMAND ----------

# Setup, not part of the task: the schema, two UC volumes (the input CSV and the
# model registry) and the CSV itself (fetched once, later runs reuse it). UC
# volumes are the only local path serverless allows.
import os
import urllib.request

catalog = dbutils.widgets.get("titanic_catalog")
schema = dbutils.widgets.get("titanic_schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.model_store")

csv_path = f"/Volumes/{catalog}/{schema}/data/titanic.csv"
if not os.path.exists(csv_path):
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
        csv_path,
    )

# transformations.py reads these three straight from os.environ (they are not
# in config.yaml), so they are set here.
os.environ["TITANIC_MODEL_STORE"] = f"/Volumes/{catalog}/{schema}/model_store"
os.environ["TITANIC_MIN_AUC"] = dbutils.widgets.get("min_auc")
os.environ["MLFLOW_EXPERIMENT_NAME"] = "/Shared/titanic_ml"

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

    # TRANSFORM: run transformations.py. It trains the model, registers it and
    # runs the promotion gate; a failed gate raises PromotionBlockedError here and
    # the run is recorded as failed. One DataFrame per output (the audit row).
    outputs = run.transform(sources)

    # WRITE: check the data-quality rules first. If one fails, nothing is written
    # and the run is recorded as failed. Otherwise each output is written to its
    # table.
    run.write(outputs)
