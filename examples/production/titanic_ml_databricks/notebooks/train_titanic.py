# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic Classifier: training (Databricks serverless)
# MAGIC
# MAGIC Invoked by the ``titanic_train`` job defined in ``databricks.yml``. The
# MAGIC job passes its parameters in as widgets. You can also open this notebook
# MAGIC and run it cell by cell.
# MAGIC
# MAGIC The training logic lives in
# MAGIC ``pipelines/titanic/ml/train_classifier/transformations.py``. This
# MAGIC notebook runs it through the notebook API: ``ctx.read()``,
# MAGIC ``ctx.transform()`` (trains, registers and gates the model) and
# MAGIC ``ctx.write()`` (writes the audit row).

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
dbutils.widgets.text("min_auc", "0.80", "Promotion gate: minimum AUC")

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
titanic_catalog = dbutils.widgets.get("titanic_catalog")
titanic_schema = dbutils.widgets.get("titanic_schema")
min_auc = dbutils.widgets.get("min_auc")

assert task_dir, "task_dir must be supplied by the job (see databricks.yml)"

# COMMAND ----------

# The schema and two UC volumes: one for the input CSV, one for the model
# registry. UC volumes are the only local path serverless allows.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {titanic_catalog}.{titanic_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {titanic_catalog}.{titanic_schema}.model_store")

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

# transformations.py reads these three straight from os.environ (they are not
# in config.yaml), so they are set here.
os.environ["TITANIC_MODEL_STORE"] = f"/Volumes/{titanic_catalog}/{titanic_schema}/model_store"
os.environ["TITANIC_MIN_AUC"] = min_auc
os.environ["MLFLOW_EXPERIMENT_NAME"] = "/Shared/titanic_ml"

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
for name, df in sources.items():
    print(f"--- input: {name} ({df.count()} rows) ---")
    df.show(5, truncate=False)

# COMMAND ----------

# Trains the model, registers it and runs the promotion gate. A failed gate
# raises PromotionBlockedError here, and nothing is written.
outputs = ctx.transform(sources)

# The one-row audit log shows the promotion outcome and headline metrics.
for name, df in outputs.items():
    print(f"--- output: {name} ---")
    df.show(truncate=False)

# COMMAND ----------

ctx.write(outputs)
print(f"Outputs written: {list(outputs.keys())}")
ctx.close()
