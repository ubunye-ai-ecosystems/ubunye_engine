# Databricks notebook source
# MAGIC %md
# MAGIC # Johannesburg Weather: hourly forecast (Databricks serverless)
# MAGIC
# MAGIC Run by the `jhb_weather_forecast` job in `databricks.yml`. The job gives this
# MAGIC notebook everything it needs as parameters: the task folder, the run date,
# MAGIC the catalog and schema, where to save run records, and the git commit.
# MAGIC
# MAGIC **No logic here.** The REST call and the output table are in `config.yaml`;
# MAGIC the logic is in `transformations.py` next to it.
# MAGIC
# MAGIC The job installs the engine (`environments` in `databricks.yml`). To run this
# MAGIC notebook by hand, give it an environment with `ubunye-engine==0.9.3`, or run
# MAGIC `%pip install "ubunye-engine==0.9.3"` once in a cell above this one.

# COMMAND ----------

# For a run by hand. A job run fills these from its parameters.
dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.text("weather_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("weather_schema", "weather", "Unity Catalog schema")

# COMMAND ----------

# Setup, not part of the task: Unity Catalog raises on a missing schema when the
# writer creates the table, so make sure it exists.
catalog = dbutils.widgets.get("weather_catalog")
schema = dbutils.widgets.get("weather_schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

import ubunye

# COMMAND ----------

# config.yaml's {{ env.WEATHER_CATALOG }} and {{ env.WEATHER_SCHEMA }} come from
# the parameters of the same name in lower case.
with ubunye.notebook(dbutils.widgets.get("task_dir"), dt=dbutils.widgets.get("dt")) as run:
    # READ: load every input under CONFIG.inputs in config.yaml (one call to the
    # Open-Meteo API), one DataFrame per input. Nothing is changed yet.
    sources = run.read()

    # TRANSFORM: run transformations.py. One DataFrame per output.
    # All the business logic is in that file.
    outputs = run.transform(sources)

    # WRITE: check the data-quality rules first. If one fails, nothing is written
    # and the run is recorded as failed. Otherwise each output is written to its
    # table.
    run.write(outputs)
