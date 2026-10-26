# Databricks notebook source
# MAGIC %md
# MAGIC # Johannesburg Weather: hourly forecast (Databricks serverless)
# MAGIC
# MAGIC Invoked by the `jhb_weather_forecast` job defined in `databricks.yml`.
# MAGIC The job passes its parameters in as widgets. You can also open this
# MAGIC notebook and run it cell by cell.
# MAGIC
# MAGIC It uses the notebook API: `ubunye.notebook()` gives a context, then
# MAGIC `ctx.read()`, `ctx.transform()` and `ctx.write()` run one step each, so
# MAGIC you can look at the data between steps.

# COMMAND ----------

# MAGIC %pip install "ubunye-engine==0.8.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Widgets come after the restart, so they are read once. A job run fills them
# from base_parameters; the defaults are for running the notebook by hand.
dbutils.widgets.text("task_dir", "", "Workspace task directory (absolute)")
dbutils.widgets.text("dt", "2026-04-15", "Data timestamp")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("weather_catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("weather_schema", "weather", "Unity Catalog schema")

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt")
mode = dbutils.widgets.get("mode")
weather_catalog = dbutils.widgets.get("weather_catalog")
weather_schema = dbutils.widgets.get("weather_schema")

assert task_dir, "task_dir must be supplied by the job (see databricks.yml)"

# COMMAND ----------

# Unity Catalog raises on a missing schema when the writer creates the table,
# so the run makes sure it exists.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {weather_catalog}.{weather_schema}")

# COMMAND ----------

import ubunye

# config.yaml uses {{ env.WEATHER_CATALOG }} and {{ env.WEATHER_SCHEMA }}.
# ubunye.notebook() fills them from the widgets of the same name in lower
# case (weather_catalog, weather_schema), so there is no os.environ to set.
ctx = ubunye.notebook(task_dir, mode=mode, dt=dt, lineage=True)
print(f"Values taken from widgets: {ctx.env_vars}")

# COMMAND ----------

sources = ctx.read()
for name, df in sources.items():
    print(f"--- input: {name} ---")
    df.show(5, truncate=False)

# COMMAND ----------

outputs = ctx.transform(sources)
for name, df in outputs.items():
    print(f"--- output: {name} ---")
    df.show(10, truncate=False)

# COMMAND ----------

ctx.write(outputs)
print(f"Outputs written: {list(outputs.keys())}")
ctx.close()
