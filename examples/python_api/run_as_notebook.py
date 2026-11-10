"""Run one Ubunye task the way a Databricks job notebook does.

This script demonstrates ``ubunye.notebook()`` as a ``with`` block: three
steps, one run. It is the shape of every job notebook on Databricks; here it
runs on your laptop with local Spark.

Usage
-----
    pip install ubunye-engine[spark]
    cd examples/python_api
    python run_as_notebook.py
"""

from pathlib import Path

from pyspark.sql import SparkSession

import ubunye

HERE = Path(__file__).resolve().parent

spark = SparkSession.builder.appName("ubunye-python-api").getOrCreate()

# A few sample events (one duplicate, one null) if there is no input yet.
# The config reads the path given as ``dt``, relative to where you run this.
if not Path("data/raw_events.parquet").exists():
    spark.createDataFrame(
        [(1, "a", 10.0), (2, "b", 5.5), (2, "b", 5.5), (3, "c", 7.25), (4, None, 1.0)],
        "event_id INT, user STRING, amount DOUBLE",
    ).write.parquet("data/raw_events.parquet")

# On Databricks the task folder and dt come from the job's parameters
# (dbutils.widgets.get("task_dir"), dbutils.widgets.get("dt")), and the job
# parameter ubunye_run_records names the run record table. Here they are given
# in the code, and the run record goes to a local folder.
with ubunye.notebook(
    str(HERE / "pipelines" / "demo" / "etl" / "clean_data"),
    dt="data/raw_events.parquet",
    spark=spark,
    run_records=".ubunye/runs",
) as run:
    # READ: load every input under CONFIG.inputs in config.yaml, one DataFrame
    # per input. Nothing is changed yet.
    sources = run.read()

    # TRANSFORM: run transformations.py. One DataFrame per output.
    # All the business logic is in that file.
    outputs = run.transform(sources)

    # WRITE: check the data-quality rules first. If one fails, nothing is
    # written and the run is recorded as failed. Otherwise each output is
    # written to its table or path.
    run.write(outputs)

for name, df in outputs.items():
    print(f"\n--- {name} ---")
    df.show(5)
