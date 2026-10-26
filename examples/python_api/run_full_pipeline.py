"""Run a multi-task pipeline via the Python API.

This script demonstrates ``ubunye.run_pipeline()`` — running multiple
tasks sequentially with a shared SparkSession.

Usage
-----
    pip install ubunye-engine[spark]
    cd examples/python_api
    python run_full_pipeline.py
"""

from pathlib import Path

from pyspark.sql import SparkSession

import ubunye

HERE = Path(__file__).resolve().parent

# Pass the session in so it stays open after the run: a session that
# run_task() starts itself is stopped when the task ends, and then the
# returned DataFrames can no longer be shown or counted.
spark = SparkSession.builder.appName("ubunye-python-api").getOrCreate()

# A few sample events (one duplicate, one null) if there is no input yet.
# The config reads the path given as ``dt``, relative to where you run this.
if not Path("data/raw_events.parquet").exists():
    spark.createDataFrame(
        [(1, "a", 10.0), (2, "b", 5.5), (2, "b", 5.5), (3, "c", 7.25), (4, None, 1.0)],
        "event_id INT, user STRING, amount DOUBLE",
    ).write.parquet("data/raw_events.parquet")

results = ubunye.run_pipeline(
    usecase_dir=str(HERE / "pipelines"),
    usecase="demo",
    package="etl",
    tasks=["clean_data", "aggregate"],
    mode="DEV",
    dt="data/raw_events.parquet",
    spark=spark,
)

for task_name, outputs in results.items():
    print(f"\n=== {task_name} ===")
    for name, df in outputs.items():
        print(f"  {name}: {df.count()} rows")
        df.show(5)
