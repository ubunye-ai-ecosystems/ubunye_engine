"""Run a multi-task pipeline via the Python API.

This script demonstrates ``ubunye.run_pipeline()`` — running multiple
tasks sequentially with a shared SparkSession.

Usage
-----
    pip install ubunye-engine[spark]
    python run_full_pipeline.py
"""

from pathlib import Path

import ubunye

HERE = Path(__file__).resolve().parent

results = ubunye.run_pipeline(
    usecase_dir=str(HERE / "pipelines"),
    usecase="demo",
    package="etl",
    tasks=["clean_data", "aggregate"],
    mode="DEV",
    dt="data/raw_events.parquet",
)

for task_name, outputs in results.items():
    print(f"\n=== {task_name} ===")
    for name, df in outputs.items():
        print(f"  {name}: {df.count()} rows")
        df.show(5)
