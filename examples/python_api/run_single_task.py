"""Run a single Ubunye task via the Python API.

This script demonstrates ``ubunye.run_task()`` — the simplest way to
execute one task from Python code or a Databricks notebook.

Usage
-----
    pip install ubunye-engine[spark]
    python run_single_task.py
"""

from pathlib import Path

import ubunye

HERE = Path(__file__).resolve().parent

outputs = ubunye.run_task(
    task_dir=str(HERE / "pipelines" / "demo" / "etl" / "clean_data"),
    mode="DEV",
    dt="data/raw_events.parquet",
)

print("Task outputs:", list(outputs.keys()))
for name, df in outputs.items():
    print(f"\n--- {name} ---")
    df.show(5)
