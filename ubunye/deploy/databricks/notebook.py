"""Generate a Databricks Python notebook that runs an Ubunye task."""

from __future__ import annotations

from typing import Optional


def generate_notebook(
    workspace_task_path: str,
    mode: str = "PROD",
    dt: Optional[str] = None,
    pip_install: str = "ubunye-engine",
) -> str:
    """Return the source of a Databricks notebook that runs the deployed task.

    The generated notebook:
    1. ``%pip`` installs the engine and restarts the Python process
    2. Calls ``ubunye.run_task()`` to reuse the active SparkSession
    3. Prints the output table names

    Uses the Databricks ``.py`` source notebook format: ``# MAGIC`` prefix
    for magic commands, ``# COMMAND ----------`` as cell separators.
    """
    dt_kwarg = f'    dt="{dt}",\n' if dt else ""

    return f"""\
# Databricks notebook source
# MAGIC %pip install {pip_install}

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import ubunye

outputs = ubunye.run_task(
    task_dir="{workspace_task_path}",
    mode="{mode}",
{dt_kwarg})

# COMMAND ----------

for name, df in outputs.items():
    print(f"Output: {{name}} — columns: {{df.columns}}")
    df.show(5)
"""
