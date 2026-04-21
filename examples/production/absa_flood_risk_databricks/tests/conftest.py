"""Pytest fixtures: sys.path for both task dirs + shared SparkSession."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE = Path(__file__).resolve().parent.parent / "pipelines" / "flood" / "etl"
TASK_DIRS = [BASE / "geocode_addresses", BASE / "flood_risk"]

for task_dir in TASK_DIRS:
    if str(task_dir) not in sys.path:
        sys.path.insert(0, str(task_dir))


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for the flood-risk example's tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("absa-flood-risk-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    yield session
    session.stop()
