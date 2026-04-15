"""Pytest fixtures: sys.path for transformations + shared SparkSession."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

TASK_DIR = (
    Path(__file__).resolve().parent.parent
    / "pipelines"
    / "jhb_weather"
    / "ingestion"
    / "hourly_forecast"
)

if str(TASK_DIR) not in sys.path:
    sys.path.insert(0, str(TASK_DIR))


@pytest.fixture(scope="session")
def spark():
    """Module-scoped local SparkSession for the example's tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("jhb-weather-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    yield session
    session.stop()
