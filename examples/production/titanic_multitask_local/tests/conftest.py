"""Pytest fixtures: sys.path for transformations + shared SparkSession."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipelines" / "titanic" / "pipeline"
CLEAN_DIR = PIPELINE_DIR / "clean_data"
AGG_DIR = PIPELINE_DIR / "aggregate"

for d in (CLEAN_DIR, AGG_DIR):
    if str(d) not in sys.path:
        sys.path.insert(0, str(d))


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for the example's tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("titanic-multitask-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    yield session
    session.stop()
