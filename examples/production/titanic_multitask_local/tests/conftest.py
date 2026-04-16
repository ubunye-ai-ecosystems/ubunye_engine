"""Pytest fixtures: shared SparkSession for the multi-task example tests."""

from __future__ import annotations

import pytest


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
