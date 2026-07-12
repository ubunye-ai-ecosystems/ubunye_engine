"""Integration-tier fixtures.

The one thing that has to happen here rather than in a test module: Spark
resolves ``spark.jars.packages`` when the **JVM launches**, and the first
SparkSession in the process is what launches it. If a plain session is created
first (the lineage tests do exactly that), Delta's jars can never be added
afterwards — every Delta test then dies with ``ClassNotFoundException:
delta.DefaultSource`` no matter how the session is configured later.

So the Delta-enabled session is built up front, session-scoped and autouse.
Other modules' ``getOrCreate()`` calls reuse it.
"""

from __future__ import annotations

import importlib.util

import pytest

_HAS_DELTA = importlib.util.find_spec("delta") is not None

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"


@pytest.fixture(scope="session", autouse=True)
def _spark_jvm_with_delta():
    """Launch the JVM with Delta's jars before any test creates a session."""
    if not _HAS_DELTA:
        return False

    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[1]")
            .appName("ubunye-integration")
            .config("spark.sql.extensions", DELTA_EXTENSION)
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "512m")
        )
        configure_spark_with_delta_pip(builder).getOrCreate()
        return True
    except Exception:  # jars unresolvable (no network, version skew) — Delta tests skip
        return False
