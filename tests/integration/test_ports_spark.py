"""The claim that made the data port additive: a Spark DataFrame fits it NATIVELY.

Verified against a real session, in the tier that has one — not assumed from the docs.
(Found while writing this: pyspark 3.5 workers crash under Python 3.13, so locally this
needs 3.9-3.12. CI's integration tier runs supported versions.)
"""

from __future__ import annotations

import pytest

from ubunye.core.ports import DataFramePort

pytestmark = pytest.mark.integration


def test_a_spark_dataframe_satisfies_the_port_without_an_adapter():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("ubunye-port-check")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    df = spark.range(3).toDF("id")  # JVM-side data: no Python workers involved
    assert isinstance(df, DataFramePort), "Spark stopped fitting the port"
    assert df.count() == 3
    assert [r["id"] for r in df.collect()] == [0, 1, 2]
