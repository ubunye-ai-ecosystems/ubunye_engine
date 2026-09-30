"""E-06: the job, written once, run plain or through Ubunye.

The same functions do the work in both cases, so a difference in time is the
engine's, not the logic's. ``python e06_logic.py pandas|spark ROOT`` is the plain
run: read, transform, write, with nothing of Ubunye imported. The Ubunye task
folders copy this file next to their ``transformations.py``.

The job (all integers, so every sum is exact and every backend gives the same
rows whatever the order):

* read ``events`` (N rows) and ``regions`` (1,000 rows);
* keep rows with ``qty > 0``;
* derive ``revenue = amount * qty`` and ``big = revenue > 500000``;
* join to ``regions`` (inner: regions 900 to 999 have no row there);
* ``detail``: the joined rows, appended;
* ``summary``: rows, revenue and qty per region name and category, overwritten.
"""

from __future__ import annotations

import os
import sys


def pandas_job(events, regions):
    """pandas: two frames in, {"detail", "summary"} out."""
    f = events[events["qty"] > 0].copy()
    f["revenue"] = f["amount"] * f["qty"]
    f["big"] = f["revenue"] > 500000
    detail = f.merge(regions, on="region", how="inner")
    summary = detail.groupby(["region_name", "cat"], as_index=False).agg(
        n=("id", "size"), revenue=("revenue", "sum"), qty=("qty", "sum")
    )
    return {"detail": detail, "summary": summary}


def spark_job(events, regions):
    """PySpark: two DataFrames in, {"detail", "summary"} out."""
    from pyspark.sql import functions as F

    f = events.where(F.col("qty") > 0)
    f = f.withColumn("revenue", F.col("amount") * F.col("qty"))
    f = f.withColumn("big", F.col("revenue") > F.lit(500000))
    detail = f.join(regions, on="region", how="inner")
    summary = detail.groupBy("region_name", "cat").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum("revenue").alias("revenue"),
        F.sum("qty").alias("qty"),
    )
    return {"detail": detail, "summary": summary}


def plain_pandas(root: str) -> None:
    import pandas as pd

    events = pd.read_parquet(os.path.join(root, "data", "events.parquet"))
    regions = pd.read_parquet(os.path.join(root, "data", "regions.parquet"))
    out = pandas_job(events, regions)
    detail_dir = os.path.join(root, "out", "detail")
    os.makedirs(detail_dir, exist_ok=True)
    out["detail"].to_parquet(os.path.join(detail_dir, "part-0.parquet"), index=False)
    summary_dir = os.path.join(root, "out", "summary")
    os.makedirs(summary_dir, exist_ok=True)
    out["summary"].to_parquet(os.path.join(summary_dir, "part-0.parquet"), index=False)


def plain_spark(root: str) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("e06-plain").getOrCreate()
    try:
        events = spark.read.parquet(os.path.join(root, "data", "events.parquet"))
        regions = spark.read.parquet(os.path.join(root, "data", "regions.parquet"))
        out = spark_job(events, regions)
        out["detail"].write.mode("append").format("parquet").save(
            os.path.join(root, "out", "detail")
        )
        out["summary"].write.mode("overwrite").format("parquet").save(
            os.path.join(root, "out", "summary")
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    kind, where = sys.argv[1], sys.argv[2]
    (plain_pandas if kind == "pandas" else plain_spark)(where)
