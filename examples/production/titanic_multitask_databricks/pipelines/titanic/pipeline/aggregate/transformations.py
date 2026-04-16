"""Aggregate cleaned Titanic data: survival stats by class and age group."""

from __future__ import annotations

from typing import Any, Dict

from ubunye.core.interfaces import Task

OUTPUT_COLUMNS = ("Pclass", "age_group", "passenger_count", "survivors_count", "survival_rate")


def aggregate_survival(df: "Any") -> "Any":
    from pyspark.sql import functions as F

    required = {"Pclass", "Survived", "age_group"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return (
        df.groupBy("Pclass", "age_group")
        .agg(
            F.count("*").alias("passenger_count"),
            F.sum(F.col("Survived").cast("long")).alias("survivors_count"),
        )
        .withColumn(
            "survival_rate",
            F.round(F.col("survivors_count") / F.col("passenger_count"), 4),
        )
        .orderBy("Pclass", "age_group")
        .select(*OUTPUT_COLUMNS)
    )


class Aggregate(Task):
    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        return {"survival_summary": aggregate_survival(sources["cleaned"])}
