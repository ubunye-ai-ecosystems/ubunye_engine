"""Titanic survival analytics - compute survival rate per passenger class.

Portability contract: this module is imported verbatim by both runtimes in
examples/production/ (titanic_local and titanic_databricks). The business
logic lives here; only the config.yaml and deployment wrappers differ per
runtime.

Single implementation - Spark only. Tested via a local SparkSession
fixture in ``tests/test_transformations.py``.
"""

from __future__ import annotations

from typing import Any, Dict

from ubunye.core.interfaces import Task

OUTPUT_COLUMNS = ("Pclass", "passenger_count", "survivors_count", "survival_rate")


def compute_survival_by_class(df: "Any") -> "Any":
    """Group by Pclass; emit passenger count, survivors, survival rate.

    Expected input columns: ``PassengerId``, ``Pclass``, ``Survived``.
    Survival rate is rounded to 4 decimal places so golden-file
    comparisons are exact across runs.
    """
    from pyspark.sql import functions as F

    required = {"PassengerId", "Pclass", "Survived"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Input DataFrame must contain columns: {sorted(required)} (missing: {sorted(missing)})"
        )

    return (
        df.groupBy("Pclass")
        .agg(
            F.count("PassengerId").alias("passenger_count"),
            F.sum(F.col("Survived").cast("long")).alias("survivors_count"),
        )
        .withColumn(
            "survival_rate",
            F.round(F.col("survivors_count") / F.col("passenger_count"), 4),
        )
        .orderBy("Pclass")
        .select(*OUTPUT_COLUMNS)
    )


class SurvivalByClass(Task):
    """Ubunye Task: survival rate per passenger class from the Titanic dataset."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        titanic = sources["titanic"]
        return {"survival_by_class": compute_survival_by_class(titanic)}
