"""Titanic survival analytics — compute survival rate per passenger class.

Portability contract: this module is imported verbatim by both runtimes in
examples/production/ (titanic_local and titanic_databricks). The business logic
lives here; only the config.yaml and deployment wrappers differ per runtime.

Two implementations are provided:

* ``compute_survival_by_class_spark`` — Spark DataFrame API. Runs inside the
  Ubunye Task.transform contract. Distributed; suitable for arbitrarily large
  inputs.
* ``compute_survival_by_class_pandas`` — equivalent pandas expression. Used
  by unit tests so the business logic can be exercised without a SparkSession
  or Java toolchain.

Both functions are pure — given the same input, they return the same output
columns in the same order. The pandas version exists as a test double, not
as a production path.
"""

from __future__ import annotations

from typing import Any, Dict

from ubunye.core.interfaces import Task

OUTPUT_COLUMNS = ("Pclass", "passenger_count", "survivors_count", "survival_rate")


def compute_survival_by_class_pandas(df: "Any") -> "Any":
    """Pandas implementation of the survival-by-class aggregation.

    Expected input columns: ``PassengerId``, ``Pclass``, ``Survived``.
    Survival rate is rounded to 4 decimal places — matches the Spark version
    so golden-file comparisons are exact.
    """
    import pandas as pd

    if not {"PassengerId", "Pclass", "Survived"}.issubset(df.columns):
        raise ValueError(
            "Input DataFrame must contain columns: PassengerId, Pclass, Survived"
        )

    result = (
        df.groupby("Pclass", as_index=False)
        .agg(
            passenger_count=("PassengerId", "count"),
            survivors_count=("Survived", "sum"),
        )
        .sort_values("Pclass")
        .reset_index(drop=True)
    )
    result["survival_rate"] = (
        (result["survivors_count"] / result["passenger_count"]).round(4)
    )
    return result[list(OUTPUT_COLUMNS)].astype(
        {"Pclass": "int64", "passenger_count": "int64", "survivors_count": "int64"}
    )


def compute_survival_by_class_spark(df: "Any") -> "Any":
    """Spark implementation — identical semantics to the pandas version."""
    from pyspark.sql import functions as F

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
        return {"survival_by_class": compute_survival_by_class_spark(titanic)}
