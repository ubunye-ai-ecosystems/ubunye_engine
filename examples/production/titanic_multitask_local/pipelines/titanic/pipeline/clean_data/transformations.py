"""Clean Titanic data: drop rows with missing survival/class info, add derived columns."""

from __future__ import annotations

from typing import Any, Dict

from ubunye.core.interfaces import Task

REQUIRED_COLUMNS = ("PassengerId", "Pclass", "Survived", "Sex", "Age")


def clean_titanic(df: "Any") -> "Any":
    from pyspark.sql import functions as F

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return (
        df.filter(F.col("Survived").isNotNull() & F.col("Pclass").isNotNull())
        .withColumn("survived_label", F.when(F.col("Survived") == 1, "yes").otherwise("no"))
        .withColumn("age_group", F.when(F.col("Age") < 18, "child").otherwise("adult"))
    )


class CleanData(Task):
    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        return {"cleaned": clean_titanic(sources["titanic"])}
