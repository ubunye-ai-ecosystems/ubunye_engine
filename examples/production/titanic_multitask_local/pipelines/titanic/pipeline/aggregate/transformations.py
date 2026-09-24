"""Aggregate cleaned Titanic data: survival stats by class and age group.

Written with Narwhals, so the same code runs on Spark and on the pandas
backend with the same result (ADR 005).
"""

from __future__ import annotations

from typing import Any, Dict

import narwhals as nw

from ubunye.core.interfaces import Task

OUTPUT_COLUMNS = ("Pclass", "age_group", "passenger_count", "survivors_count", "survival_rate")


def aggregate_survival(df: Any) -> Any:
    """Takes and returns the engine's own frame (Spark or pandas)."""
    frame = nw.from_native(df)
    required = {"Pclass", "Survived", "age_group"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return (
        # Spark makes every sum a 64 bit integer; cast first so pandas does too
        # (before the group by, so the sum stays a plain, fast aggregation).
        frame.with_columns(nw.col("Survived").cast(nw.Int64))
        .group_by("Pclass", "age_group")
        .agg(
            nw.len().alias("passenger_count"),
            nw.col("Survived").sum().alias("survivors_count"),
        )
        .with_columns(
            survival_rate=(nw.col("survivors_count") / nw.col("passenger_count")).round(4)
        )
        .sort("Pclass", "age_group")
        .select(*OUTPUT_COLUMNS)
        .to_native()
    )


class Aggregate(Task):
    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        return {"survival_summary": aggregate_survival(sources["cleaned"])}
