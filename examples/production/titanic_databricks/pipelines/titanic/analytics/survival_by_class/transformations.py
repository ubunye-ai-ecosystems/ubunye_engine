"""Titanic survival analytics - compute survival rate per passenger class.

Portability contract: this module is imported verbatim by both runtimes in
examples/production/ (titanic_local and titanic_databricks). The business
logic lives here; only the config.yaml and deployment wrappers differ per
runtime.

One implementation for every engine, written with Narwhals: it runs on Spark
(locally and on Databricks) and on the pandas backend, and both give the same
data hash in the run record (ADR 005). Tested on both in
``tests/test_transformations.py``.
"""

from __future__ import annotations

from typing import Any, Dict

import narwhals as nw

from ubunye.core.interfaces import Task

OUTPUT_COLUMNS = ("Pclass", "passenger_count", "survivors_count", "survival_rate")


def compute_survival_by_class(df: Any) -> Any:
    """Group by Pclass; emit passenger count, survivors, survival rate.

    Takes and returns the engine's own frame (Spark or pandas). Expected input
    columns: ``PassengerId``, ``Pclass``, ``Survived``. Survival rate is rounded
    to 4 decimal places so golden-file comparisons are exact across runs.
    """
    frame = nw.from_native(df)
    required = {"PassengerId", "Pclass", "Survived"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(
            f"Input DataFrame must contain columns: {sorted(required)} (missing: {sorted(missing)})"
        )

    return (
        # Spark makes every sum a 64 bit integer; cast first so pandas does too
        # (before the group by, so the sum stays a plain, fast aggregation).
        frame.with_columns(nw.col("Survived").cast(nw.Int64))
        .group_by("Pclass")
        .agg(
            nw.col("PassengerId").count().alias("passenger_count"),
            nw.col("Survived").sum().alias("survivors_count"),
        )
        .with_columns(
            survival_rate=(nw.col("survivors_count") / nw.col("passenger_count")).round(4)
        )
        .sort("Pclass")
        .select(*OUTPUT_COLUMNS)
        .to_native()
    )


class SurvivalByClass(Task):
    """Ubunye Task: survival rate per passenger class from the Titanic dataset."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        titanic = sources["titanic"]
        return {"survival_by_class": compute_survival_by_class(titanic)}
