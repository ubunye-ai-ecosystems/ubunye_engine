"""Clean Titanic data: drop rows with missing survival/class info, add derived columns.

Written with Narwhals, so the same code runs on Spark and on the pandas
backend with the same result (ADR 005).
"""

from __future__ import annotations

from typing import Any, Dict

import narwhals as nw

from ubunye.core.interfaces import Task

REQUIRED_COLUMNS = ("PassengerId", "Pclass", "Survived", "Sex", "Age")


def clean_titanic(df: Any) -> Any:
    """Takes and returns the engine's own frame (Spark or pandas)."""
    frame = nw.from_native(df)
    missing = set(REQUIRED_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return (
        frame.filter(~nw.col("Survived").is_null() & ~nw.col("Pclass").is_null())
        .with_columns(
            survived_label=nw.when(nw.col("Survived") == 1)
            .then(nw.lit("yes"))
            .otherwise(nw.lit("no")),
            age_group=nw.when(nw.col("Age") < 18).then(nw.lit("child")).otherwise(nw.lit("adult")),
        )
        .to_native()
    )


class CleanData(Task):
    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        return {"cleaned": clean_titanic(sources["titanic"])}
