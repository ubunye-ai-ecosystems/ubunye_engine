"""Spark unit tests for the multi-task Titanic pipeline (Databricks variant).

Exercises both tasks end-to-end on a local SparkSession: clean_data produces
the intermediate DataFrame that aggregate consumes. This validates the
contract between the two tasks.

Each task has its own ``transformations.py``, so we load them via importlib
under distinct names to avoid the sys.modules collision that identical
module names would cause.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipelines" / "titanic" / "pipeline"


def _load_module(name: str, path: Path):
    """Load a Python file as a module under a unique name."""
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


clean_mod = _load_module(
    "clean_transformations", PIPELINE_DIR / "clean_data" / "transformations.py"
)
agg_mod = _load_module("agg_transformations", PIPELINE_DIR / "aggregate" / "transformations.py")


@pytest.fixture
def raw_passengers(spark):
    """Small hand-computed fixture with known outcomes."""
    return spark.createDataFrame(
        [
            (1, 1, 1, "male", 30.0, "Allen"),
            (2, 1, 0, "female", 10.0, "Brown"),
            (3, 2, 1, "male", 45.0, "Clark"),
            (4, 2, 0, "female", 8.0, "Davis"),
            (5, 3, 1, "male", 22.0, "Evans"),
            (6, 3, 0, "female", None, "Frank"),
        ],
        ["PassengerId", "Pclass", "Survived", "Sex", "Age", "Name"],
    )


class TestCleanData:
    def test_adds_survived_label(self, raw_passengers):
        result = clean_mod.clean_titanic(raw_passengers)
        labels = {r["PassengerId"]: r["survived_label"] for r in result.collect()}
        assert labels[1] == "yes"
        assert labels[2] == "no"

    def test_adds_age_group(self, raw_passengers):
        result = clean_mod.clean_titanic(raw_passengers)
        groups = {r["PassengerId"]: r["age_group"] for r in result.collect()}
        assert groups[1] == "adult"  # age 30
        assert groups[2] == "child"  # age 10

    def test_preserves_all_rows_when_no_nulls_in_key_cols(self, raw_passengers):
        result = clean_mod.clean_titanic(raw_passengers)
        assert result.count() == 6

    def test_missing_column_raises(self, spark):
        bad = spark.createDataFrame([(1, 1)], ["PassengerId", "Pclass"])
        with pytest.raises(ValueError, match="Missing required columns"):
            clean_mod.clean_titanic(bad)


class TestAggregate:
    @pytest.fixture
    def cleaned_df(self, raw_passengers):
        return clean_mod.clean_titanic(raw_passengers)

    def test_groups_by_class_and_age(self, cleaned_df):
        result = agg_mod.aggregate_survival(cleaned_df)
        assert set(result.columns) == set(
            ("Pclass", "age_group", "passenger_count", "survivors_count", "survival_rate")
        )
        assert result.count() > 0

    def test_survival_rate_bounded(self, cleaned_df):
        result = agg_mod.aggregate_survival(cleaned_df)
        for row in result.collect():
            assert 0.0 <= row["survival_rate"] <= 1.0

    def test_missing_column_raises(self, spark):
        bad = spark.createDataFrame([(1,)], ["Pclass"])
        with pytest.raises(ValueError, match="Missing required columns"):
            agg_mod.aggregate_survival(bad)
