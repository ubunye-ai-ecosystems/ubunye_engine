"""Spark unit tests for the Titanic survival-by-class transformation.

Exercises ``compute_survival_by_class`` end-to-end on a local SparkSession
so the production code is the code under test.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from transformations import (  # noqa: E402 (conftest mutates sys.path)
    OUTPUT_COLUMNS,
    compute_survival_by_class,
)

EXPECTED_OUTPUT = (
    Path(__file__).resolve().parent.parent / "expected_output" / "survival_by_class.parquet"
)


@pytest.fixture
def toy_passengers_df(spark):
    """Small hand-computed fixture: 2 classes, known survival outcomes."""
    return spark.createDataFrame(
        [
            (1, 1, 1, "A"),
            (2, 1, 0, "B"),
            (3, 1, 1, "C"),
            (4, 2, 1, "D"),
            (5, 2, 1, "E"),
            (6, 2, 0, "F"),
        ],
        ["PassengerId", "Pclass", "Survived", "Name"],
    )


def test_schema_matches_contract(toy_passengers_df):
    result = compute_survival_by_class(toy_passengers_df)
    assert tuple(result.columns) == OUTPUT_COLUMNS


def test_aggregation_values(toy_passengers_df):
    rows = {r["Pclass"]: r for r in compute_survival_by_class(toy_passengers_df).collect()}
    # Pclass 1: 3 passengers, 2 survived -> 0.6667
    # Pclass 2: 3 passengers, 2 survived -> 0.6667
    assert rows[1]["passenger_count"] == 3
    assert rows[1]["survivors_count"] == 2
    assert rows[1]["survival_rate"] == pytest.approx(0.6667)
    assert rows[2]["passenger_count"] == 3


def test_missing_column_raises(spark):
    bad = spark.createDataFrame([(1, 1)], ["PassengerId", "Pclass"])  # no Survived
    with pytest.raises(ValueError, match="must contain columns"):
        compute_survival_by_class(bad)


def test_deterministic_ordering(spark):
    """Output must be sorted by Pclass regardless of input order."""
    shuffled = spark.createDataFrame(
        [(1, 3, 0), (2, 1, 1), (3, 2, 0), (4, 1, 1)],
        ["PassengerId", "Pclass", "Survived"],
    )
    result = compute_survival_by_class(shuffled).collect()
    assert [r["Pclass"] for r in result] == [1, 2, 3]


@pytest.mark.skipif(not EXPECTED_OUTPUT.exists(), reason="golden parquet not present")
def test_golden_matches_canonical_titanic_stats(spark):
    """The committed golden output must match the canonical figures for the
    full 891-passenger Titanic training set."""
    golden = spark.read.parquet(str(EXPECTED_OUTPUT)).orderBy("Pclass").collect()
    expected = [
        (1, 216, 136, 0.6296),
        (2, 184, 87, 0.4728),
        (3, 491, 119, 0.2424),
    ]
    assert len(golden) == 3
    for row, (pclass, pcount, survivors, rate) in zip(golden, expected):
        assert row["Pclass"] == pclass
        assert row["passenger_count"] == pcount
        assert row["survivors_count"] == survivors
        assert row["survival_rate"] == pytest.approx(rate, abs=1e-4)
