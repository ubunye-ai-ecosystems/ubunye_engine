"""Unit tests for the Titanic survival-by-class transformation.

These tests exercise ``compute_survival_by_class_pandas``. The pandas and Spark
implementations in transformations.py share the same contract, so the pandas
tests are the source of truth for the business logic. An integration test that
runs the Spark version end-to-end is provided in the CI workflow itself.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from transformations import (  # noqa: E402  (conftest mutates sys.path)
    OUTPUT_COLUMNS,
    compute_survival_by_class_pandas,
)

EXPECTED_OUTPUT = (
    Path(__file__).resolve().parent.parent / "expected_output" / "survival_by_class.parquet"
)


@pytest.fixture
def toy_passengers() -> pd.DataFrame:
    """Small hand-computed fixture: 2 classes, known survival outcomes."""
    return pd.DataFrame(
        {
            "PassengerId": [1, 2, 3, 4, 5, 6],
            "Pclass": [1, 1, 1, 2, 2, 2],
            "Survived": [1, 0, 1, 1, 1, 0],
            "Name": ["A", "B", "C", "D", "E", "F"],
        }
    )


def test_schema_matches_contract(toy_passengers: pd.DataFrame) -> None:
    result = compute_survival_by_class_pandas(toy_passengers)
    assert tuple(result.columns) == OUTPUT_COLUMNS


def test_aggregation_values(toy_passengers: pd.DataFrame) -> None:
    result = compute_survival_by_class_pandas(toy_passengers)
    # Pclass 1: 3 passengers, 2 survived -> 0.6667
    # Pclass 2: 3 passengers, 2 survived -> 0.6667
    assert result.loc[result["Pclass"] == 1, "passenger_count"].iloc[0] == 3
    assert result.loc[result["Pclass"] == 1, "survivors_count"].iloc[0] == 2
    assert result.loc[result["Pclass"] == 1, "survival_rate"].iloc[0] == pytest.approx(0.6667)
    assert result.loc[result["Pclass"] == 2, "passenger_count"].iloc[0] == 3


def test_missing_column_raises() -> None:
    bad = pd.DataFrame({"PassengerId": [1], "Pclass": [1]})  # no Survived
    with pytest.raises(ValueError, match="must contain columns"):
        compute_survival_by_class_pandas(bad)


def test_deterministic_ordering() -> None:
    """Output must be sorted by Pclass regardless of input order."""
    shuffled = pd.DataFrame(
        {
            "PassengerId": [1, 2, 3, 4],
            "Pclass": [3, 1, 2, 1],
            "Survived": [0, 1, 0, 1],
        }
    )
    result = compute_survival_by_class_pandas(shuffled)
    assert list(result["Pclass"]) == [1, 2, 3]


@pytest.mark.skipif(
    not EXPECTED_OUTPUT.exists(), reason="golden parquet not present"
)
def test_golden_matches_canonical_titanic_stats() -> None:
    """The committed golden output must match the known canonical figures
    for the full 891-passenger Titanic training set. If someone regenerates
    the golden against different input, this test catches it."""
    golden = pd.read_parquet(EXPECTED_OUTPUT)
    expected = pd.DataFrame(
        {
            "Pclass": [1, 2, 3],
            "passenger_count": [216, 184, 491],
            "survivors_count": [136, 87, 119],
            "survival_rate": [0.6296, 0.4728, 0.2424],
        }
    )
    pd.testing.assert_frame_equal(
        golden.reset_index(drop=True),
        expected,
        check_dtype=False,
    )
