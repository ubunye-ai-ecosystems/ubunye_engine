"""Unit tests for ``TitanicSurvivalModel``.

Pandas-only — exercises the UbunyeModel contract (train, predict, save,
load, metadata) without needing Spark. The model.predict() path on a
pandas DataFrame returns a pandas DataFrame; the Spark path is exercised
implicitly on Databricks.
"""

from __future__ import annotations

import pandas as pd
import pytest

from model import FEATURES, TARGET, TitanicSurvivalModel


@pytest.fixture
def titanic_df() -> pd.DataFrame:
    # A tiny, deterministic dataset large enough for stratified split on the target.
    rows = []
    for i in range(80):
        survived = i % 3 == 0
        rows.append(
            {
                "Pclass": 1 if survived else 3,
                "Sex": "female" if survived else "male",
                "Age": 30.0 + (i % 10),
                "SibSp": i % 2,
                "Parch": 0,
                "Fare": 50.0 + (i % 20),
                "Embarked": "S" if i % 2 else "C",
                TARGET: int(survived),
            }
        )
    return pd.DataFrame(rows)


def test_train_returns_headline_metrics(titanic_df: pd.DataFrame) -> None:
    model = TitanicSurvivalModel(n_estimators=20, max_depth=3)
    metrics = model.train(titanic_df)
    assert set(metrics) >= {"accuracy", "auc", "f1", "rows"}
    assert 0.0 <= metrics["accuracy"] <= 1.0
    assert 0.0 <= metrics["auc"] <= 1.0
    assert metrics["rows"] > 0


def test_predict_on_pandas_adds_prediction_and_proba(titanic_df: pd.DataFrame) -> None:
    model = TitanicSurvivalModel(n_estimators=20, max_depth=3)
    model.train(titanic_df)

    scored = model.predict(titanic_df[FEATURES])
    assert "prediction" in scored.columns
    assert "proba" in scored.columns
    assert scored["prediction"].isin([0, 1]).all()
    assert scored["proba"].between(0.0, 1.0).all()


def test_save_and_load_roundtrip(titanic_df: pd.DataFrame, tmp_path) -> None:
    model = TitanicSurvivalModel(n_estimators=20, max_depth=3)
    model.train(titanic_df)

    artifact_dir = tmp_path / "model"
    model.save(str(artifact_dir))
    assert (artifact_dir / "pipeline.joblib").exists()

    loaded = TitanicSurvivalModel.load(str(artifact_dir))
    original = model.predict(titanic_df[FEATURES])
    reloaded = loaded.predict(titanic_df[FEATURES])
    pd.testing.assert_series_equal(
        original["prediction"].reset_index(drop=True),
        reloaded["prediction"].reset_index(drop=True),
    )


def test_metadata_reports_library_and_features() -> None:
    model = TitanicSurvivalModel(n_estimators=50, max_depth=4)
    meta = model.metadata()
    assert meta["library"] == "sklearn"
    assert meta["features"] == FEATURES
    assert meta["params"] == {"n_estimators": 50, "max_depth": 4}


def test_validate_returns_same_shape_as_train(titanic_df: pd.DataFrame) -> None:
    model = TitanicSurvivalModel(n_estimators=20, max_depth=3)
    model.train(titanic_df)
    val_metrics = model.validate(titanic_df)
    assert set(val_metrics) >= {"accuracy", "auc", "f1", "rows"}
