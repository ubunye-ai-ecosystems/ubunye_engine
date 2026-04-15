"""Titanic survival classifier — a ``UbunyeModel`` subclass.

Random Forest over the classic Titanic features with a scikit-learn
``ColumnTransformer`` for numeric scaling + categorical one-hot encoding.
The model is trained on pandas (via ``df.toPandas()``) because Titanic is
small enough that distributed training would be pure overhead.

A byte-identical copy of this file lives under ``predict_classifier/`` so
that both tasks can load the class without a shared-package import hack.
The CI workflow enforces the contract by running ``diff`` between the two.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import joblib

from ubunye.models.base import UbunyeModel


FEATURES = ["Pclass", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked"]
TARGET = "Survived"


class TitanicSurvivalModel(UbunyeModel):
    """sklearn ``RandomForestClassifier`` wrapped in the Ubunye contract."""

    def __init__(self, *, n_estimators: int = 200, max_depth: int = 6) -> None:
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self._pipeline = None
        self._metrics: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # UbunyeModel contract
    # ------------------------------------------------------------------

    def train(self, df: Any) -> Dict[str, Any]:
        pdf = df.toPandas() if hasattr(df, "toPandas") else df
        pdf = pdf.dropna(subset=FEATURES + [TARGET])

        self._pipeline = _build_pipeline(self.n_estimators, self.max_depth)
        X, y = pdf[FEATURES], pdf[TARGET].astype(int)

        from sklearn.model_selection import train_test_split

        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        self._pipeline.fit(X_train, y_train)
        self._metrics = _evaluate(self._pipeline, X_val, y_val)
        return dict(self._metrics)

    def predict(self, df: Any) -> Any:
        pdf = df.toPandas() if hasattr(df, "toPandas") else df
        pdf = pdf.copy()
        pdf = pdf.dropna(subset=FEATURES)

        X = pdf[FEATURES]
        pdf["prediction"] = self._pipeline.predict(X)
        pdf["proba"] = self._pipeline.predict_proba(X)[:, 1]

        if hasattr(df, "sparkSession"):
            return df.sparkSession.createDataFrame(pdf)
        return pdf

    def save(self, path: str) -> None:
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._pipeline, target / "pipeline.joblib")

    @classmethod
    def load(cls, path: str) -> "TitanicSurvivalModel":
        instance = cls()
        instance._pipeline = joblib.load(Path(path) / "pipeline.joblib")
        return instance

    def metadata(self) -> Dict[str, Any]:
        import sklearn

        return {
            "library": "sklearn",
            "library_version": sklearn.__version__,
            "features": FEATURES,
            "target": TARGET,
            "params": {
                "n_estimators": self.n_estimators,
                "max_depth": self.max_depth,
            },
        }

    def validate(self, df: Any) -> Dict[str, Any]:
        pdf = df.toPandas() if hasattr(df, "toPandas") else df
        pdf = pdf.dropna(subset=FEATURES + [TARGET])
        return _evaluate(self._pipeline, pdf[FEATURES], pdf[TARGET].astype(int))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_pipeline(n_estimators: int, max_depth: int):
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    categorical = ["Sex", "Embarked"]
    numeric = [c for c in FEATURES if c not in categorical]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    [("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]
                ),
                numeric,
            ),
            (
                "cat",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical,
            ),
        ],
        remainder="drop",
    )
    return Pipeline(
        [
            ("preprocessor", preprocessor),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=n_estimators,
                    max_depth=max_depth,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )


def _evaluate(pipeline, X, y) -> Dict[str, float]:
    from sklearn.metrics import accuracy_score, f1_score, roc_auc_score

    y_pred = pipeline.predict(X)
    y_proba = pipeline.predict_proba(X)[:, 1]
    return {
        "accuracy": float(accuracy_score(y, y_pred)),
        "f1": float(f1_score(y, y_pred)),
        "auc": float(roc_auc_score(y, y_proba)),
        "rows": int(len(y)),
    }
