"""
Scikit-learn model wrapper implementing BaseModel.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import joblib

from .adapters import ensure_Xy_numpy
from .base import BaseModel, FeatureSchema


class SklearnModel(BaseModel):
    """Wrap any sklearn estimator with a unified Ubunye interface."""

    def __init__(self, estimator: Any, *, schema: Optional[FeatureSchema] = None, **kwargs: Any) -> None:
        super().__init__(schema=schema, **kwargs)
        self.estimator = estimator
        self._last_metrics: Dict[str, float] = {}

    # ---- core hooks ----
    def _fit_core(self, X: Any, y: Optional[Any]) -> None:
        feats = self.schema.features if self.schema else None
        target = self.schema.target if self.schema else None
        X_np, y_np = ensure_Xy_numpy(X, feats, target)
        if y_np is None and y is not None:
            y_np = y
        self.estimator.fit(X_np, y_np)

    def _predict_core(self, X: Any, proba: bool = False) -> Any:
        feats = self.schema.features if self.schema else None
        target = self.schema.target if self.schema else None
        X_np, _ = ensure_Xy_numpy(X, feats, target)
        if proba and hasattr(self.estimator, "predict_proba"):
            probs = self.estimator.predict_proba(X_np)[:, -1]
            preds = (probs >= 0.5).astype(int)
            return preds, probs
        return self.estimator.predict(X_np)

    def _save_core(self, path: Path) -> None:
        joblib.dump(self.estimator, path / "model.joblib")

    def _load_core(self, path: Path) -> None:
        self.estimator = joblib.load(path / "model.joblib")

    @property
    def params(self):
        base = super().params
        try:
            base.update(self.estimator.get_params())
        except Exception:
            pass
        return base
