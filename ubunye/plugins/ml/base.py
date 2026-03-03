"""
Framework-agnostic ML base classes for Ubunye.

Defines a unified contract for model training/inference and common utilities:
- BaseModel: fit/predict/save/load/metrics/params
- BatchPredictMixin: efficient batch scoring on Spark/pandas
- HasSchema: feature/target schema handling
- MLflowLoggingMixin: optional MLflow logging (no hard dep)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Protocol, runtime_checkable


# --------- Schemas ---------
@dataclass
class FeatureSchema:
    features: Iterable[str]
    target: Optional[str] = None
    # optional: dtypes, categorical encodings, etc.


class HasSchema(ABC):
    """Models that can expose and validate their input/output schema."""

    def __init__(self, schema: Optional[FeatureSchema] = None) -> None:
        self._schema = schema

    @property
    def schema(self) -> Optional[FeatureSchema]:
        return self._schema

    def set_schema(self, schema: FeatureSchema) -> None:
        self._schema = schema


# --------- Data interchange (duck typing) ---------
@runtime_checkable
class PandasLike(Protocol):
    def __getitem__(self, cols: Iterable[str]): ...
    def to_numpy(self): ...


@runtime_checkable
class SparkDataFrameLike(Protocol):
    def select(self, *cols): ...
    def withColumn(self, name: str, col): ...
    def schema(self): ...


# --------- BaseModel contract ---------
class BaseModel(HasSchema, ABC):
    """
    Framework-agnostic estimator interface.

    Subclasses must implement:
      - _fit_core(X, y)
      - _predict_core(X) -> y_pred (and optionally probabilities)
      - _save_core(path)
      - _load_core(path)
      - params property (read-only view)
    """

    def __init__(self, *, schema: Optional[FeatureSchema] = None, **kwargs: Any) -> None:
        super().__init__(schema)
        self._params: Dict[str, Any] = kwargs
        self._is_fitted: bool = False

    # ---- public API ----
    def fit(self, X: Any, y: Optional[Any] = None) -> "BaseModel":
        """Fit the model; `X` can be Spark DF, pandas DF, numpy array, etc."""
        self._fit_core(X, y)
        self._is_fitted = True
        return self

    def predict(self, X: Any, proba: bool = False) -> Any:
        """Predict on inputs; returns labels or (labels, probabilities) if proba=True."""
        self._assert_fitted()
        return self._predict_core(X, proba=proba)

    def save(self, path: str | Path) -> None:
        """Persist model and metadata to a directory path."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        self._save_meta(path)
        self._save_core(path)

    def load(self, path: str | Path) -> "BaseModel":
        """Load model and metadata from path."""
        path = Path(path)
        self._load_meta(path)
        self._load_core(path)
        self._is_fitted = True
        return self

    # ---- metrics / params ----
    @property
    def params(self) -> Dict[str, Any]:
        """Model hyperparameters (read-only view)."""
        return dict(self._params)

    def metrics(self) -> Dict[str, Any]:
        """Optional: training/validation metrics (subclasses may override)."""
        return {}

    # ---- subclass hooks ----
    @abstractmethod
    def _fit_core(self, X: Any, y: Optional[Any]) -> None: ...

    @abstractmethod
    def _predict_core(self, X: Any, proba: bool = False) -> Any: ...

    @abstractmethod
    def _save_core(self, path: Path) -> None: ...

    @abstractmethod
    def _load_core(self, path: Path) -> None: ...

    # ---- internals ----
    def _save_meta(self, path: Path) -> None:
        meta = {
            "schema": asdict(self._schema) if self._schema else None,
            "params": self._params,
            "class": f"{self.__class__.__module__}.{self.__class__.__name__}",
            "version": "0.1.0",
        }
        (path / "ubunye_model.json").write_text(__import__("json").dumps(meta, indent=2))

    def _load_meta(self, path: Path) -> None:
        import json
        meta = json.loads((path / "ubunye_model.json").read_text())
        sch = meta.get("schema")
        if sch:
            self._schema = FeatureSchema(**sch)
        self._params.update(meta.get("params") or {})

    def _assert_fitted(self) -> None:
        if not self._is_fitted:
            raise RuntimeError("Model is not fitted. Call fit() before predict().")


# --------- Batch prediction on Spark/pandas ---------
class BatchPredictMixin:
    """Utility mixin for efficient batch inference on Spark or pandas."""

    def predict_on_spark(
        self, sdf: SparkDataFrameLike, *, output_col: str = "prediction", proba_col: Optional[str] = None
    ):
        """
        Apply predict() to a Spark DataFrame in a UDF/ Pandas UDF style.
        Subclasses can override to install vectorized UDF for speed.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType

        features = self.schema.features if self.schema else [c for c in sdf.columns if c != output_col]

        # Simple (non-vectorized) UDF baseline:
        def _predict_row(*cols):
            import numpy as np
            X = np.array(cols).reshape(1, -1)
            yhat = self.predict(X, proba=False)
            # yhat may be array-like
            return float(yhat[0]) if hasattr(yhat, "__len__") else float(yhat)

        udf_pred = F.udf(_predict_row, DoubleType())
        sdf_out = sdf.withColumn(output_col, udf_pred(*[sdf[c] for c in features]))

        # Optional probabilities
        if proba_col:
            def _predict_proba_row(*cols):
                import numpy as np
                X = np.array(cols).reshape(1, -1)
                _, p = self.predict(X, proba=True)
                return float(p[0] if hasattr(p, "__len__") else p)

            udf_proba = F.udf(_predict_proba_row, DoubleType())
            sdf_out = sdf_out.withColumn(proba_col, udf_proba(*[sdf[c] for c in features]))

        return sdf_out

    def predict_on_pandas(self, pdf: PandasLike, *, proba: bool = False):
        X = pdf[self.schema.features] if self.schema else pdf
        return self.predict(X, proba=proba)


# --------- Optional MLflow logging ---------
class MLflowLoggingMixin:
    """Drop-in mixin to log params/metrics/artifacts if MLflow is available."""

    def mlflow_log_all(
        self,
        *,
        run_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, float]] = None,
        artifacts_dir: Optional[str | Path] = None,
        artifact_subpath: str = "",
        experiment: Optional[str] = None,
    ) -> None:
        try:
            import mlflow
        except Exception:
            return  # MLflow not installed, silently skip

        if experiment:
            mlflow.set_experiment(experiment)
        with mlflow.start_run(run_name=run_name):
            if params:
                mlflow.log_params(params)
            if metrics:
                mlflow.log_metrics(metrics)
            if artifacts_dir:
                mlflow.log_artifacts(str(artifacts_dir), artifact_subpath)
