"""UbunyeModel — library-independent abstract contract for user-defined models.

This is the user-facing base class. Users subclass UbunyeModel and implement the
five abstract methods. The engine never imports sklearn, PyTorch, XGBoost, or any
other ML library — it only calls these methods through this contract.

See also:
    ubunye.plugins.ml.base.BaseModel — the internal base class for ubunye's own
    sklearn/spark wrappers (not user-facing; uses _fit_core/_predict_core hooks).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict


class UbunyeModel(ABC):
    """Abstract contract every user model must implement.

    The engine calls these methods during the pipeline lifecycle. Users implement
    them with whatever ML library they choose — the engine never imports or knows
    about the underlying library.

    Minimal example::

        from ubunye.models.base import UbunyeModel
        import joblib

        class FraudRiskModel(UbunyeModel):
            def __init__(self):
                self._clf = None

            def train(self, df):
                import pandas as pd
                from sklearn.ensemble import GradientBoostingClassifier
                pdf = df.toPandas()
                X, y = pdf.drop("label", axis=1), pdf["label"]
                self._clf = GradientBoostingClassifier().fit(X, y)
                return {"accuracy": self._clf.score(X, y)}

            def predict(self, df):
                pdf = df.toPandas()
                pdf["score"] = self._clf.predict_proba(pdf)[:, 1]
                return spark.createDataFrame(pdf)

            def save(self, path):
                import pathlib, joblib
                pathlib.Path(path).mkdir(parents=True, exist_ok=True)
                joblib.dump(self._clf, f"{path}/clf.joblib")

            @classmethod
            def load(cls, path):
                import joblib
                m = cls()
                m._clf = joblib.load(f"{path}/clf.joblib")
                return m

            def metadata(self):
                import sklearn
                return {
                    "library": "sklearn",
                    "library_version": sklearn.__version__,
                    "features": list(self._clf.feature_names_in_),
                    "params": self._clf.get_params(),
                }
    """

    @abstractmethod
    def train(self, df: Any) -> Dict[str, Any]:
        """Train the model on the provided DataFrame.

        Args:
            df: Spark DataFrame (or any DataFrame-like) with training data.

        Returns:
            Dict of metrics e.g. ``{"auc": 0.94, "f1": 0.87, "accuracy": 0.91}``.
            These metrics are stored in the registry alongside the model version.
        """

    @abstractmethod
    def predict(self, df: Any) -> Any:
        """Generate predictions on the provided DataFrame.

        Args:
            df: Spark DataFrame with feature columns.

        Returns:
            Spark DataFrame (or DataFrame-like) with prediction columns added.
        """

    @abstractmethod
    def save(self, path: str) -> None:
        """Serialize the model to the given directory path.

        The user chooses the serialization format (pickle, joblib, ONNX, etc.).
        The engine only provides the path — it never inspects the saved files.

        Args:
            path: Directory path where model files should be written.
        """

    @classmethod
    @abstractmethod
    def load(cls, path: str) -> "UbunyeModel":
        """Deserialize a model from the given directory path.

        Args:
            path: Directory path where model files were saved.

        Returns:
            An instance of the model ready for prediction.
        """

    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """Return model metadata for the registry.

        Should include at minimum:
        - ``library``: str — e.g. ``"xgboost"``, ``"sklearn"``, ``"pytorch"``
        - ``library_version``: str — e.g. ``"2.0.3"``
        - ``features``: List[str] — feature names used during training
        - ``params``: dict — model hyperparameters

        Additional keys are allowed and will be stored in the registry.
        """

    def validate(self, df: Any) -> Dict[str, Any]:
        """Optional: validate model on holdout data.

        Returns:
            Metrics dict in the same format as :meth:`train`.

        Raises:
            NotImplementedError: if the subclass has not overridden this method.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement validate(). "
            "Override this method to add holdout validation."
        )
