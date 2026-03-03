"""
Spark MLlib wrapper.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from pyspark.ml import Estimator, PipelineModel
from pyspark.sql import DataFrame as SparkDF

from .base import BaseModel, FeatureSchema


class SparkMLModel(BaseModel):
    """Wrap a Spark ML Estimator or loaded PipelineModel."""

    def __init__(
        self,
        estimator: Optional[Estimator] = None,
        *,
        schema: Optional[FeatureSchema] = None,
        **kwargs,
    ):
        super().__init__(schema=schema, **kwargs)
        self.estimator = estimator
        self.pipeline_model: Optional[PipelineModel] = None

    def _fit_core(self, X: SparkDF, y: Optional[Any]) -> None:
        if self.estimator is None:
            raise ValueError("SparkMLModel requires an Estimator to fit.")
        self.pipeline_model = self.estimator.fit(X)

    def _predict_core(self, X: SparkDF, proba: bool = False) -> SparkDF:
        if self.pipeline_model is None:
            raise RuntimeError("Model not fitted or loaded.")
        # Spark model returns a DataFrame with prediction columns
        return self.pipeline_model.transform(X)

    def _save_core(self, path: Path) -> None:
        if self.pipeline_model is None:
            raise RuntimeError("Nothing to save; fit or load first.")
        self.pipeline_model.write().overwrite().save(str(path / "spark_pipeline_model"))

    def _load_core(self, path: Path) -> None:
        self.pipeline_model = PipelineModel.load(str(path / "spark_pipeline_model"))
