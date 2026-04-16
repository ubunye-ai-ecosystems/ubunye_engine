"""Titanic prediction task.

Loads the current registered model — production if present, otherwise the
staging candidate — from the Ubunye Model Registry and scores the raw
Titanic DataFrame. The Engine writes the predictions DataFrame to a
Unity Catalog Delta table per ``config.yaml``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict

# See train_classifier/transformations.py for the rationale — this self-heal
# can be removed once the engine ships the sys.path fix (0.1.7+).
_TASK_DIR = str(Path(__file__).resolve().parent)
if _TASK_DIR not in sys.path:
    sys.path.insert(0, _TASK_DIR)

from ubunye.core.interfaces import Task
from ubunye.models.registry import ModelRegistry, ModelStage

from model import TitanicSurvivalModel


MODEL_STORE_ENV = "TITANIC_MODEL_STORE"
USE_CASE = "titanic"
MODEL_NAME = "TitanicSurvivalModel"


class PredictTitanicClassifier(Task):
    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        store = os.environ.get(MODEL_STORE_ENV)
        if not store:
            raise RuntimeError(
                f"{MODEL_STORE_ENV} must be set so the registry can locate model artifacts."
            )

        registry = ModelRegistry(store)
        artifact_path, version = _resolve_active_version(registry)
        model = TitanicSurvivalModel.load(artifact_path)

        predictions = model.predict(sources["titanic"])

        spark = sources["titanic"].sparkSession
        from pyspark.sql import functions as F

        predictions = predictions.withColumn(
            "model_version", F.lit(version.version)
        ).withColumn("model_stage", F.lit(version.stage.value))

        return {"predictions": predictions}


def _resolve_active_version(registry: ModelRegistry):
    """Prefer production; fall back to staging so first-deploy predict works."""
    for stage in (ModelStage.PRODUCTION, ModelStage.STAGING):
        try:
            return registry.get_model(
                use_case=USE_CASE, model_name=MODEL_NAME, stage=stage
            )
        except ValueError:
            continue
    raise RuntimeError(
        f"No production or staging version of {MODEL_NAME} is available. "
        "Run the train job first."
    )
