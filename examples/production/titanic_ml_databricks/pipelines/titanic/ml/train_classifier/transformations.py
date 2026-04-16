"""Titanic training task.

Responsibilities:

1. Fit ``TitanicSurvivalModel`` on the raw Titanic DataFrame.
2. Log the run to MLflow (params + metrics + artifact). On Databricks this
   targets the workspace tracking server automatically; locally it writes
   under ``./mlruns`` unless ``MLFLOW_TRACKING_URI`` is set.
3. Register the trained model in the Ubunye Model Registry, using a UC
   volume as the filesystem store on Databricks.
4. Promote to ``staging`` iff a minimum-AUC gate passes.
5. Emit a one-row ``training_metrics`` Spark DataFrame so the Engine's
   writer can persist it as an audit row to Unity Catalog.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

# The engine puts task_dir on sys.path before transforms run, but ubunye
# <= 0.1.6 does so *after* loading transformations.py, so a top-level
# ``from model import ...`` raises at import time. Self-heal here.
_TASK_DIR = str(Path(__file__).resolve().parent)
if _TASK_DIR not in sys.path:
    sys.path.insert(0, _TASK_DIR)

from ubunye.core.interfaces import Task
from ubunye.models.registry import ModelRegistry, ModelStage

from model import TitanicSurvivalModel


MIN_AUC = float(os.environ.get("TITANIC_MIN_AUC", "0.80"))
MODEL_STORE_ENV = "TITANIC_MODEL_STORE"
USE_CASE = "titanic"
MODEL_NAME = "TitanicSurvivalModel"


class TrainTitanicClassifier(Task):
    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        titanic_df = sources["titanic"]

        model = TitanicSurvivalModel()
        metrics = model.train(titanic_df)
        _mlflow_log(model, metrics)

        store = os.environ.get(MODEL_STORE_ENV)
        if not store:
            raise RuntimeError(
                f"{MODEL_STORE_ENV} must be set so the registry has a writable store path."
            )

        registry = ModelRegistry(store)
        version = registry.register(
            use_case=USE_CASE,
            model_name=MODEL_NAME,
            version=None,  # auto-bump semver
            model=model,
            metrics=metrics,
        )

        promoted_to: str = "none"
        promotion_error: str = ""
        try:
            registry.promote(
                use_case=USE_CASE,
                model_name=MODEL_NAME,
                version=version.version,
                to_stage=ModelStage.STAGING,
                gates={"min_auc": MIN_AUC},
            )
            promoted_to = "staging"
        except ValueError as exc:
            promotion_error = str(exc).splitlines()[0]

        return {
            "training_metrics": _metrics_row(
                sources, metrics, version.version, promoted_to, promotion_error
            )
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mlflow_log(model: TitanicSurvivalModel, metrics: Dict[str, float]) -> None:
    try:
        import mlflow
    except ImportError:
        return

    experiment = os.environ.get("MLFLOW_EXPERIMENT_NAME")
    if experiment:
        mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=f"titanic-train-{datetime.utcnow():%Y%m%dT%H%M%S}"):
        mlflow.log_params(model.metadata()["params"])
        mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, (int, float))})


def _metrics_row(
    sources: Dict[str, Any],
    metrics: Dict[str, float],
    version: str,
    promoted_to: str,
    promotion_error: str,
):
    spark = sources["titanic"].sparkSession
    row = {
        "model_version": version,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "accuracy": float(metrics.get("accuracy", 0.0)),
        "auc": float(metrics.get("auc", 0.0)),
        "f1": float(metrics.get("f1", 0.0)),
        "rows": int(metrics.get("rows", 0)),
        "promoted_to": promoted_to,
        "promotion_error": promotion_error,
    }
    return spark.createDataFrame([row])
