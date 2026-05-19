"""MLflow-aware registry backend.

Delegates artifact storage and lifecycle management to the filesystem
:class:`~ubunye.models.registry.ModelRegistry` and layers MLflow
experiment/run logging on top when MLflow is available.

When MLflow is not installed or not configured, all operations still
succeed — MLflow logging is best-effort and never blocks the pipeline.
This makes the backend usable in both Databricks (where MLflow is
pre-installed) and local/CI environments (where it may not be).

Discovered via the ``ubunye.registry_backends`` entry-point group as
``"mlflow"``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from ubunye.interfaces.registry import ModelVersionInfo
from ubunye.models.registry import FilesystemRegistryBackend

logger = logging.getLogger(__name__)


def _try_log_to_mlflow(
    *,
    use_case: str,
    model_name: str,
    version: str,
    metrics: Dict[str, Any],
    metadata: Optional[Dict[str, str]] = None,
    stage: Optional[str] = None,
) -> None:
    """Best-effort MLflow logging — never raises."""
    try:
        import mlflow

        experiment_name = f"/ubunye/{use_case}/{model_name}"
        mlflow.set_experiment(experiment_name)
        with mlflow.start_run(run_name=f"{model_name}-{version}"):
            mlflow.log_param("use_case", use_case)
            mlflow.log_param("model_name", model_name)
            mlflow.log_param("version", version)
            if stage:
                mlflow.log_param("stage", stage)
            for k, v in metrics.items():
                if isinstance(v, (int, float)):
                    mlflow.log_metric(k, v)
            if metadata:
                for k, v in metadata.items():
                    mlflow.log_param(f"meta_{k}", str(v)[:250])
    except ImportError:
        logger.debug("mlflow not installed — skipping MLflow logging")
    except Exception as exc:
        logger.warning("MLflow logging failed (non-fatal): %s", exc)


class MLflowRegistryBackend:
    """Registry backend that combines filesystem storage with MLflow logging.

    All model lifecycle operations (register, promote, demote, delete)
    are handled by the underlying ``FilesystemRegistryBackend``.  On
    ``register()`` and ``promote()``, the backend additionally logs
    metrics, params, and stage transitions to MLflow — but only when
    MLflow is installed and configured.  Logging failures are non-fatal
    and logged as warnings.
    """

    def __init__(self, store_path: str) -> None:
        self._fs = FilesystemRegistryBackend(store_path)

    def flush(self, timeout: Optional[float] = None) -> int:
        return self._fs.flush(timeout=timeout)

    def register(
        self,
        *,
        use_case: str,
        model_name: str,
        version: Optional[str],
        model: Any,
        metrics: Dict[str, Any],
        metadata: Optional[Dict[str, str]] = None,
        lineage_run_id: Optional[str] = None,
    ) -> ModelVersionInfo:
        info = self._fs.register(
            use_case=use_case,
            model_name=model_name,
            version=version,
            model=model,
            metrics=metrics,
            metadata=metadata,
            lineage_run_id=lineage_run_id,
        )
        _try_log_to_mlflow(
            use_case=use_case,
            model_name=model_name,
            version=info.version,
            metrics=metrics,
            metadata=metadata,
            stage=info.stage,
        )
        return info

    def get(
        self,
        *,
        use_case: str,
        model_name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> Tuple[str, ModelVersionInfo]:
        return self._fs.get(
            use_case=use_case,
            model_name=model_name,
            version=version,
            stage=stage,
        )

    def list_versions(
        self,
        *,
        use_case: str,
        model_name: str,
    ) -> List[ModelVersionInfo]:
        return self._fs.list_versions(use_case=use_case, model_name=model_name)

    def promote(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
        gates: Optional[Dict[str, Any]] = None,
        promoted_by: Optional[str] = None,
    ) -> ModelVersionInfo:
        info = self._fs.promote(
            use_case=use_case,
            model_name=model_name,
            version=version,
            to_stage=to_stage,
            gates=gates,
            promoted_by=promoted_by,
        )
        _try_log_to_mlflow(
            use_case=use_case,
            model_name=model_name,
            version=version,
            metrics={},
            stage=to_stage,
        )
        return info

    def demote(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
    ) -> ModelVersionInfo:
        return self._fs.demote(
            use_case=use_case,
            model_name=model_name,
            version=version,
            to_stage=to_stage,
        )

    def delete(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
    ) -> None:
        self._fs.delete(use_case=use_case, model_name=model_name, version=version)
