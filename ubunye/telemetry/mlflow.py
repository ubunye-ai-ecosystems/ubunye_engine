"""Optional MLflow monitor integration for task-level logging."""

from __future__ import annotations

from typing import Any, Dict, Optional


class MLflowMonitor:
    """Log run params/metrics/artifacts to MLflow when available."""

    def __init__(
        self,
        *,
        experiment: Optional[str] = None,
        run_name: Optional[str] = None,
        params_path: str = "CONFIG",
        metrics_path: str = "CONFIG.monitoring.metrics",
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        self.experiment = experiment
        self.run_name = run_name
        self.params_path = params_path
        self.metrics_path = metrics_path
        self.tags = tags or {}

    def task_start(self, *, context: Any, config: dict) -> None:
        try:
            import mlflow
        except Exception:
            return

        if self.experiment:
            mlflow.set_experiment(self.experiment)
        mlflow.start_run(run_name=self.run_name or context.task_name)
        if self.tags:
            mlflow.set_tags(self.tags)

        params = _extract_path(config, self.params_path)
        if isinstance(params, dict):
            mlflow.log_params(_stringify(params))

    def task_end(
        self,
        *,
        context: Any,
        config: dict,
        outputs: Dict[str, Any] | None,
        status: str,
        duration_sec: float,
    ) -> None:
        try:
            import mlflow
        except Exception:
            return

        metrics = _extract_path(config, self.metrics_path) or {}
        if isinstance(metrics, dict):
            metrics_payload = dict(metrics)
            metrics_payload["duration_sec"] = duration_sec
            metrics_payload["status_success"] = 1.0 if status == "success" else 0.0
            mlflow.log_metrics(metrics_payload)
        mlflow.end_run()


def _extract_path(config: dict, path: str) -> Any:
    node: Any = config
    for part in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


def _stringify(values: Dict[str, Any]) -> Dict[str, str]:
    return {k: str(v) for k, v in values.items()}
