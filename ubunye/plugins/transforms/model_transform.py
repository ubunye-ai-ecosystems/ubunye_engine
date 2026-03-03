"""ModelTransform — Transform plugin for ML train/predict operations.

Registered as ``type: model`` in the transform entry points, so any pipeline
config with ``transform.type: model`` will dispatch here.

The transform supports two actions controlled by ``params.action``:

* ``train``: Instantiate the user's model, call ``train(df)``, register in
  the model registry (if configured), auto-promote if requested.
* ``predict``: Load a model from the registry (by stage) or from a direct
  path, call ``predict(df)``, return predictions.

Config example::

    transform:
      type: model
      params:
        action: train
        model_class: "model.FraudRiskModel"   # model.py relative to task_dir
        model_dir: null                        # optional; null = use sys.path
        registry:
          store: ".ubunye/model_store"
          use_case: "fraud_detection"
          auto_version: true
          promote_to: staging
          promotion_gates:
            min_auc: 0.85
            min_f1: 0.80

Note on model_dir:
  When called via ``ubunye run``, the task directory is added to ``sys.path``
  by ``_run_single_task`` before transforms run. This means ``model.py`` imports
  work automatically even when ``model_dir`` is not set.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from ubunye.core.interfaces import Backend, Transform
from ubunye.models.loader import load_model_class
from ubunye.models.registry import ModelRegistry, ModelStage


class ModelTransform(Transform):
    """Transform plugin for ML operations (train / predict).

    Implements :class:`ubunye.core.interfaces.Transform` so it plugs into the
    existing engine dispatch with zero changes to the core runtime.
    """

    def apply(
        self,
        inputs: Dict[str, Any],
        cfg: dict,
        backend: Backend,
    ) -> Dict[str, Any]:
        """Dispatch to train or predict based on ``cfg["action"]``.

        Args:
            inputs: Named DataFrames from the read phase.
            cfg: Transform params from ``config.yaml`` (the ``params`` sub-dict).
            backend: Active engine backend (provides Spark session if needed).

        Returns:
            Dict of named outputs. For ``train``: ``{"model_metrics": dict}``.
            For ``predict``: ``{"predictions": DataFrame}``.
        """
        action = cfg.get("action")
        if action == "train":
            return self._train(inputs, cfg, backend)
        elif action == "predict":
            return self._predict(inputs, cfg, backend)
        else:
            raise ValueError(
                f"Unknown model action: '{action}'. "
                f"Set params.action to 'train' or 'predict'."
            )

    # ------------------------------------------------------------------
    # Train
    # ------------------------------------------------------------------

    def _train(
        self, inputs: Dict[str, Any], cfg: dict, backend: Backend
    ) -> Dict[str, Any]:
        model_class = cfg.get("model_class")
        if not model_class:
            raise ValueError("params.model_class is required for action='train'.")

        model_dir: Optional[str] = cfg.get("model_dir")
        input_name: Optional[str] = cfg.get("input_name")

        df = _get_df(inputs, input_name)

        cls = load_model_class(model_dir, model_class)
        model = cls()
        metrics = model.train(df)

        registry_cfg = cfg.get("registry")
        if registry_cfg:
            use_case = registry_cfg.get("use_case", "default")
            store = registry_cfg["store"]
            version = registry_cfg.get("version")  # None → auto-generate
            lineage_run_id = _get_run_id(backend)

            registry = ModelRegistry(store)
            model_version = registry.register(
                use_case=use_case,
                model_name=model_class.rsplit(".", 1)[-1],
                version=version,
                model=model,
                metrics=metrics,
                lineage_run_id=lineage_run_id,
            )

            promote_to = registry_cfg.get("promote_to")
            if promote_to:
                gates = registry_cfg.get("promotion_gates")
                registry.promote(
                    use_case=use_case,
                    model_name=model_class.rsplit(".", 1)[-1],
                    version=model_version.version,
                    to_stage=ModelStage(promote_to),
                    gates=gates,
                )

        return {"model_metrics": metrics}

    # ------------------------------------------------------------------
    # Predict
    # ------------------------------------------------------------------

    def _predict(
        self, inputs: Dict[str, Any], cfg: dict, backend: Backend
    ) -> Dict[str, Any]:
        model_class = cfg.get("model_class")
        if not model_class:
            raise ValueError("params.model_class is required for action='predict'.")

        model_dir: Optional[str] = cfg.get("model_dir")
        input_name: Optional[str] = cfg.get("input_name")
        df = _get_df(inputs, input_name)

        cls = load_model_class(model_dir, model_class)

        registry_cfg = cfg.get("registry")
        if registry_cfg:
            use_case = registry_cfg.get("use_case", "default")
            store = registry_cfg["store"]
            use_stage = ModelStage(registry_cfg.get("use_stage", "production"))
            model_name = model_class.rsplit(".", 1)[-1]

            registry = ModelRegistry(store)
            artifact_path, _ = registry.get_model(
                use_case=use_case,
                model_name=model_name,
                stage=use_stage,
            )
            model = cls.load(artifact_path)
        else:
            model_path = cfg.get("model_path")
            if not model_path:
                raise ValueError(
                    "For action='predict' without a registry config, "
                    "params.model_path must be set."
                )
            model = cls.load(model_path)

        predictions = model.predict(df)
        return {"predictions": predictions}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_df(inputs: Dict[str, Any], input_name: Optional[str]) -> Any:
    if not inputs:
        raise ValueError("ModelTransform received no inputs.")
    if input_name:
        if input_name not in inputs:
            raise KeyError(
                f"Requested input '{input_name}' not found. "
                f"Available: {list(inputs)}"
            )
        return inputs[input_name]
    return next(iter(inputs.values()))


def _get_run_id(backend: Backend) -> Optional[str]:
    """Best-effort: extract run_id from backend context if available."""
    return getattr(backend, "run_id", None)
