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
          backend: filesystem                  # optional; see ADR 010
          use_case: "fraud_detection"
          promote_to: staging
          promotion_gates:
            min_auc: 0.85
            min_f1: 0.80
          on_blocked: raise                    # or record (ADR 012)
          # predict only:
          stamp_columns: false                 # true adds model_version, model_stage
          check_features: null                 # true refuses, false skips (ADR 012)

Every train and predict is put on the run record (``model_runs``, ADR 012).

Note on model_dir:
  When called via ``ubunye run``, the task directory is added to ``sys.path``
  by ``_run_single_task`` before transforms run. This means ``model.py`` imports
  work automatically even when ``model_dir`` is not set.
"""

from __future__ import annotations

import logging
import warnings
from typing import Any, Dict, List, Optional

from ubunye.core.errors import FeatureContractError, PromotionBlockedError, UbunyeError
from ubunye.core.interfaces import Backend, Transform
from ubunye.models import receipt
from ubunye.models.gates import PromotionGate
from ubunye.models.loader import load_model_class
from ubunye.models.registry import ModelRegistry, ModelStage

logger = logging.getLogger(__name__)

#: The columns ``registry.stamp_columns: true`` adds to scored rows.
STAMP_COLUMNS = ("model_version", "model_stage")


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
        # The engine hands over the whole transform block ({type, params});
        # a direct call may pass the params alone. Both mean the same.
        if "action" not in cfg and isinstance(cfg.get("params"), dict):
            cfg = cfg["params"]
        action = cfg.get("action")
        if action == "train":
            return self._train(inputs, cfg, backend)
        elif action == "predict":
            return self._predict(inputs, cfg, backend)
        else:
            raise UbunyeError(
                f"Unknown model action: '{action}'.",
                context={"action": action},
                hint="Set params.action to 'train' or 'predict'.",
            )

    # ------------------------------------------------------------------
    # Train
    # ------------------------------------------------------------------

    def _train(self, inputs: Dict[str, Any], cfg: dict, backend: Backend) -> Dict[str, Any]:
        model_class = cfg.get("model_class")
        if not model_class:
            raise ValueError("params.model_class is required for action='train'.")

        model_dir: Optional[str] = cfg.get("model_dir")
        input_name: Optional[str] = cfg.get("input_name")

        df = _get_df(inputs, input_name)

        cls = load_model_class(model_dir, model_class)
        model = cls()
        metrics = model.train(df)
        model_name = model_class.rsplit(".", 1)[-1]

        # What the run record says about this model (ADR 012).
        entry: Dict[str, Any] = {
            "action": "train",
            "model": model_name,
            "model_class": model_class,
            "use_case": None,
            "registered": False,
            "version": None,
            "stage": None,
            "promote_to": None,
            "gate_result": None,
            "gates": [],
            "blocked_by": [],
            "metrics": receipt.plain(metrics or {}),
        }
        receipt.record(entry)

        registry_cfg = cfg.get("registry")
        if registry_cfg:
            if "auto_version" in registry_cfg:
                warnings.warn(
                    "registry.auto_version does nothing: a version is made whenever "
                    "registry.version is not set. Remove the key; it is refused from 1.0.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            use_case = registry_cfg.get("use_case", "default")
            store = registry_cfg["store"]
            version = registry_cfg.get("version")  # None → auto-generate
            lineage_run_id = _get_run_id(backend)

            # backend: the config key, else UBUNYE_REGISTRY_BACKEND, else filesystem.
            registry = ModelRegistry(store, backend=registry_cfg.get("backend"))
            model_version = registry.register(
                use_case=use_case,
                model_name=model_name,
                version=version,
                model=model,
                metrics=metrics,
                lineage_run_id=lineage_run_id,
                # Stored with the model, so a later promotion from the CLI checks
                # the same gates this run would have.
                promotion_gates=registry_cfg.get("promotion_gates"),
            )
            entry.update(
                use_case=use_case,
                registered=True,
                version=model_version.version,
                stage=_stage(model_version.stage),
            )

            promote_to = registry_cfg.get("promote_to")
            if promote_to:
                gates = registry_cfg.get("promotion_gates")
                entry["promote_to"] = promote_to
                self._promote(registry, registry_cfg, entry, model_version, gates)

        return {"model_metrics": metrics}

    @staticmethod
    def _promote(
        registry: ModelRegistry,
        registry_cfg: dict,
        entry: Dict[str, Any],
        model_version: Any,
        gates: Optional[Dict[str, Any]],
    ) -> None:
        """Promote, and put the gate result on the record whether it passes or not."""
        on_blocked = registry_cfg.get("on_blocked") or "raise"
        if on_blocked not in ("raise", "record"):
            raise UbunyeError(
                f"Unknown registry.on_blocked: '{on_blocked}'.",
                context={"on_blocked": on_blocked},
                hint="Use 'raise' (the default: a blocked promotion fails the run) or "
                "'record' (the run goes on and the record says it was blocked).",
            )
        use_case, model_name = entry["use_case"], entry["model"]
        # The same gates promote() checks: the ones given, else those stored.
        if gates is None:
            try:
                gates = registry.promotion_gates(use_case, model_name)
            except Exception:  # noqa: BLE001 - only for the record; promote() decides
                gates = None
        checks = (
            receipt.gate_checks(
                PromotionGate(gates).evaluate(model_version.metrics, model_version.metadata)
            )
            if gates
            else []
        )
        entry["gates"] = checks
        try:
            promoted = registry.promote(
                use_case=use_case,
                model_name=model_name,
                version=model_version.version,
                to_stage=ModelStage(entry["promote_to"]),
                gates=gates,
            )
        except PromotionBlockedError as exc:
            entry["gate_result"] = "blocked"
            entry["blocked_by"] = receipt.blocked_by(checks)
            entry["error"] = str(exc)
            if on_blocked == "raise":
                raise
            logger.warning(
                "Model %s %s was not promoted to %s: %s",
                model_name,
                model_version.version,
                entry["promote_to"],
                "; ".join(c["message"] for c in checks if not c["passed"]) or exc,
            )
            return
        entry["stage"] = _stage(promoted.stage)
        entry["gate_result"] = "passed" if checks else "no gates"

    # ------------------------------------------------------------------
    # Predict
    # ------------------------------------------------------------------

    def _predict(self, inputs: Dict[str, Any], cfg: dict, backend: Backend) -> Dict[str, Any]:
        model_class = cfg.get("model_class")
        if not model_class:
            raise ValueError("params.model_class is required for action='predict'.")

        model_dir: Optional[str] = cfg.get("model_dir")
        input_name: Optional[str] = cfg.get("input_name")
        df = _get_df(inputs, input_name)

        cls = load_model_class(model_dir, model_class)
        model_name = model_class.rsplit(".", 1)[-1]
        entry: Dict[str, Any] = {
            "action": "predict",
            "model": model_name,
            "model_class": model_class,
            "use_case": None,
            "version": None,
            "stage": None,
            "model_path": None,
            "features": None,
        }

        registry_cfg = cfg.get("registry")
        declared: List[str] = []
        if registry_cfg:
            use_case = registry_cfg.get("use_case", "default")
            store = registry_cfg["store"]
            use_stage = ModelStage(registry_cfg.get("use_stage", "production"))

            # backend: the config key, else UBUNYE_REGISTRY_BACKEND, else filesystem.
            registry = ModelRegistry(store, backend=registry_cfg.get("backend"))
            artifact_path, version_info = registry.get_model(
                use_case=use_case,
                model_name=model_name,
                stage=use_stage,
            )
            entry.update(
                use_case=use_case,
                version=version_info.version,
                stage=_stage(version_info.stage),
            )
            declared = _declared_features(version_info.metadata)
            model = cls.load(artifact_path)
        else:
            model_path = cfg.get("model_path")
            if not model_path:
                raise ValueError(
                    "For action='predict' without a registry config, "
                    "params.model_path must be set."
                )
            entry["model_path"] = model_path
            model = cls.load(model_path)
        receipt.record(entry)

        check = (registry_cfg or {}).get("check_features")
        missing: List[str] = []
        if declared and check is not False:
            missing = _missing(df, declared)
            entry["features"] = {
                "declared": declared,
                "missing": missing,
                "check": "refuse" if check else "warn",
            }
            if missing and check:
                raise FeatureContractError(
                    f"The frame given to {model_name} {entry['version']} lacks "
                    f"{len(missing)} of the features the model declares.",
                    context={
                        "Model": f"{entry['use_case']}/{model_name}",
                        "Version": f"{entry['version']} ({entry['stage']})",
                        "Missing": ", ".join(missing),
                        "Declared": ", ".join(declared),
                    },
                    hint="Add the missing columns to the input (check the column names "
                    "and the input_name), or score with the version trained on these "
                    "columns. If the model builds these columns itself in predict(), "
                    "set registry.check_features: false.",
                )
            if missing:
                logger.warning(
                    "The frame given to %s %s lacks declared features: %s. Calling the "
                    "model anyway (registry.check_features: true refuses such a frame).",
                    model_name,
                    entry["version"],
                    ", ".join(missing),
                )

        try:
            predictions = model.predict(df)
        except Exception as exc:
            if missing:
                note = (
                    f"Ubunye: the frame given to {model_name} {entry['version']} lacked "
                    f"declared features: {', '.join(missing)}."
                )
                add_note = getattr(exc, "add_note", None)  # Python 3.11+
                if add_note is not None:
                    add_note(note)
                else:
                    # Python 3.10: keep the same __notes__ list callers can read,
                    # and log it, since 3.10 tracebacks do not print notes.
                    try:
                        exc.__notes__ = [*getattr(exc, "__notes__", []), note]  # type: ignore[attr-defined]
                    except Exception:  # noqa: BLE001 (an exception that refuses attributes)
                        pass
                    logger.error(note)
            raise

        if registry_cfg and registry_cfg.get("stamp_columns"):
            predictions = _stamp(predictions, entry["version"], entry["stage"])
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
                f"Requested input '{input_name}' not found. " f"Available: {list(inputs)}"
            )
        return inputs[input_name]
    return next(iter(inputs.values()))


def _get_run_id(backend: Backend) -> Optional[str]:
    """Best-effort: extract run_id from backend context if available."""
    return getattr(backend, "run_id", None)


def _stage(stage: Any) -> str:
    return stage.value if isinstance(stage, ModelStage) else str(stage)


def _declared_features(metadata: Any) -> List[str]:
    """The feature names a registered version declares (``metadata["features"]``)."""
    feats = (metadata or {}).get("features") if isinstance(metadata, dict) else None
    if isinstance(feats, str) or not feats:
        return []
    try:
        return [str(f) for f in feats]
    except TypeError:
        return []


def _missing(df: Any, declared: List[str]) -> List[str]:
    """Declared features the frame lacks; a frame with no column list lacks none."""
    try:
        columns = set(str(c) for c in df.columns)
    except (AttributeError, TypeError):
        return []
    return [f for f in declared if f not in columns]


def _stamp(predictions: Any, version: Any, stage: Any) -> Any:
    """Add the model version and stage as columns, on pandas and Spark alike."""
    try:
        existing = [str(c) for c in predictions.columns]
    except AttributeError as exc:
        raise UbunyeError(
            "registry.stamp_columns needs predict() to return a DataFrame.",
            context={"Returned": type(predictions).__name__},
            hint="Return a pandas or Spark DataFrame from predict(), or turn "
            "registry.stamp_columns off.",
        ) from exc
    clash = [c for c in STAMP_COLUMNS if c in existing]
    if clash:
        raise UbunyeError(
            f"registry.stamp_columns would overwrite column(s) {', '.join(clash)} in the "
            "predictions.",
            context={"Columns": ", ".join(clash)},
            hint="Rename or drop those columns in predict(), or turn "
            "registry.stamp_columns off.",
        )
    values = dict(zip(STAMP_COLUMNS, (str(version), str(stage))))
    if hasattr(predictions, "withColumn"):  # Spark
        from pyspark.sql import functions as F

        for name, value in values.items():
            predictions = predictions.withColumn(name, F.lit(value))
        return predictions
    return predictions.assign(**values)  # pandas, and anything with assign()
