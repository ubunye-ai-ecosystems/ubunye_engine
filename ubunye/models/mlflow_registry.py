"""MLflow registry backends.

``MLflowModelRegistryBackend`` (the ``mlflow`` entry point, ADR 014) keeps every
version in the MLflow Model Registry: a registered model version, its stage as an
alias, everything else as version tags. It never calls the MLflow stage API, so the
same code works where only aliases exist (Unity Catalog).

``MLflowRegistryBackend`` is the older backend, deprecated in 0.9. It delegates artifact storage and lifecycle management to the filesystem
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

import json
import logging
import os
import tempfile
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from ubunye.core.errors import (
    PromotionBlockedError,
    RegistryNotFoundError,
    VersionExistsError,
    VersionNotFoundError,
)
from ubunye.interfaces.registry import ModelVersionInfo
from ubunye.models.lifecycle import check_demote
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
    """Deprecated: filesystem storage with MLflow run logging on top.

    Use :class:`MLflowModelRegistryBackend`. This class still works as before.

    All model lifecycle operations (register, promote, demote, delete)
    are handled by the underlying ``FilesystemRegistryBackend``.  On
    ``register()`` and ``promote()``, the backend additionally logs
    metrics, params, and stage transitions to MLflow — but only when
    MLflow is installed and configured.  Logging failures are non-fatal
    and logged as warnings.
    """

    def __init__(self, store_path: str) -> None:
        warnings.warn(
            "MLflowRegistryBackend keeps models in a folder and only logs runs to MLflow. "
            "It is deprecated since 0.9. Use MLflowModelRegistryBackend (--backend mlflow), "
            "which keeps them in the MLflow Model Registry.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._fs = FilesystemRegistryBackend(store_path)

    def flush(self, timeout: Optional[float] = None) -> int:
        return self._fs.flush(timeout=timeout)

    def promotion_gates(self, use_case: str, model_name: str) -> Dict[str, Any]:
        """The gates stored with a model, kept by the filesystem it builds on."""
        return self._fs.promotion_gates(use_case, model_name)

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
        force: bool = False,
    ) -> ModelVersionInfo:
        info = self._fs.promote(
            use_case=use_case,
            model_name=model_name,
            version=version,
            to_stage=to_stage,
            gates=gates,
            force=force,
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


# ---------------------------------------------------------------------------
# The MLflow Model Registry backend (ADR 014)
# ---------------------------------------------------------------------------

#: How a use case and model name become an MLflow registered model name.
NAME_ENV_VAR = "UBUNYE_MLFLOW_MODEL_NAME"
DEFAULT_NAME_TEMPLATE = "{use_case}.{model_name}"

#: The experiment that holds the runs carrying model artifacts, when the store is empty.
DEFAULT_EXPERIMENT = "ubunye-models"

_TAG = "ubunye."
_VERSION_TAG = _TAG + "version"
_STAGE_TAG = _TAG + "stage"
_ENGINE_TAG = _TAG + "engine_version"
_METRIC = _TAG + "metric."
_META = _TAG + "meta."

_STAGES = ("development", "staging", "production", "archived")
#: Stages one version holds at a time are MLflow aliases. The others are only tags.
_ALIASES = {"staging": "staging", "production": "production"}
#: The metadata key that says when a version entered a stage.
_STAGE_TIME = {
    "staging": "promoted_to_staging",
    "production": "promoted_to_prod",
    "archived": "archived_at",
}


def _text(value: Any) -> str:
    """A tag value: strings as they are, everything else as JSON."""
    return value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)


def _is_missing(exc: Exception) -> bool:
    return getattr(exc, "error_code", "") == "RESOURCE_DOES_NOT_EXIST"


class MLflowModelRegistryBackend:
    """A registry backend on the MLflow Model Registry (ADR 014).

    - Where MLflow lives is MLflow's own business: ``MLFLOW_TRACKING_URI`` and
      ``MLFLOW_REGISTRY_URI`` (or ``mlflow.set_tracking_uri``) as MLflow reads them.
    - The store string is the MLflow experiment that holds the runs carrying the
      model artifacts (for example ``/Shared/ubunye/models`` on Databricks).
    - A Ubunye model is the registered model ``{use_case}.{model_name}``. Set
      ``UBUNYE_MLFLOW_MODEL_NAME`` to another template (Unity Catalog wants
      ``catalog.schema.{model_name}``).
    - A Ubunye version is a registered model version. The Ubunye version string,
      stage, metrics and metadata (gates, lineage run id) are version tags.
    - ``staging`` and ``production`` are also MLflow aliases, so
      ``models:/name@production`` loads the production version anywhere. The MLflow
      stage API is never called: it is deprecated, and Unity Catalog has none.

    Rollback, archive, compare and one-production-at-a-time come from
    :mod:`ubunye.models.lifecycle`, as for every backend.
    """

    def __init__(self, store: str) -> None:
        try:
            from mlflow import MlflowClient
        except ImportError as exc:  # pragma: no cover - mlflow is in the [ml] extra
            raise ImportError(
                "The 'mlflow' registry backend needs MLflow. "
                "Try: pip install 'ubunye-engine[ml]'"
            ) from exc
        self.experiment = store or DEFAULT_EXPERIMENT
        self.client = MlflowClient()

    # -- naming and reading ----------------------------------------------------

    def model_name(self, use_case: str, model_name: str) -> str:
        """The MLflow registered model name for a Ubunye model."""
        template = os.environ.get(NAME_ENV_VAR, "").strip() or DEFAULT_NAME_TEMPLATE
        try:
            return template.format(use_case=use_case, model_name=model_name)
        except (KeyError, IndexError, ValueError) as exc:
            raise ValueError(
                f"{NAME_ENV_VAR}='{template}' is not a valid model name template ({exc}). "
                "Use only {use_case} and {model_name}, e.g. 'main.ml.{model_name}'."
            ) from exc

    def _aliases(self, name: str) -> Dict[str, str]:
        try:
            model = self.client.get_registered_model(name)
        except Exception as exc:
            if _is_missing(exc):
                raise RegistryNotFoundError(
                    f"No registered model '{name}' in the MLflow Model Registry.",
                    context={"Model": name, "Registry": self.client._registry_uri},
                    hint="Register a model version first, or check MLFLOW_TRACKING_URI, "
                    f"MLFLOW_REGISTRY_URI and {NAME_ENV_VAR}.",
                ) from exc
            raise
        return {str(k): str(v) for k, v in (model.aliases or {}).items()}

    def _versions(self, name: str) -> List[Any]:
        """Ubunye's versions of a registered model, newest first."""
        self._aliases(name)  # a clear error when the model does not exist
        found: List[Any] = []
        token = None
        while True:
            page = self.client.search_model_versions(f"name='{name}'", page_token=token)
            found.extend(mv for mv in page if _VERSION_TAG in (mv.tags or {}))
            token = getattr(page, "token", None)
            if not token:
                break
        return sorted(found, key=lambda mv: int(mv.version), reverse=True)

    def _find(self, name: str, version: str) -> Any:
        for mv in self._versions(name):
            if mv.tags[_VERSION_TAG] == version:
                return mv
        raise VersionNotFoundError(
            f"Version '{version}' of '{name}' is not in the MLflow Model Registry.",
            context={"Model": name, "Version": version},
            hint="List the versions with 'ubunye models list --backend mlflow'.",
        )

    def _info(self, name: str, mv: Any) -> ModelVersionInfo:
        tags = dict(mv.tags or {})
        metrics = {
            k[len(_METRIC) :]: json.loads(v) for k, v in tags.items() if k.startswith(_METRIC)
        }
        metadata = {k[len(_META) :]: v for k, v in tags.items() if k.startswith(_META)}
        metadata["mlflow_model"] = name
        metadata["mlflow_version"] = str(mv.version)
        created = datetime.fromtimestamp(int(mv.creation_timestamp or 0) / 1000, tz=timezone.utc)
        return ModelVersionInfo(
            name=name,
            version=tags[_VERSION_TAG],
            stage=tags.get(_STAGE_TAG, "development"),
            created_at=created.isoformat(),
            engine_version=tags.get(_ENGINE_TAG, "unknown"),
            artifact_path=mv.source,
            metrics=metrics,
            metadata=metadata,
        )

    def _local_path(self, name: str, mv: Any) -> str:
        """A local folder holding the artifact: the store itself when it is local."""
        import mlflow
        from mlflow.utils.file_utils import local_file_uri_to_path

        uri = self.client.get_model_version_download_uri(name, mv.version)
        if urlparse(uri).scheme in ("", "file"):
            path = local_file_uri_to_path(uri) if uri.startswith("file:") else unquote(uri)
            if os.path.isdir(path):
                return path
        return str(
            mlflow.artifacts.download_artifacts(
                artifact_uri=uri, dst_path=tempfile.mkdtemp(prefix="ubunye-mlflow-")
            )
        )

    # -- the six protocol methods ------------------------------------------------

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
        from ubunye.models.lifecycle import next_version
        from ubunye.models.registry import _engine_version

        name = self.model_name(use_case, model_name)
        try:
            existing = [mv.tags[_VERSION_TAG] for mv in self._versions(name)]
        except RegistryNotFoundError:
            self.client.create_registered_model(name)
            existing = []
        if version is None:
            version = next_version(existing)
        if version in existing:
            raise VersionExistsError(
                f"Version '{version}' already exists for {name}.",
                context={"Version": version, "Model": name},
                hint="Use a different version string or leave it out to number it.",
            )

        run_id, source = self._log_artifact(use_case, model_name, version, model, metrics)

        meta: Dict[str, str] = {k: _text(v) for k, v in (model.metadata() or {}).items()}
        meta.update({k: _text(v) for k, v in (metadata or {}).items()})
        if lineage_run_id:
            meta["lineage_run_id"] = lineage_run_id
        tags = {
            _VERSION_TAG: version,
            _STAGE_TAG: "development",
            _ENGINE_TAG: _engine_version(),
            **{_METRIC + k: json.dumps(v, default=str) for k, v in metrics.items()},
            **{_META + k: v for k, v in meta.items()},
        }
        mv = self.client.create_model_version(name, source=source, run_id=run_id, tags=tags)
        return self._info(name, self._wait_ready(name, mv))

    def get(
        self,
        *,
        use_case: str,
        model_name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> Tuple[str, ModelVersionInfo]:
        name = self.model_name(use_case, model_name)
        if version is not None:
            mv = self._find(name, version)
        elif stage is not None:
            mv = self._in_stage(name, stage)
        else:
            raise VersionNotFoundError(
                "Specify either 'version' or 'stage'.",
                hint="Pass version='x.y.z' or stage='production'.",
            )
        return self._local_path(name, mv), self._info(name, mv)

    def list_versions(self, *, use_case: str, model_name: str) -> List[ModelVersionInfo]:
        name = self.model_name(use_case, model_name)
        return [self._info(name, mv) for mv in self._versions(name)]

    def promote(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
        gates: Optional[Dict[str, Any]] = None,
        promoted_by: Optional[str] = None,
        force: bool = False,
    ) -> ModelVersionInfo:
        name = self.model_name(use_case, model_name)
        mv = self._find(name, version)
        extra: Dict[str, str] = {}
        if gates and force:
            extra = {"promotion_forced": "true", "promotion_gates_skipped": _text(dict(gates))}
        elif gates:
            self._check_gates(name, mv, gates, to_stage)
        if promoted_by:
            extra["promoted_by"] = promoted_by
        return self._set_stage(name, mv, to_stage, extra)

    def demote(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
    ) -> ModelVersionInfo:
        name = self.model_name(use_case, model_name)
        mv = self._find(name, version)
        check_demote(version, (mv.tags or {}).get(_STAGE_TAG, "development"), to_stage)
        return self._set_stage(name, mv, to_stage, {})

    def delete(self, *, use_case: str, model_name: str, version: str) -> None:
        name = self.model_name(use_case, model_name)
        mv = self._find(name, version)
        stage = (mv.tags or {}).get(_STAGE_TAG, "development")
        self.client.delete_model_version(name, mv.version)
        self._sync_alias(name, stage)

    # -- helpers ---------------------------------------------------------------

    def _log_artifact(
        self, use_case: str, model_name: str, version: str, model: Any, metrics: Dict[str, Any]
    ) -> Tuple[str, str]:
        """Log the saved model as the artifact of a new MLflow run; its id and URI."""
        experiment = self.client.get_experiment_by_name(self.experiment)
        experiment_id = (
            experiment.experiment_id
            if experiment is not None
            else self.client.create_experiment(self.experiment)
        )
        run = self.client.create_run(
            experiment_id,
            run_name=f"{model_name}-{version}",
            tags={
                _TAG + "use_case": use_case,
                _TAG + "model_name": model_name,
                _VERSION_TAG: version,
            },
        )
        run_id = run.info.run_id
        try:
            with tempfile.TemporaryDirectory(prefix="ubunye-mlflow-") as tmp:
                local = Path(tmp) / "model"
                model.save(str(local))
                self.client.log_artifacts(run_id, str(local), "model")
            for key, value in metrics.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    self.client.log_metric(run_id, key, float(value))
        except BaseException:
            self.client.set_terminated(run_id, status="FAILED")
            raise
        self.client.set_terminated(run_id)
        return run_id, f"{run.info.artifact_uri}/model"

    def _wait_ready(self, name: str, mv: Any, timeout: float = 600.0) -> Any:
        """Some registries copy the artifact after creating the version; wait for it."""
        import time

        deadline = time.monotonic() + timeout
        while str(getattr(mv, "status", "READY")) == "PENDING_REGISTRATION":
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"MLflow version {mv.version} of '{name}' is still pending after "
                    f"{timeout:.0f} s. Check the registry, then list the versions again."
                )
            time.sleep(1)
            mv = self.client.get_model_version(name, mv.version)
        return mv

    def _check_gates(self, name: str, mv: Any, gates: Dict[str, Any], to_stage: str) -> None:
        from ubunye.models.gates import PromotionGate
        from ubunye.models.lifecycle import _gate_metadata

        info = self._info(name, mv)
        failed = PromotionGate(gates).failed_gates(info.metrics, _gate_metadata(info.metadata))
        if failed:
            details = "\n".join(f"  - {r.gate_name}: {r.message}" for r in failed)
            raise PromotionBlockedError(
                f"Promotion blocked: {len(failed)} gate(s) failed:\n{details}",
                context={"Version": info.version, "Target stage": to_stage},
                hint="Fix the failing gates or adjust thresholds in promotion_gates config.",
            )

    def _in_stage(self, name: str, stage: str) -> Any:
        alias = _ALIASES.get(stage)
        if alias is not None:
            holder = self._aliases(name).get(alias)
            matches = [mv for mv in self._versions(name) if str(mv.version) == holder]
        else:
            matches = [mv for mv in self._versions(name) if mv.tags.get(_STAGE_TAG) == stage]
        if not matches:
            raise VersionNotFoundError(
                f"No version of '{name}' is currently in stage '{stage}'.",
                context={"Model": name, "Stage": stage},
                hint="Register and promote a model version to this stage first.",
            )
        return matches[0]

    def _set_stage(self, name: str, mv: Any, stage: str, extra: Dict[str, str]) -> ModelVersionInfo:
        from ubunye.models.registry import _utcnow

        if stage not in _STAGES:
            raise ValueError(f"Unknown stage '{stage}'. Use one of: {', '.join(_STAGES)}.")
        old = (mv.tags or {}).get(_STAGE_TAG, "development")
        tags = {_STAGE_TAG: stage, **{_META + k: v for k, v in extra.items()}}
        if stage in _STAGE_TIME:
            tags[_META + _STAGE_TIME[stage]] = _utcnow()
        for key, value in tags.items():
            self.client.set_model_version_tag(name, mv.version, key, value)
        if stage in _ALIASES:
            # The version moved here last holds the alias.
            self.client.set_registered_model_alias(name, _ALIASES[stage], mv.version)
        if old != stage:
            self._sync_alias(name, old)
        return self._info(name, self.client.get_model_version(name, mv.version))

    def _sync_alias(self, name: str, stage: str) -> None:
        """Point a stage's alias at a version still in that stage, or remove it."""
        alias = _ALIASES.get(stage)
        if alias is None:
            return
        holder = self._aliases(name).get(alias)
        left = [mv for mv in self._versions(name) if mv.tags.get(_STAGE_TAG) == stage]
        if holder in {str(mv.version) for mv in left}:
            return
        if left:
            when = _META + _STAGE_TIME[stage]
            best = max(left, key=lambda mv: (mv.tags.get(when, ""), int(mv.version)))
            self.client.set_registered_model_alias(name, alias, best.version)
        elif holder is not None:
            try:
                self.client.delete_registered_model_alias(name, alias)
            except Exception as exc:  # already gone with its version
                if not _is_missing(exc):
                    raise
