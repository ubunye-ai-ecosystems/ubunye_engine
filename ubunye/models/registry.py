"""Model Registry: manages ML model versions and their lifecycle stages.

:class:`ModelRegistry` is one thin front over :mod:`ubunye.models.lifecycle`, for
every registry backend (ADR 010). The lifecycle rules (version numbers, promotion
gates, one production version at a time, rollback, archive, compare) live there
once. A backend only stores versions.

:class:`FilesystemRegistryBackend` is the default backend. Its layout::

    {store_path}/{use_case}/{model_name}/
        registry.json                    <- ModelRecord (all versions index)
        versions/
            1.0.0/
                model/                   <- user's opaque artifact files
                metadata.json            <- model.metadata() output
                metrics.json             <- metrics from model.train()

Lifecycle stages (ModelStage): development, staging, production, archived.
When a new version is promoted to production, the current production version is
archived, so there is only one production version at a time.
"""

from __future__ import annotations

import importlib.metadata as _meta
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from ubunye.core.errors import RegistryNotFoundError, VersionExistsError, VersionNotFoundError
from ubunye.interfaces.registry import ModelVersionInfo
from ubunye.models.base import UbunyeModel
from ubunye.models.lifecycle import (
    _FIELDS,
    GATES_KEY,
    BackendLifecycle,
    check_demote,
    check_gates,
    next_version,
)

#: Where FilesystemRegistryBackend keeps protocol metadata inside a version's metadata.
_PROTOCOL_METADATA = "registry_metadata"


def _engine_version() -> str:
    try:
        return _meta.version("ubunye-engine")
    except _meta.PackageNotFoundError:
        return "unknown"


# ---------------------------------------------------------------------------
# Enums and Dataclasses
# ---------------------------------------------------------------------------


class ModelStage(str, Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


@dataclass
class ModelVersion:
    """Metadata for a single registered model version."""

    version: str
    stage: ModelStage = ModelStage.DEVELOPMENT
    registered_at: str = ""
    promoted_to_staging: Optional[str] = None
    promoted_to_prod: Optional[str] = None
    archived_at: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    lineage_run_id: Optional[str] = None
    config_hash: Optional[str] = None
    promoted_by: Optional[str] = None

    def __post_init__(self):
        if not self.registered_at:
            self.registered_at = _utcnow()
        # Coerce stage string back to enum (needed when deserialising from dict)
        if isinstance(self.stage, str):
            self.stage = ModelStage(self.stage)

    def to_info(self, *, name: str = "", artifact_path: str = "") -> ModelVersionInfo:
        """Convert to the cross-boundary :class:`ModelVersionInfo` type.

        The model's metadata keeps its JSON types, so it converts back to the
        same :class:`ModelVersion` (:func:`ubunye.models.lifecycle.to_model_version`).
        """
        own = dict(self.metadata or {})
        meta: Dict[str, Any] = own
        # Metadata a caller passed through the RegistryBackend protocol (ADR 010).
        for key, value in (own.pop(_PROTOCOL_METADATA, None) or {}).items():
            meta.setdefault(str(key), str(value))
        for key in _FIELDS:
            if getattr(self, key):
                meta[key] = getattr(self, key)
        return ModelVersionInfo(
            name=name,
            version=self.version,
            stage=self.stage.value if isinstance(self.stage, ModelStage) else self.stage,
            created_at=self.registered_at,
            engine_version=_engine_version(),
            artifact_path=artifact_path,
            metrics=dict(self.metrics),
            metadata=meta,
        )


@dataclass
class ModelRecord:
    """Registry entry for a single model — contains all its versions."""

    model_name: str
    use_case: str
    versions: Dict[str, ModelVersion] = field(default_factory=dict)
    #: The gates every promotion of this model must pass (min_<metric>,
    #: max_<metric>, require_drift_check), set when a training run registers it.
    #: Kept here so a promotion from the CLI or any other caller checks them too.
    promotion_gates: Dict[str, Any] = field(default_factory=dict)

    def get_active_version(self, stage: ModelStage) -> Optional[ModelVersion]:
        """Return the version currently in the given stage, or None."""
        for v in self.versions.values():
            if v.stage == stage:
                return v
        return None

    def get_production_version(self) -> Optional[ModelVersion]:
        return self.get_active_version(ModelStage.PRODUCTION)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class ModelRegistry:
    """Manages model versions and lifecycle transitions.

    Args:
        store_path: Where the models live. For the filesystem backend, the root
            directory (or ``s3://``-style URL) for all artifacts and registry files.
            Any other backend gets it unchanged.
        backend: The registry backend (ADR 010): a name registered under the
            ``ubunye.registry_backends`` entry point group, or a backend object.
            Not given, ``UBUNYE_REGISTRY_BACKEND`` decides, else ``filesystem``.
            Every backend gets this whole API built on its six protocol methods
            (:mod:`ubunye.models.lifecycle`).
    """

    def __init__(self, store_path: str, backend: Any = None):
        from ubunye.models.backends import DEFAULT_REGISTRY_BACKEND, choose, resolve_registry

        self.store_path = str(store_path)
        if backend is None or isinstance(backend, str):
            self.backend_name = choose(backend)
            if self.backend_name == DEFAULT_REGISTRY_BACKEND:
                backend = FilesystemRegistryBackend(self.store_path)
            else:
                backend = resolve_registry(self.backend_name, store=self.store_path)
        else:
            self.backend_name = type(backend).__name__
        self._lifecycle = BackendLifecycle(backend)

    def register(
        self,
        use_case: str,
        model_name: str,
        version: Optional[str],
        model: UbunyeModel,
        metrics: Dict[str, Any],
        lineage_run_id: Optional[str] = None,
        registered_by: Optional[str] = None,
        promotion_gates: Optional[Dict[str, Any]] = None,
    ) -> ModelVersion:
        """Register a trained model as a new version (stage=development).

        Args:
            use_case: Logical use-case grouping (e.g. ``"fraud_detection"``).
            model_name: Model identifier (e.g. ``"FraudRiskModel"``).
            version: Semver string. Pass ``None`` to auto-generate.
            model: A trained :class:`UbunyeModel` instance.
            metrics: Metrics dict returned by :meth:`UbunyeModel.train`.
            lineage_run_id: The ``run_id`` of the training run. When omitted
                inside a run (a task's ``transformations.py``), it is that run's
                id, so the model points at its run record.
            registered_by: Optional username for the audit trail.
            promotion_gates: The gates every later promotion of this model must
                pass. Given, they replace the model's stored gates.

        Returns:
            The newly created :class:`ModelVersion`.
        """
        if lineage_run_id is None:
            from ubunye.core.runtime import current_run_id

            lineage_run_id = current_run_id()
        return self._lifecycle.register(
            use_case,
            model_name,
            version,
            model,
            metrics,
            lineage_run_id=lineage_run_id,
            registered_by=registered_by,
            promotion_gates=promotion_gates,
        )

    def promote(
        self,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: ModelStage,
        promoted_by: Optional[str] = None,
        gates: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> ModelVersion:
        """Promote a model version to a higher stage.

        Every gate must pass first: ``gates`` if given, else the gates stored with
        the model when it was registered. ``force=True`` skips them, and the
        version records that it was forced and which gates it skipped.
        If promoting to production, the current production version is automatically
        archived.

        Raises:
            VersionNotFoundError: Version not found.
            PromotionBlockedError: One or more promotion gates failed.

        Returns:
            Updated :class:`ModelVersion`.
        """
        return self._lifecycle.promote(
            use_case,
            model_name,
            version,
            to_stage,
            promoted_by=promoted_by,
            gates=gates,
            force=force,
        )

    def demote(
        self,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: ModelStage,
    ) -> ModelVersion:
        """Demote a model version to a lower stage.

        Demote only moves a version down: to ``development`` or ``archived``, or to
        ``staging`` from ``production``. Moving up is a promotion; use
        :meth:`promote`, which checks the promotion gates.

        Raises:
            RegistryError: if the move would take the version up a stage.
        """
        return self._lifecycle.demote(use_case, model_name, version, to_stage)

    def rollback(self, use_case: str, model_name: str, to_version: str) -> ModelVersion:
        """Roll production back to ``to_version``; the current production version is archived."""
        return self._lifecycle.rollback(use_case, model_name, to_version)

    def archive(self, use_case: str, model_name: str, version: str) -> ModelVersion:
        """Archive a model version."""
        return self._lifecycle.archive(use_case, model_name, version)

    def get_model(
        self,
        use_case: str,
        model_name: str,
        task_dir: Optional[str] = None,
        version: Optional[str] = None,
        stage: Optional[ModelStage] = None,
    ) -> Tuple[str, ModelVersion]:
        """Return the artifact path and version metadata for a model.

        Specify either ``version`` (exact) or ``stage`` (active version in that stage).
        ``task_dir`` is accepted for older callers and not used.

        Returns:
            ``(artifact_path, model_version)``. The caller calls
            ``ModelClass.load(artifact_path)`` to obtain the model instance.

        Raises:
            VersionNotFoundError: No matching version found.
        """
        return self._lifecycle.get_model(use_case, model_name, version=version, stage=stage)

    def list_versions(self, use_case: str, model_name: str) -> List[ModelVersion]:
        """List all registered versions for a model (newest first by registered_at).

        Raises:
            RegistryNotFoundError: Model not found in registry.
        """
        return self._lifecycle.list_versions(use_case, model_name)

    def compare_versions(
        self,
        use_case: str,
        model_name: str,
        version_a: str,
        version_b: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Compare metrics between two versions.

        Returns:
            Dict mapping metric name to ``{"a": val_a, "b": val_b, "delta": diff}``.
            Only metrics present in at least one version are included.
        """
        return self._lifecycle.compare_versions(use_case, model_name, version_a, version_b)

    def promotion_gates(self, use_case: str, model_name: str) -> Dict[str, Any]:
        """The gates stored with a model (empty if none were set)."""
        return self._lifecycle.promotion_gates(use_case, model_name)


# ---------------------------------------------------------------------------
# FilesystemRegistryBackend: the default RegistryBackend
# ---------------------------------------------------------------------------


class FilesystemRegistryBackend:
    """The default :class:`RegistryBackend`: a folder (or bucket) of JSON files.

    The SCHEME of the store path picks where the files go: a plain directory or
    ``file://`` stays local, ``s3://``, ``gs://`` and friends go through fsspec,
    and any scheme registered under the ``ubunye.artifact_stores`` entry point
    group goes to that plugin.

    Metadata it reports keeps the JSON types the store holds. Writes are
    synchronous: local disk is fast enough that a background worker would only
    add overhead.
    """

    def __init__(self, store_path: str) -> None:
        from ubunye.models.artifact_store import resolve_store

        self._store_path = str(store_path)
        self._store = resolve_store(self._store_path)

    # -- the record ----------------------------------------------------------------

    def _version_dir(self, use_case: str, model_name: str, version: str) -> str:
        return self._store.join(self._store_path, use_case, model_name, "versions", version)

    def _model_dir(self, use_case: str, model_name: str, version: str) -> str:
        return self._store.join(self._version_dir(use_case, model_name, version), "model")

    def _registry_path(self, use_case: str, model_name: str) -> str:
        return self._store.join(self._store_path, use_case, model_name, "registry.json")

    def _load_record(self, use_case: str, model_name: str) -> ModelRecord:
        path = self._registry_path(use_case, model_name)
        if not self._store.exists(path):
            raise RegistryNotFoundError(
                f"No registry found for '{use_case}/{model_name}'.",
                context={"Model": f"{use_case}/{model_name}", "Expected": str(path)},
                hint="Register a model version first with registry.register().",
            )
        data = json.loads(self._store.read_text(path))
        versions = {k: ModelVersion(**v) for k, v in data.get("versions", {}).items()}
        return ModelRecord(
            model_name=data["model_name"],
            use_case=data["use_case"],
            versions=versions,
            promotion_gates=dict(data.get("promotion_gates") or {}),
        )

    def _save_record(self, record: ModelRecord) -> None:
        versions = {k: asdict(v) for k, v in record.versions.items()}
        for v in versions.values():
            v["stage"] = ModelStage(v["stage"]).value
        data = {
            "model_name": record.model_name,
            "use_case": record.use_case,
            "versions": versions,
            "promotion_gates": record.promotion_gates,
        }
        path = self._registry_path(record.use_case, record.model_name)
        self._store.write_text(path, json.dumps(data, indent=2))

    @staticmethod
    def _version(record: ModelRecord, version: str) -> ModelVersion:
        mv = record.versions.get(version)
        if mv is None:
            raise VersionNotFoundError(
                f"Version '{version}' not found in {record.use_case}/{record.model_name}.",
                context={
                    "Version": version,
                    "Model": f"{record.use_case}/{record.model_name}",
                    "Available": sorted(record.versions) or ["(none)"],
                },
                hint="Check the version string or list versions with 'ubunye models list'.",
            )
        return mv

    def _info(self, record: ModelRecord, mv: ModelVersion) -> ModelVersionInfo:
        path = self._model_dir(record.use_case, record.model_name, mv.version)
        return mv.to_info(name=record.model_name, artifact_path=path)

    # -- the protocol ----------------------------------------------------------------

    def promotion_gates(self, use_case: str, model_name: str) -> Dict[str, Any]:
        """The gates stored with a model (empty if none, or no model yet)."""
        try:
            return dict(self._load_record(use_case, model_name).promotion_gates)
        except RegistryNotFoundError:
            return {}

    def flush(self, timeout: Optional[float] = None) -> int:
        """No-op for filesystem backend — writes are synchronous."""
        return 0

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
        meta = dict(metadata or {})
        # The gates live on the model's record, as they always have.
        gates_json = meta.pop(GATES_KEY, None)
        registered_by = meta.pop("promoted_by", None)
        try:
            record = self._load_record(use_case, model_name)
        except RegistryNotFoundError:
            record = ModelRecord(model_name=model_name, use_case=use_case)
        if gates_json is not None:
            record.promotion_gates = dict(json.loads(gates_json))
        if version is None:
            version = next_version(list(record.versions))
        if version in record.versions:
            raise VersionExistsError(
                f"Version '{version}' already exists for {use_case}/{model_name}.",
                context={"Version": version, "Model": f"{use_case}/{model_name}"},
                hint="Use a different version string or leave it out to number it.",
            )

        # Models write real files (joblib, a Keras zip), and those need a real
        # local directory: a Keras save fails on storage that cannot seek.
        # staging_dir() gives them one and publishes it to the store at the end.
        version_dir = self._version_dir(use_case, model_name, version)
        with self._store.staging_dir(self._store.join(version_dir, "model")) as local_dir:
            model.save(str(local_dir))
        model_meta = model.metadata()
        self._store.write_text(
            self._store.join(version_dir, "metadata.json"),
            json.dumps(model_meta, indent=2, default=str),
        )
        self._store.write_text(
            self._store.join(version_dir, "metrics.json"),
            json.dumps(metrics, indent=2, default=str),
        )

        mv = ModelVersion(
            version=version,
            stage=ModelStage.DEVELOPMENT,
            metrics=metrics,
            metadata=model_meta,
            lineage_run_id=lineage_run_id,
            promoted_by=registered_by,
        )
        if meta:
            # Kept, not dropped: it comes back from get() and list_versions().
            mv.metadata = {**(mv.metadata or {}), _PROTOCOL_METADATA: meta}
        record.versions[version] = mv
        self._save_record(record)
        return self._info(record, mv)

    def get(
        self,
        *,
        use_case: str,
        model_name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> Tuple[str, ModelVersionInfo]:
        record = self._load_record(use_case, model_name)
        if version is not None:
            mv = self._version(record, version)
        elif stage is not None:
            active = record.get_active_version(ModelStage(stage))
            if active is None:
                raise VersionNotFoundError(
                    f"No version of '{model_name}' is currently in stage '{stage}'.",
                    context={"Model": model_name, "Stage": stage},
                    hint="Register and promote a model version to this stage first.",
                )
            mv = active
        else:
            raise VersionNotFoundError(
                "Specify either 'version' or 'stage'.",
                hint="Pass version='x.y.z' or stage=ModelStage.PRODUCTION.",
            )
        # The caller passes this to ModelClass.load(), which reads real files, so a
        # remote store downloads to a local directory first. A local one is as is.
        path = self._store.materialize(self._model_dir(use_case, model_name, mv.version))
        return path, mv.to_info(name=model_name, artifact_path=path)

    def list_versions(self, *, use_case: str, model_name: str) -> List[ModelVersionInfo]:
        record = self._load_record(use_case, model_name)
        newest = sorted(record.versions.values(), key=lambda v: v.registered_at, reverse=True)
        return [self._info(record, mv) for mv in newest]

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
        stage = ModelStage(to_stage)
        record = self._load_record(use_case, model_name)
        mv = self._version(record, version)
        if gates is None:
            gates = record.promotion_gates
        if gates and force:
            mv.metadata = {
                **(mv.metadata or {}),
                "promotion_forced": True,
                "promotion_gates_skipped": dict(gates),
            }
        elif gates:
            check_gates(gates, mv.to_info(name=model_name), stage.value)

        now = _utcnow()
        if stage == ModelStage.PRODUCTION:
            # One production version at a time; the old one is archived in the same write.
            current = record.get_production_version()
            if current is not None and current.version != version:
                current.stage = ModelStage.ARCHIVED
                current.archived_at = now
        mv.stage = stage
        mv.promoted_by = promoted_by
        if stage == ModelStage.STAGING:
            mv.promoted_to_staging = now
        elif stage == ModelStage.PRODUCTION:
            mv.promoted_to_prod = now
        elif stage == ModelStage.ARCHIVED:
            mv.archived_at = now
        self._save_record(record)
        return self._info(record, mv)

    def demote(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
    ) -> ModelVersionInfo:
        stage = ModelStage(to_stage)
        record = self._load_record(use_case, model_name)
        mv = self._version(record, version)
        check_demote(version, mv.stage.value, stage.value)
        mv.stage = stage
        self._save_record(record)
        return self._info(record, mv)

    def delete(self, *, use_case: str, model_name: str, version: str) -> None:
        self._store.delete(self._version_dir(use_case, model_name, version))
        record = self._load_record(use_case, model_name)
        record.versions.pop(version, None)
        self._save_record(record)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def compare_metrics(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Metric name to ``{"a": val_a, "b": val_b, "delta": b - a}`` (delta None if not numbers)."""
    result: Dict[str, Dict[str, Any]] = {}
    for key in sorted(set(a) | set(b)):
        a_val = a.get(key)
        b_val = b.get(key)
        delta: Optional[float] = None
        if isinstance(a_val, (int, float)) and isinstance(b_val, (int, float)):
            delta = round(float(b_val) - float(a_val), 6)
        result[key] = {"a": a_val, "b": b_val, "delta": delta}
    return result


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()
