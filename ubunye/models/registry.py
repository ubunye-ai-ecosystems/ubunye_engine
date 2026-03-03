"""Model Registry — manages ML model versions and their lifecycle stages.

Storage layout on the filesystem::

    {store_path}/{use_case}/{model_name}/
        registry.json                    ← ModelRecord (all versions index)
        versions/
            1.0.0/
                model/                   ← user's opaque artifact files
                metadata.json            ← model.metadata() output
                metrics.json             ← metrics from model.train()

Lifecycle stages (ModelStage): development → staging → production → archived.
When a new version is promoted to production, the current production version is
automatically archived to ensure there is only one production version at a time.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ubunye.models.base import UbunyeModel
from ubunye.models.gates import PromotionGate

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


@dataclass
class ModelRecord:
    """Registry entry for a single model — contains all its versions."""

    model_name: str
    use_case: str
    versions: Dict[str, ModelVersion] = field(default_factory=dict)

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
        store_path: Root directory for all model artifacts and registry JSON files.
    """

    def __init__(self, store_path: str):
        self.store_path = Path(store_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(
        self,
        use_case: str,
        model_name: str,
        version: Optional[str],
        model: UbunyeModel,
        metrics: Dict[str, Any],
        lineage_run_id: Optional[str] = None,
        registered_by: Optional[str] = None,
    ) -> ModelVersion:
        """Register a trained model as a new version (stage=development).

        1. Saves model artifacts to ``versions/<version>/model/``.
        2. Writes ``metadata.json`` and ``metrics.json``.
        3. Adds a new :class:`ModelVersion` entry to ``registry.json``.

        Args:
            use_case: Logical use-case grouping (e.g. ``"fraud_detection"``).
            model_name: Model identifier (e.g. ``"FraudRiskModel"``).
            version: Semver string. Pass ``None`` to auto-generate.
            model: A trained :class:`UbunyeModel` instance.
            metrics: Metrics dict returned by :meth:`UbunyeModel.train`.
            lineage_run_id: Optional lineage ``run_id`` from the training run.
            registered_by: Optional username for the audit trail.

        Returns:
            The newly created :class:`ModelVersion`.
        """
        record = self._load_or_create_record(use_case, model_name)

        if version is None:
            version = self._next_version(record)

        if version in record.versions:
            raise ValueError(
                f"Version '{version}' already exists for {use_case}/{model_name}. "
                "Use a different version string or set auto_version=True."
            )

        # Persist model artifacts
        version_dir = self._version_dir(use_case, model_name, version)
        model_dir = version_dir / "model"
        model_dir.mkdir(parents=True, exist_ok=True)
        model.save(str(model_dir))

        # Write metadata and metrics
        _write_json(version_dir / "metadata.json", model.metadata())
        _write_json(version_dir / "metrics.json", metrics)

        model_version = ModelVersion(
            version=version,
            stage=ModelStage.DEVELOPMENT,
            metrics=metrics,
            metadata=model.metadata(),
            lineage_run_id=lineage_run_id,
            promoted_by=registered_by,
        )
        record.versions[version] = model_version
        self._save_record(record)
        return model_version

    def promote(
        self,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: ModelStage,
        promoted_by: Optional[str] = None,
        gates: Optional[Dict[str, Any]] = None,
    ) -> ModelVersion:
        """Promote a model version to a higher stage.

        If ``gates`` are provided, all gate checks must pass before promotion.
        If promoting to production, the current production version is automatically
        archived.

        Args:
            use_case: Use-case grouping.
            model_name: Model identifier.
            version: Version string to promote.
            to_stage: Target :class:`ModelStage`.
            promoted_by: Optional username.
            gates: Optional dict of promotion gate rules (see :class:`PromotionGate`).

        Raises:
            ValueError: Version not found, or one or more promotion gates failed.

        Returns:
            Updated :class:`ModelVersion`.
        """
        record = self._load_record(use_case, model_name)
        mv = self._get_version_or_raise(record, version)

        # Evaluate promotion gates if provided
        if gates:
            gate = PromotionGate(gates)
            failed = gate.failed_gates(mv.metrics, mv.metadata)
            if failed:
                details = "\n".join(f"  - {r.gate_name}: {r.message}" for r in failed)
                raise ValueError(f"Promotion blocked — {len(failed)} gate(s) failed:\n{details}")

        now = _utcnow()

        # Archive current production version before promoting new one
        if to_stage == ModelStage.PRODUCTION:
            current_prod = record.get_production_version()
            if current_prod is not None and current_prod.version != version:
                current_prod.stage = ModelStage.ARCHIVED
                current_prod.archived_at = now

        # Update stage timestamps
        mv.stage = to_stage
        mv.promoted_by = promoted_by
        if to_stage == ModelStage.STAGING:
            mv.promoted_to_staging = now
        elif to_stage == ModelStage.PRODUCTION:
            mv.promoted_to_prod = now
        elif to_stage == ModelStage.ARCHIVED:
            mv.archived_at = now

        self._save_record(record)
        return mv

    def demote(
        self,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: ModelStage,
    ) -> ModelVersion:
        """Demote a model version to a lower stage.

        Args:
            to_stage: Target stage (typically ``development`` or ``staging``).

        Returns:
            Updated :class:`ModelVersion`.
        """
        record = self._load_record(use_case, model_name)
        mv = self._get_version_or_raise(record, version)
        mv.stage = to_stage
        self._save_record(record)
        return mv

    def rollback(
        self,
        use_case: str,
        model_name: str,
        to_version: str,
    ) -> ModelVersion:
        """Roll back production to a specific previous version.

        1. Archives the current production version.
        2. Promotes ``to_version`` to production.

        Args:
            to_version: The version string to restore to production.

        Returns:
            The restored :class:`ModelVersion` (now in production stage).
        """
        record = self._load_record(use_case, model_name)
        self._get_version_or_raise(record, to_version)  # validate exists

        now = _utcnow()
        current_prod = record.get_production_version()
        if current_prod is not None and current_prod.version != to_version:
            current_prod.stage = ModelStage.ARCHIVED
            current_prod.archived_at = now

        target = record.versions[to_version]
        target.stage = ModelStage.PRODUCTION
        target.promoted_to_prod = now

        self._save_record(record)
        return target

    def archive(self, use_case: str, model_name: str, version: str) -> ModelVersion:
        """Archive a model version.

        Returns:
            Updated :class:`ModelVersion`.
        """
        return self.demote(use_case, model_name, version, ModelStage.ARCHIVED)

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

        Args:
            use_case: Use-case grouping.
            model_name: Model identifier.
            task_dir: Task directory containing model.py (passed to caller for loading).
            version: Exact version string.
            stage: Stage to look up active version for.

        Returns:
            ``(artifact_path, model_version)`` — caller calls
            ``ModelClass.load(artifact_path)`` to obtain the model instance.

        Raises:
            ValueError: No matching version found.
        """
        record = self._load_record(use_case, model_name)

        if version is not None:
            mv = self._get_version_or_raise(record, version)
        elif stage is not None:
            mv = record.get_active_version(stage)
            if mv is None:
                raise ValueError(
                    f"No version of '{model_name}' is currently in stage '{stage.value}'."
                )
        else:
            raise ValueError("Specify either 'version' or 'stage'.")

        model_path = str(self._version_dir(use_case, model_name, mv.version) / "model")
        return model_path, mv

    def list_versions(self, use_case: str, model_name: str) -> List[ModelVersion]:
        """List all registered versions for a model (newest first by registered_at).

        Raises:
            FileNotFoundError: Model not found in registry.
        """
        record = self._load_record(use_case, model_name)
        return sorted(
            record.versions.values(),
            key=lambda v: v.registered_at,
            reverse=True,
        )

    def compare_versions(
        self,
        use_case: str,
        model_name: str,
        version_a: str,
        version_b: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Compare metrics between two versions.

        Returns:
            Dict mapping metric name → ``{"a": val_a, "b": val_b, "delta": diff}``.
            Only metrics present in at least one version are included.
        """
        record = self._load_record(use_case, model_name)
        va = self._get_version_or_raise(record, version_a)
        vb = self._get_version_or_raise(record, version_b)

        all_keys = set(va.metrics) | set(vb.metrics)
        result: Dict[str, Dict[str, Any]] = {}
        for key in sorted(all_keys):
            a_val = va.metrics.get(key)
            b_val = vb.metrics.get(key)
            delta: Optional[float] = None
            if isinstance(a_val, (int, float)) and isinstance(b_val, (int, float)):
                delta = round(float(b_val) - float(a_val), 6)
            result[key] = {"a": a_val, "b": b_val, "delta": delta}
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _version_dir(self, use_case: str, model_name: str, version: str) -> Path:
        return self.store_path / use_case / model_name / "versions" / version

    def _registry_path(self, use_case: str, model_name: str) -> Path:
        return self.store_path / use_case / model_name / "registry.json"

    def _load_or_create_record(self, use_case: str, model_name: str) -> ModelRecord:
        path = self._registry_path(use_case, model_name)
        if path.exists():
            return self._load_record(use_case, model_name)
        return ModelRecord(model_name=model_name, use_case=use_case)

    def _load_record(self, use_case: str, model_name: str) -> ModelRecord:
        path = self._registry_path(use_case, model_name)
        if not path.exists():
            raise FileNotFoundError(
                f"No registry found for '{use_case}/{model_name}'. " f"Expected: {path}"
            )
        data = json.loads(path.read_text(encoding="utf-8"))
        versions = {k: ModelVersion(**v) for k, v in data.get("versions", {}).items()}
        return ModelRecord(
            model_name=data["model_name"],
            use_case=data["use_case"],
            versions=versions,
        )

    def _save_record(self, record: ModelRecord) -> None:
        path = self._registry_path(record.use_case, record.model_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "model_name": record.model_name,
            "use_case": record.use_case,
            "versions": {k: asdict(v) for k, v in record.versions.items()},
        }
        # Convert ModelStage enum values to strings for JSON serialisation
        for v_dict in data["versions"].values():
            if isinstance(v_dict.get("stage"), ModelStage):
                v_dict["stage"] = v_dict["stage"].value
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    @staticmethod
    def _next_version(record: ModelRecord) -> str:
        """Auto-generate the next patch version based on existing versions."""
        if not record.versions:
            return "1.0.0"
        parsed = []
        for v in record.versions:
            try:
                parts = tuple(int(x) for x in v.split("."))
                if len(parts) == 3:
                    parsed.append(parts)
            except ValueError:
                pass
        if not parsed:
            return "1.0.0"
        major, minor, patch = max(parsed)
        return f"{major}.{minor}.{patch + 1}"

    @staticmethod
    def _get_version_or_raise(record: ModelRecord, version: str) -> ModelVersion:
        mv = record.versions.get(version)
        if mv is None:
            available = ", ".join(record.versions) or "(none)"
            raise ValueError(
                f"Version '{version}' not found in {record.use_case}/{record.model_name}. "
                f"Available: {available}"
            )
        return mv


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
