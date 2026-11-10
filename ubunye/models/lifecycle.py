"""The model lifecycle on top of any registry backend (ADR 010).

A registry backend implements six methods (:class:`ubunye.interfaces.RegistryBackend`).
This module builds the rest of what :class:`ubunye.models.ModelRegistry` offers on
those six, so every backend gets it for free and stays small:

- version numbers when none is given (the next patch after the highest);
- refusing a version that already exists;
- promotion gates, stored with the model and checked here before a promotion
  (``force`` skips them);
- one production version at a time (the old one is archived);
- rollback, archive and compare;
- demote only ever moves a version down (:func:`check_demote`), so it cannot be
  used to skip the promotion gates.

Gates are stored as the version metadata key ``promotion_gates`` (JSON). A version
registered without gates carries the model's current ones forward, so the newest
version always holds the gates that apply; gates given empty are stored as
``{}``, which clears them. A backend that keeps gates its own way
may offer ``promotion_gates(use_case, model_name)``; it is used when present.

The stages are the public contract: ``development``, ``staging``, ``production``,
``archived``. A backend may name them differently inside, and maps them at its edge.
"""

from __future__ import annotations

import inspect
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from ubunye.core.errors import (
    PromotionBlockedError,
    RegistryError,
    RegistryNotFoundError,
    VersionExistsError,
    VersionNotFoundError,
)
from ubunye.interfaces.registry import ModelVersionInfo
from ubunye.models.gates import PromotionGate

log = logging.getLogger(__name__)

GATES_KEY = "promotion_gates"

# ModelVersion fields a backend reports through ModelVersionInfo.metadata.
_FIELDS = (
    "lineage_run_id",
    "config_hash",
    "promoted_by",
    "promoted_to_staging",
    "promoted_to_prod",
    "archived_at",
)


def to_model_version(info: ModelVersionInfo) -> Any:
    """A backend's :class:`ModelVersionInfo` as the public :class:`ModelVersion`."""
    from ubunye.models.registry import ModelStage, ModelVersion

    meta = dict(info.metadata or {})
    fields = {k: meta.pop(k) for k in _FIELDS if k in meta}
    return ModelVersion(
        version=info.version,
        stage=ModelStage(info.stage),
        registered_at=info.created_at,
        metrics=dict(info.metrics or {}),
        metadata=meta,
        **fields,
    )


def check_demote(version: str, current: str, to_stage: str) -> None:
    """Refuse a demote that would move a version up.

    Demote goes to ``development`` or ``archived`` from anywhere, and to ``staging``
    only from ``production`` (or ``staging``). Reaching ``staging`` or
    ``production`` from lower down is a promotion and must pass the gates, so it
    goes through ``promote``. Every registry backend calls this in its ``demote``.
    """
    if to_stage in ("development", "archived"):
        return
    if to_stage == "staging" and current in ("staging", "production"):
        return
    raise RegistryError(
        f"Cannot demote version {version} from '{current}' to '{to_stage}': "
        "demote only moves a version down.",
        context={"Version": version, "Stage now": current, "Target stage": to_stage},
        hint="Use promote to move a version up; it checks the promotion gates. "
        "Demote goes to development, archived, or staging from production.",
    )


def next_version(versions: List[str]) -> str:
    """The next patch version after the highest ``x.y.z`` in ``versions``."""
    parsed = []
    for v in versions:
        try:
            parts = tuple(int(x) for x in v.split("."))
        except ValueError:
            continue
        if len(parts) == 3:
            parsed.append(parts)
    if not parsed:
        return "1.0.0"
    major, minor, patch = max(parsed)
    return f"{major}.{minor}.{patch + 1}"


def _gate_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Backend metadata is text; a gate reading a flag wants a bool."""
    out: Dict[str, Any] = dict(metadata)
    for key, value in metadata.items():
        if isinstance(value, str) and value.lower() in ("true", "false"):
            out[key] = value.lower() == "true"
    return out


def check_gates(gates: Dict[str, Any], info: ModelVersionInfo, to_stage: str) -> None:
    """Raise :class:`PromotionBlockedError` when ``info`` fails any of ``gates``."""
    failed = PromotionGate(gates).failed_gates(
        dict(info.metrics or {}), _gate_metadata(dict(info.metadata or {}))
    )
    if failed:
        details = "\n".join(f"  - {r.gate_name}: {r.message}" for r in failed)
        raise PromotionBlockedError(
            f"Promotion blocked: {len(failed)} gate(s) failed:\n{details}",
            context={"Version": info.version, "Target stage": to_stage},
            hint="Fix the failing gates or adjust thresholds in promotion_gates config.",
        )


class BackendLifecycle:
    """The :class:`ModelRegistry` API, done on top of a :class:`RegistryBackend`."""

    def __init__(self, backend: Any) -> None:
        self.backend = backend

    # -- reading ---------------------------------------------------------------

    def _infos(self, use_case: str, model_name: str) -> List[ModelVersionInfo]:
        try:
            return list(self.backend.list_versions(use_case=use_case, model_name=model_name))
        except RegistryNotFoundError:
            return []

    def _info(self, use_case: str, model_name: str, version: str) -> ModelVersionInfo:
        # From the list, so a remote artifact is not downloaded just to read its stage.
        for info in self.backend.list_versions(use_case=use_case, model_name=model_name):
            if info.version == version:
                return info
        # Not there: the backend's own get raises its clear not-found error.
        _, info = self.backend.get(use_case=use_case, model_name=model_name, version=version)
        return info

    def list_versions(self, use_case: str, model_name: str) -> List[Any]:
        infos = self.backend.list_versions(use_case=use_case, model_name=model_name)
        return [to_model_version(i) for i in infos]

    def get_model(
        self,
        use_case: str,
        model_name: str,
        version: Optional[str] = None,
        stage: Optional[Any] = None,
    ) -> Tuple[str, Any]:
        if version is None and stage is None:
            raise VersionNotFoundError(
                "Specify either 'version' or 'stage'.",
                hint="Pass version='x.y.z' or stage=ModelStage.PRODUCTION.",
            )
        path, info = self.backend.get(
            use_case=use_case,
            model_name=model_name,
            version=version,
            stage=None if version is not None else _stage_name(stage),
        )
        return str(path), to_model_version(info)

    def promotion_gates(self, use_case: str, model_name: str) -> Dict[str, Any]:
        native = getattr(self.backend, "promotion_gates", None)
        if callable(native):
            return dict(native(use_case, model_name))
        for info in self._infos(use_case, model_name):  # newest first
            raw = (info.metadata or {}).get(GATES_KEY)
            if raw:
                return dict(json.loads(raw))
        return {}

    # -- writing ---------------------------------------------------------------

    def register(
        self,
        use_case: str,
        model_name: str,
        version: Optional[str],
        model: Any,
        metrics: Dict[str, Any],
        lineage_run_id: Optional[str] = None,
        registered_by: Optional[str] = None,
        promotion_gates: Optional[Dict[str, Any]] = None,
    ) -> Any:
        existing = [i.version for i in self._infos(use_case, model_name)]
        if version is None:
            version = next_version(existing)
        if version in existing:
            raise VersionExistsError(
                f"Version '{version}' already exists for {use_case}/{model_name}.",
                context={"Version": version, "Model": f"{use_case}/{model_name}"},
                hint="Use a different version string or leave it out to number it.",
            )
        gates = (
            promotion_gates
            if promotion_gates is not None
            else (self.promotion_gates(use_case, model_name) if existing else {})
        )
        metadata: Dict[str, str] = {}
        if gates or promotion_gates is not None:  # given empty: the gates are cleared
            metadata[GATES_KEY] = json.dumps(gates, sort_keys=True)
        if registered_by:
            metadata["promoted_by"] = registered_by
        info = self.backend.register(
            use_case=use_case,
            model_name=model_name,
            version=version,
            model=model,
            metrics=metrics,
            metadata=metadata,
            lineage_run_id=lineage_run_id,
        )
        return to_model_version(info)

    def _archive_other_production(self, use_case: str, model_name: str, keep: str) -> None:
        for info in self._infos(use_case, model_name):
            if info.stage == "production" and info.version != keep:
                self.backend.demote(
                    use_case=use_case,
                    model_name=model_name,
                    version=info.version,
                    to_stage="archived",
                )

    def _move_up(
        self,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
        promoted_by: Optional[str],
        **extra: Any,
    ) -> Any:
        self.backend.promote(
            use_case=use_case,
            model_name=model_name,
            version=version,
            to_stage=to_stage,
            promoted_by=promoted_by,
            **extra,
        )
        if to_stage == "production":
            self._archive_other_production(use_case, model_name, keep=version)
        # Read back: archiving the old production version may have touched this one's store.
        return to_model_version(self._info(use_case, model_name, version))

    def promote(
        self,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: Any,
        promoted_by: Optional[str] = None,
        gates: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> Any:
        stage = _stage_name(to_stage)
        info = self._info(use_case, model_name, version)  # raises if missing
        if gates is None:
            gates = self.promotion_gates(use_case, model_name)
        if gates and force:
            log.warning(
                "Promoting %s/%s %s to %s without checking its gates: %s",
                use_case,
                model_name,
                version,
                stage,
                gates,
            )
            if "force" in inspect.signature(self.backend.promote).parameters:
                # The backend records that the promotion was forced.
                return self._move_up(
                    use_case, model_name, version, stage, promoted_by, gates=gates, force=True
                )
        elif gates:
            check_gates(gates, info, stage)
        # The gates are checked here; an empty dict tells the backend there is nothing left.
        return self._move_up(use_case, model_name, version, stage, promoted_by, gates={})

    def demote(self, use_case: str, model_name: str, version: str, to_stage: Any) -> Any:
        stage = _stage_name(to_stage)
        check_demote(version, self._info(use_case, model_name, version).stage, stage)
        info = self.backend.demote(
            use_case=use_case,
            model_name=model_name,
            version=version,
            to_stage=stage,
        )
        return to_model_version(info)

    def rollback(self, use_case: str, model_name: str, to_version: str) -> Any:
        info = self._info(use_case, model_name, to_version)  # raises if missing
        # A rollback restores a version; who promoted it stays as it was.
        by = (info.metadata or {}).get("promoted_by")
        return self._move_up(use_case, model_name, to_version, "production", by, gates={})

    def archive(self, use_case: str, model_name: str, version: str) -> Any:
        return self.demote(use_case, model_name, version, "archived")

    def compare_versions(
        self, use_case: str, model_name: str, version_a: str, version_b: str
    ) -> Dict[str, Dict[str, Any]]:
        from ubunye.models.registry import compare_metrics

        a = self._info(use_case, model_name, version_a)
        b = self._info(use_case, model_name, version_b)
        return compare_metrics(dict(a.metrics or {}), dict(b.metrics or {}))


def _stage_name(stage: Any) -> str:
    return str(getattr(stage, "value", stage))
