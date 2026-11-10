"""The tests every model registry backend must pass, shipped so a new one can prove itself.

A model registry backend is a plugin (ADR 010): anyone can write one. A backend
implements the six methods of :class:`ubunye.interfaces.RegistryBackend`
(register, get, list_versions, promote, demote, delete). Everything else users
rely on (auto versioning, promotion gates stored with the model, one production
version at a time, rollback, archive, compare) is done by
:class:`ubunye.models.ModelRegistry` on top of those six, so every backend gets
it for free. This suite checks both: the backend directly, and the full
``ModelRegistry`` API running on it.

The filesystem and MLflow backends run it too
(``tests/unit/models/test_registry_conformance.py``).

Use it from your backend's tests (needs pytest)::

    import pytest

    from ubunye.testing.registry_conformance import RegistryConformance


    class TestMyRegistry(RegistryConformance):
        @pytest.fixture
        def backend(self, tmp_path):
            # A fresh, empty registry for every test.
            return MyRegistryBackend(str(tmp_path / "store"))
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict

import pytest

from ubunye.core.errors import (
    PromotionBlockedError,
    RegistryError,
    VersionExistsError,
    VersionNotFoundError,
)
from ubunye.interfaces.registry import ModelVersionInfo, RegistryBackend
from ubunye.models.base import UbunyeModel
from ubunye.models.registry import ModelRegistry, ModelStage

USE_CASE = "conformance"
MODEL = "ConformanceModel"
STAGES = {s.value for s in ModelStage}


class ConformanceModel(UbunyeModel):
    """The smallest model there is: its artifact is one JSON file holding a tag."""

    def __init__(self, tag: str = "m") -> None:
        self.tag = tag

    def train(self, df: Any) -> Dict[str, Any]:
        return {"auc": 0.9}

    def predict(self, df: Any) -> Any:
        return df

    def save(self, path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)
        (Path(path) / "model.json").write_text(json.dumps({"tag": self.tag}), encoding="utf-8")

    @classmethod
    def load(cls, path: str) -> "ConformanceModel":
        data = json.loads((Path(path) / "model.json").read_text(encoding="utf-8"))
        return cls(data["tag"])

    def metadata(self) -> Dict[str, Any]:
        return {"library": "conformance", "library_version": "1"}


class RegistryConformance:
    """Subclass this and give it a ``backend`` fixture: a fresh backend on an empty store."""

    @pytest.fixture
    def backend(self) -> Any:  # pragma: no cover - the subclass provides it
        raise NotImplementedError("Give your conformance class a 'backend' fixture.")

    @pytest.fixture
    def registry(self, backend: Any) -> ModelRegistry:
        return ModelRegistry("", backend=backend)

    @staticmethod
    def _register(
        registry: ModelRegistry,
        version: Any = None,
        auc: float = 0.9,
        tag: str = "m",
        **kwargs: Any,
    ) -> str:
        # Registered one after another; a pause keeps "newest first" unambiguous
        # on clocks with coarse ticks (Windows).
        time.sleep(0.02)
        mv = registry.register(
            USE_CASE, MODEL, version, ConformanceModel(tag), {"auc": auc}, **kwargs
        )
        return mv.version

    @staticmethod
    def _stage(registry: ModelRegistry, version: str) -> str:
        _, mv = registry.get_model(USE_CASE, MODEL, version=version)
        return mv.stage.value

    # -- the backend itself ---------------------------------------------------

    def test_it_follows_the_registry_backend_protocol(self, backend: Any) -> None:
        assert isinstance(backend, RegistryBackend)

    def test_it_reports_versions_with_the_public_stage_names(
        self, backend: Any, registry: ModelRegistry
    ) -> None:
        self._register(registry, "1.0.0")
        infos = backend.list_versions(use_case=USE_CASE, model_name=MODEL)
        assert [i.version for i in infos] == ["1.0.0"]
        assert all(isinstance(i, ModelVersionInfo) for i in infos)
        assert {i.stage for i in infos} <= STAGES

    def test_delete_removes_a_version(self, backend: Any, registry: ModelRegistry) -> None:
        self._register(registry, "1.0.0")
        self._register(registry, "1.0.1")
        backend.delete(use_case=USE_CASE, model_name=MODEL, version="1.0.0")
        assert [mv.version for mv in registry.list_versions(USE_CASE, MODEL)] == ["1.0.1"]
        with pytest.raises(VersionNotFoundError):
            registry.get_model(USE_CASE, MODEL, version="1.0.0")

    # -- registering ------------------------------------------------------------

    def test_versions_are_numbered_when_none_is_given(self, registry: ModelRegistry) -> None:
        assert self._register(registry) == "1.0.0"
        assert self._register(registry) == "1.0.1"
        assert self._register(registry, "2.0.0") == "2.0.0"
        assert self._register(registry) == "2.0.1"

    def test_a_version_cannot_be_registered_twice(self, registry: ModelRegistry) -> None:
        self._register(registry, "1.0.0")
        with pytest.raises(VersionExistsError):
            self._register(registry, "1.0.0")

    def test_a_new_version_starts_in_development(self, registry: ModelRegistry) -> None:
        assert self._stage(registry, self._register(registry)) == "development"

    def test_the_lineage_run_id_comes_back(self, registry: ModelRegistry) -> None:
        version = self._register(registry, lineage_run_id="run-123")
        _, mv = registry.get_model(USE_CASE, MODEL, version=version)
        assert mv.lineage_run_id == "run-123"

    def test_versions_are_listed_newest_first(self, registry: ModelRegistry) -> None:
        for v in ("1.0.0", "1.0.1", "1.0.2"):
            self._register(registry, v)
        assert [mv.version for mv in registry.list_versions(USE_CASE, MODEL)] == [
            "1.0.2",
            "1.0.1",
            "1.0.0",
        ]

    # -- getting ---------------------------------------------------------------

    def test_get_by_version_gives_an_artifact_the_model_can_load(
        self, registry: ModelRegistry
    ) -> None:
        self._register(registry, "1.0.0", tag="first")
        self._register(registry, "1.0.1", tag="second")
        path, mv = registry.get_model(USE_CASE, MODEL, version="1.0.0")
        assert mv.version == "1.0.0"
        assert ConformanceModel.load(path).tag == "first"

    def test_get_by_stage(self, registry: ModelRegistry) -> None:
        self._register(registry, "1.0.0", tag="first")
        self._register(registry, "1.0.1", tag="second")
        registry.promote(USE_CASE, MODEL, "1.0.1", ModelStage.STAGING)
        path, mv = registry.get_model(USE_CASE, MODEL, stage=ModelStage.STAGING)
        assert mv.version == "1.0.1"
        assert ConformanceModel.load(path).tag == "second"

    def test_an_unknown_version_or_empty_stage_is_not_found(self, registry: ModelRegistry) -> None:
        self._register(registry, "1.0.0")
        with pytest.raises(VersionNotFoundError):
            registry.get_model(USE_CASE, MODEL, version="9.9.9")
        with pytest.raises(VersionNotFoundError):
            registry.get_model(USE_CASE, MODEL, stage=ModelStage.PRODUCTION)

    # -- promotion and gates -----------------------------------------------------

    def test_promotion_passes_gates_it_meets(self, registry: ModelRegistry) -> None:
        version = self._register(registry, auc=0.9)
        mv = registry.promote(USE_CASE, MODEL, version, ModelStage.STAGING, gates={"min_auc": 0.8})
        assert mv.stage == ModelStage.STAGING
        assert self._stage(registry, version) == "staging"

    def test_a_failing_gate_blocks_promotion_and_changes_nothing(
        self, registry: ModelRegistry
    ) -> None:
        version = self._register(registry, auc=0.5)
        with pytest.raises(PromotionBlockedError):
            registry.promote(
                USE_CASE, MODEL, version, ModelStage.PRODUCTION, gates={"min_auc": 0.8}
            )
        assert self._stage(registry, version) == "development"

    def test_gates_stored_with_the_model_are_checked_later(self, registry: ModelRegistry) -> None:
        version = self._register(registry, auc=0.5, promotion_gates={"min_auc": 0.8})
        assert registry.promotion_gates(USE_CASE, MODEL) == {"min_auc": 0.8}
        with pytest.raises(PromotionBlockedError):
            registry.promote(USE_CASE, MODEL, version, ModelStage.PRODUCTION)
        # A later version registered without gates keeps the model's gates.
        later = self._register(registry, auc=0.5)
        with pytest.raises(PromotionBlockedError):
            registry.promote(USE_CASE, MODEL, later, ModelStage.PRODUCTION)
        registry.promote(USE_CASE, MODEL, version, ModelStage.PRODUCTION, force=True)
        assert self._stage(registry, version) == "production"

    def test_gates_given_empty_clear_the_stored_ones(self, registry: ModelRegistry) -> None:
        self._register(registry, auc=0.5, promotion_gates={"min_auc": 0.8})
        later = self._register(registry, auc=0.5, promotion_gates={})
        assert registry.promotion_gates(USE_CASE, MODEL) == {}
        registry.promote(USE_CASE, MODEL, later, ModelStage.PRODUCTION)
        assert self._stage(registry, later) == "production"

    def test_a_model_with_no_versions_has_no_gates(self, registry: ModelRegistry) -> None:
        assert registry.promotion_gates(USE_CASE, "NoSuchModel") == {}

    def test_only_one_version_is_in_production(self, registry: ModelRegistry) -> None:
        first = self._register(registry)
        second = self._register(registry)
        registry.promote(USE_CASE, MODEL, first, ModelStage.PRODUCTION)
        registry.promote(USE_CASE, MODEL, second, ModelStage.PRODUCTION)
        assert self._stage(registry, second) == "production"
        assert self._stage(registry, first) == "archived"

    # -- moving back ------------------------------------------------------------

    def test_demote(self, registry: ModelRegistry) -> None:
        version = self._register(registry)
        registry.promote(USE_CASE, MODEL, version, ModelStage.PRODUCTION)
        mv = registry.demote(USE_CASE, MODEL, version, ModelStage.STAGING)
        assert mv.stage == ModelStage.STAGING
        assert self._stage(registry, version) == "staging"

    def test_demote_cannot_reach_production(self, registry: ModelRegistry) -> None:
        """Demote moves a version down only, so it cannot skip the promotion gates."""
        version = self._register(registry, auc=0.5, promotion_gates={"min_auc": 0.8})
        for stage in (ModelStage.PRODUCTION, ModelStage.STAGING):
            with pytest.raises(RegistryError):
                registry.demote(USE_CASE, MODEL, version, stage)
        assert self._stage(registry, version) == "development"

    def test_the_backend_demote_cannot_reach_production(self, backend: Any) -> None:
        backend.register(
            use_case=USE_CASE,
            model_name=MODEL,
            version="1.0.0",
            model=ConformanceModel(),
            metrics={"auc": 0.5},
        )
        with pytest.raises(RegistryError):
            backend.demote(
                use_case=USE_CASE, model_name=MODEL, version="1.0.0", to_stage="production"
            )
        _, info = backend.get(use_case=USE_CASE, model_name=MODEL, version="1.0.0")
        assert info.stage == "development"

    def test_archive(self, registry: ModelRegistry) -> None:
        version = self._register(registry)
        registry.archive(USE_CASE, MODEL, version)
        assert self._stage(registry, version) == "archived"

    def test_rollback_restores_an_older_version_to_production(
        self, registry: ModelRegistry
    ) -> None:
        first = self._register(registry)
        second = self._register(registry)
        registry.promote(USE_CASE, MODEL, first, ModelStage.PRODUCTION)
        registry.promote(USE_CASE, MODEL, second, ModelStage.PRODUCTION)
        mv = registry.rollback(USE_CASE, MODEL, first)
        assert mv.version == first and mv.stage == ModelStage.PRODUCTION
        assert self._stage(registry, first) == "production"
        assert self._stage(registry, second) == "archived"

    def test_rollback_keeps_who_promoted_the_version(self, registry: ModelRegistry) -> None:
        first = self._register(registry)
        second = self._register(registry)
        mv = registry.promote(USE_CASE, MODEL, first, ModelStage.PRODUCTION, promoted_by="ann")
        if mv.promoted_by != "ann":
            pytest.skip("This backend does not record who promoted a version.")
        registry.promote(USE_CASE, MODEL, second, ModelStage.PRODUCTION, promoted_by="bo")
        assert registry.rollback(USE_CASE, MODEL, first).promoted_by == "ann"
        _, mv = registry.get_model(USE_CASE, MODEL, version=first)
        assert mv.promoted_by == "ann"

    def test_compare_gives_the_metric_deltas(self, registry: ModelRegistry) -> None:
        a = self._register(registry, auc=0.8)
        b = self._register(registry, auc=0.9)
        diff = registry.compare_versions(USE_CASE, MODEL, a, b)
        assert diff["auc"]["a"] == 0.8 and diff["auc"]["b"] == 0.9
        assert diff["auc"]["delta"] == pytest.approx(0.1)
