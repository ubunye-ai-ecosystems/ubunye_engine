"""Unit tests for ModelRegistry.

All tests are Spark-free. DummyModel writes a simple JSON file as its artifact.
A real filesystem (tmp_path) is used so CRUD operations are verified end-to-end.
"""
import json
from pathlib import Path
from typing import Any, Dict

import pytest

from ubunye.models.base import UbunyeModel
from ubunye.models.registry import ModelRegistry, ModelStage, ModelVersion


# ---------------------------------------------------------------------------
# DummyModel
# ---------------------------------------------------------------------------

class DummyModel(UbunyeModel):
    def __init__(self):
        self._trained = False

    def train(self, df: Any) -> Dict[str, Any]:
        self._trained = True
        return {"auc": 0.90, "f1": 0.87}

    def predict(self, df: Any) -> Any:
        return df

    def save(self, path: str) -> None:
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        (p / "model.json").write_text(
            json.dumps({"trained": self._trained}), encoding="utf-8"
        )

    @classmethod
    def load(cls, path: str) -> "DummyModel":
        m = cls()
        m._trained = json.loads((Path(path) / "model.json").read_text())["trained"]
        return m

    def metadata(self) -> Dict[str, Any]:
        return {
            "library": "dummy",
            "library_version": "1.0.0",
            "features": ["x1", "x2"],
            "params": {},
        }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def registry(tmp_path) -> ModelRegistry:
    return ModelRegistry(str(tmp_path / "model_store"))


@pytest.fixture
def model() -> DummyModel:
    m = DummyModel()
    m._trained = True
    return m


# ---------------------------------------------------------------------------
# register
# ---------------------------------------------------------------------------

class TestRegister:

    def test_register_creates_development_version(self, registry, model):
        mv = registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.9})
        assert mv.version == "1.0.0"
        assert mv.stage == ModelStage.DEVELOPMENT

    def test_register_saves_model_artifacts(self, registry, model, tmp_path):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.9})
        model_dir = (
            tmp_path / "model_store" / "fraud" / "RiskModel" / "versions" / "1.0.0"
        )
        assert (model_dir / "model" / "model.json").exists()
        assert (model_dir / "metadata.json").exists()
        assert (model_dir / "metrics.json").exists()

    def test_register_writes_registry_json(self, registry, model, tmp_path):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.9})
        registry_file = tmp_path / "model_store" / "fraud" / "RiskModel" / "registry.json"
        assert registry_file.exists()
        data = json.loads(registry_file.read_text())
        assert "1.0.0" in data["versions"]

    def test_register_stores_metrics(self, registry, model):
        mv = registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.92, "f1": 0.88})
        assert mv.metrics["auc"] == 0.92

    def test_register_stores_lineage_run_id(self, registry, model):
        mv = registry.register("fraud", "RiskModel", "1.0.0", model, {}, lineage_run_id="run-42")
        assert mv.lineage_run_id == "run-42"

    def test_duplicate_version_raises(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        with pytest.raises(ValueError, match="already exists"):
            registry.register("fraud", "RiskModel", "1.0.0", model, {})

    def test_auto_version_generates_semver(self, registry, model):
        v1 = registry.register("fraud", "RiskModel", None, model, {})
        v2 = registry.register("fraud", "RiskModel", None, model, {})
        assert v1.version != v2.version
        # Auto-versions should be parseable semver
        parts = [int(x) for x in v1.version.split(".")]
        assert len(parts) == 3

    def test_auto_version_bumps_patch(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        v2 = registry.register("fraud", "RiskModel", None, model, {})
        assert v2.version == "1.0.1"


# ---------------------------------------------------------------------------
# promote
# ---------------------------------------------------------------------------

class TestPromote:

    def test_promote_to_staging(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.9})
        mv = registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.STAGING)
        assert mv.stage == ModelStage.STAGING

    def test_promote_sets_staging_timestamp(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.9})
        mv = registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.STAGING)
        assert mv.promoted_to_staging is not None

    def test_promote_to_production(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.9})
        mv = registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.PRODUCTION)
        assert mv.stage == ModelStage.PRODUCTION

    def test_promote_to_production_archives_previous(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.9})
        registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.PRODUCTION)
        registry.register("fraud", "RiskModel", "1.1.0", model, {"auc": 0.95})
        registry.promote("fraud", "RiskModel", "1.1.0", ModelStage.PRODUCTION)

        versions = {v.version: v for v in registry.list_versions("fraud", "RiskModel")}
        assert versions["1.0.0"].stage == ModelStage.ARCHIVED
        assert versions["1.1.0"].stage == ModelStage.PRODUCTION

    def test_promote_with_passing_gates_succeeds(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.92})
        mv = registry.promote(
            "fraud", "RiskModel", "1.0.0", ModelStage.STAGING,
            gates={"min_auc": 0.85}
        )
        assert mv.stage == ModelStage.STAGING

    def test_promote_with_failing_gates_raises(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.70})
        with pytest.raises(ValueError, match="gate"):
            registry.promote(
                "fraud", "RiskModel", "1.0.0", ModelStage.STAGING,
                gates={"min_auc": 0.85}
            )

    def test_promote_nonexistent_version_raises(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        with pytest.raises(ValueError, match="not found"):
            registry.promote("fraud", "RiskModel", "9.9.9", ModelStage.STAGING)

    def test_promote_sets_promoted_by(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        mv = registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.STAGING, promoted_by="alice")
        assert mv.promoted_by == "alice"


# ---------------------------------------------------------------------------
# demote / archive
# ---------------------------------------------------------------------------

class TestDemoteArchive:

    def test_demote_changes_stage(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.STAGING)
        mv = registry.demote("fraud", "RiskModel", "1.0.0", ModelStage.DEVELOPMENT)
        assert mv.stage == ModelStage.DEVELOPMENT

    def test_archive_sets_archived_stage(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        mv = registry.archive("fraud", "RiskModel", "1.0.0")
        assert mv.stage == ModelStage.ARCHIVED


# ---------------------------------------------------------------------------
# rollback
# ---------------------------------------------------------------------------

class TestRollback:

    def test_rollback_restores_production(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.PRODUCTION)
        registry.register("fraud", "RiskModel", "1.1.0", model, {})
        registry.promote("fraud", "RiskModel", "1.1.0", ModelStage.PRODUCTION)

        registry.rollback("fraud", "RiskModel", "1.0.0")

        versions = {v.version: v for v in registry.list_versions("fraud", "RiskModel")}
        assert versions["1.0.0"].stage == ModelStage.PRODUCTION

    def test_rollback_archives_current_production(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.PRODUCTION)
        registry.register("fraud", "RiskModel", "1.1.0", model, {})
        registry.promote("fraud", "RiskModel", "1.1.0", ModelStage.PRODUCTION)

        registry.rollback("fraud", "RiskModel", "1.0.0")

        versions = {v.version: v for v in registry.list_versions("fraud", "RiskModel")}
        assert versions["1.1.0"].stage == ModelStage.ARCHIVED


# ---------------------------------------------------------------------------
# list / compare
# ---------------------------------------------------------------------------

class TestListAndCompare:

    def test_list_versions_returns_all(self, registry, model):
        for v in ["1.0.0", "1.1.0", "1.2.0"]:
            registry.register("fraud", "RiskModel", v, model, {})
        versions = registry.list_versions("fraud", "RiskModel")
        assert len(versions) == 3

    def test_list_versions_newest_first(self, registry, model):
        for v in ["1.0.0", "1.1.0"]:
            registry.register("fraud", "RiskModel", v, model, {})
        versions = registry.list_versions("fraud", "RiskModel")
        assert versions[0].version == "1.1.0"

    def test_list_nonexistent_model_raises(self, registry):
        with pytest.raises(FileNotFoundError):
            registry.list_versions("fraud", "NonExistentModel")

    def test_compare_versions_shows_delta(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.85, "f1": 0.80})
        registry.register("fraud", "RiskModel", "1.1.0", model, {"auc": 0.90, "f1": 0.87})
        diff = registry.compare_versions("fraud", "RiskModel", "1.0.0", "1.1.0")
        assert "auc" in diff
        assert diff["auc"]["a"] == 0.85
        assert diff["auc"]["b"] == 0.90
        assert abs(diff["auc"]["delta"] - 0.05) < 1e-6

    def test_compare_versions_includes_all_metrics(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {"auc": 0.85})
        registry.register("fraud", "RiskModel", "1.1.0", model, {"auc": 0.90, "f1": 0.87})
        diff = registry.compare_versions("fraud", "RiskModel", "1.0.0", "1.1.0")
        assert "f1" in diff
        assert diff["f1"]["a"] is None  # not present in 1.0.0


# ---------------------------------------------------------------------------
# get_model
# ---------------------------------------------------------------------------

class TestGetModel:

    def test_get_model_by_version(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        path, mv = registry.get_model("fraud", "RiskModel", version="1.0.0")
        assert mv.version == "1.0.0"
        assert "model" in path

    def test_get_model_by_stage(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        registry.promote("fraud", "RiskModel", "1.0.0", ModelStage.PRODUCTION)
        path, mv = registry.get_model("fraud", "RiskModel", stage=ModelStage.PRODUCTION)
        assert mv.stage == ModelStage.PRODUCTION

    def test_get_model_missing_stage_raises(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        with pytest.raises(ValueError, match="stage"):
            registry.get_model("fraud", "RiskModel", stage=ModelStage.PRODUCTION)

    def test_get_model_no_version_or_stage_raises(self, registry, model):
        registry.register("fraud", "RiskModel", "1.0.0", model, {})
        with pytest.raises(ValueError):
            registry.get_model("fraud", "RiskModel")
