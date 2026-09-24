"""A model's promotion gates hold however it is promoted.

Gates were read from the training task's config and applied only when the
training run promoted the model itself; `ubunye models promote` never saw them,
so a model that failed every gate could be promoted by hand. The registry now
keeps a model's gates, and every promotion checks them unless it is forced, and
a forced promotion is marked on the version.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.core.errors import PromotionBlockedError
from ubunye.models.registry import ModelRegistry, ModelStage
from unit.models.test_registry import DummyModel

runner = CliRunner()
GATES = {"min_auc": 0.85}


@pytest.fixture
def store(tmp_path):
    return str(tmp_path / "store")


def _register(store, version, auc, gates=GATES):
    return ModelRegistry(store).register(
        "uc", "M", version, DummyModel(), {"auc": auc}, promotion_gates=gates
    )


def _promote(store, version, *extra):
    return runner.invoke(
        app,
        [
            "models",
            "promote",
            "-u",
            "uc",
            "-m",
            "M",
            "-s",
            store,
            "-v",
            version,
            "--to",
            "production",
            *extra,
        ],
    )


class TestTheRegistryKeepsTheGates:
    def test_a_failing_model_is_blocked_without_passing_gates_again(self, store):
        _register(store, "1.0.0", auc=0.5)
        with pytest.raises(PromotionBlockedError, match="min_auc"):
            ModelRegistry(store).promote("uc", "M", "1.0.0", ModelStage.PRODUCTION)

    def test_a_passing_model_is_promoted(self, store):
        _register(store, "1.0.0", auc=0.9)
        mv = ModelRegistry(store).promote("uc", "M", "1.0.0", ModelStage.PRODUCTION)
        assert mv.stage == ModelStage.PRODUCTION

    def test_explicit_gates_override_the_stored_ones(self, store):
        _register(store, "1.0.0", auc=0.5)
        mv = ModelRegistry(store).promote(
            "uc", "M", "1.0.0", ModelStage.STAGING, gates={"min_auc": 0.4}
        )
        assert mv.stage == ModelStage.STAGING

    def test_a_model_without_gates_promotes_as_before(self, store):
        _register(store, "1.0.0", auc=0.1, gates=None)
        mv = ModelRegistry(store).promote("uc", "M", "1.0.0", ModelStage.PRODUCTION)
        assert mv.stage == ModelStage.PRODUCTION

    def test_force_skips_the_gates_and_says_so_on_the_version(self, store):
        _register(store, "1.0.0", auc=0.5)
        mv = ModelRegistry(store).promote(
            "uc", "M", "1.0.0", ModelStage.PRODUCTION, force=True, promoted_by="thabang"
        )
        assert mv.stage == ModelStage.PRODUCTION
        assert mv.metadata["promotion_forced"] is True
        assert mv.metadata["promotion_gates_skipped"] == GATES

    def test_registries_written_before_gates_were_kept_still_load(self, store, tmp_path):
        import json

        _register(store, "1.0.0", auc=0.9, gates=None)
        (path,) = list((tmp_path / "store").rglob("registry.json"))
        data = json.loads(path.read_text(encoding="utf-8"))
        data.pop("promotion_gates", None)
        path.write_text(json.dumps(data), encoding="utf-8")
        assert ModelRegistry(store).list_versions("uc", "M")[0].version == "1.0.0"


class TestTheCli:
    def test_promote_is_blocked_by_the_stored_gates(self, store):
        _register(store, "1.0.0", auc=0.5)
        result = _promote(store, "1.0.0")
        assert result.exit_code == 1
        assert "min_auc" in result.output

    def test_force_promotes_with_a_warning(self, store):
        _register(store, "1.0.0", auc=0.5)
        result = _promote(store, "1.0.0", "--force")
        assert result.exit_code == 0, result.output
        assert "without checking" in result.output and "min_auc" in result.output

    def test_a_passing_model_reports_its_gates(self, store):
        _register(store, "1.0.0", auc=0.9)
        result = _promote(store, "1.0.0")
        assert result.exit_code == 0, result.output
        assert "1 gate(s) passed" in result.output
