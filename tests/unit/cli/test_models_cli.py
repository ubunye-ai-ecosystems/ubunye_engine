"""`ubunye models promote/demote`: demote never moves a version up (F-091)."""

import json
import os

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.models.base import UbunyeModel
from ubunye.models.registry import ModelRegistry

try:  # Click before 8.2 mixes stderr into stdout unless asked not to
    runner = CliRunner(mix_stderr=False)
except TypeError:  # Click 8.2+ always keeps them apart
    runner = CliRunner()


class _Toy(UbunyeModel):
    def __init__(self, w=1):
        self.w = w

    def train(self, df):
        return {"auc": 0.8}

    def predict(self, df):
        return df

    def save(self, path):
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "m.json"), "w") as fh:
            json.dump({"w": self.w}, fh)

    @classmethod
    def load(cls, path):
        with open(os.path.join(path, "m.json")) as fh:
            return cls(json.load(fh)["w"])

    def metadata(self):
        return {"w": self.w}


@pytest.fixture
def store(tmp_path):
    path = str(tmp_path / "store")
    reg = ModelRegistry(path)
    reg.register("demo", "Toy", "1.0.0", _Toy(), {"auc": 0.80}, promotion_gates={"min_auc": 0.85})
    return path


def _stage(store):
    return ModelRegistry(store).list_versions("demo", "Toy")[0].stage.value


def _args(cmd, store, to):
    return ["models", cmd, "-u", "demo", "-m", "Toy", "-s", store, "-v", "1.0.0", "--to", to]


def test_promote_to_production_is_blocked_by_the_gate(store):
    result = runner.invoke(app, _args("promote", store, "production"))
    assert result.exit_code == 1
    assert _stage(store) == "development"


@pytest.mark.parametrize("to", ["production", "staging"])
def test_demote_cannot_move_a_version_up(store, to):
    result = runner.invoke(app, _args("demote", store, to))
    assert result.exit_code != 0
    assert "[OK]" not in result.stdout
    assert _stage(store) == "development"


def test_demote_to_production_is_a_usage_error_listing_the_choices(store):
    result = runner.invoke(app, _args("demote", store, "production"))
    assert result.exit_code == 2
    for name in ("development", "staging", "archived"):
        assert name in result.stderr


def test_promote_only_takes_staging_or_production(store):
    result = runner.invoke(app, _args("promote", store, "development"))
    assert result.exit_code == 2
    assert "staging" in result.stderr and "production" in result.stderr
    assert _stage(store) == "development"


def test_demote_to_archived_still_works(store):
    result = runner.invoke(app, _args("demote", store, "archived"))
    assert result.exit_code == 0, result.stderr
    assert _stage(store) == "archived"
