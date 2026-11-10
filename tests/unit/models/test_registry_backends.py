"""Registry backends are found by name and chosen in one order (ADR 010).

explicit (CLI flag, config key, kwarg) > UBUNYE_REGISTRY_BACKEND > filesystem.
Every place that opens a registry goes through that order: the CLI, the model
transform and ``ModelRegistry`` in Python.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.models.backends import (
    RegistryBackendNotFoundError,
    available,
    choose,
    resolve_registry,
)
from ubunye.models.registry import FilesystemRegistryBackend, ModelRegistry, ModelStage
from ubunye.plugins.transforms.model_transform import ModelTransform
from ubunye.testing.registry_conformance import ConformanceModel

runner = CliRunner()
ANSI = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
    monkeypatch.delenv("UBUNYE_REGISTRY_BACKEND", raising=False)


def _is_thirdparty_store(store: Path) -> bool:
    return (store / "uc" / "ConformanceModel" / "index.json").exists()


def _is_filesystem_store(store: Path) -> bool:
    return (store / "uc" / "ConformanceModel" / "registry.json").exists()


# -- the order ------------------------------------------------------------------


def test_the_default_is_filesystem():
    assert choose() == "filesystem"


def test_the_env_var_beats_the_default(monkeypatch):
    monkeypatch.setenv("UBUNYE_REGISTRY_BACKEND", "MLflow ")
    assert choose() == "mlflow"


def test_an_explicit_name_beats_the_env_var(monkeypatch):
    monkeypatch.setenv("UBUNYE_REGISTRY_BACKEND", "mlflow")
    assert choose("filesystem") == "filesystem"


def test_the_shipped_backends_are_registered():
    assert {"filesystem", "mlflow"} <= set(available())


def test_the_store_reaches_the_backend_unchanged(tmp_path):
    backend = resolve_registry("filesystem", store=str(tmp_path / "s"))
    assert isinstance(backend, FilesystemRegistryBackend)
    assert backend._store_path == str(tmp_path / "s")


def test_an_unknown_name_says_what_is_installed_and_how_to_add_one(tmp_path):
    with pytest.raises(RegistryBackendNotFoundError) as err:
        resolve_registry("nope", store=str(tmp_path))
    text = str(err.value)
    assert "nope" in text and "filesystem" in text and "mlflow" in text
    assert "ubunye.registry_backends" in text and "UBUNYE_REGISTRY_BACKEND" in text


# -- ModelRegistry in Python ----------------------------------------------------


def test_model_registry_with_no_backend_is_the_filesystem_as_before(tmp_path):
    ModelRegistry(str(tmp_path)).register("uc", "ConformanceModel", None, ConformanceModel(), {})
    assert _is_filesystem_store(tmp_path)


def test_model_registry_follows_the_env_var(tmp_path, monkeypatch, thirdparty_installed):
    monkeypatch.setenv("UBUNYE_REGISTRY_BACKEND", thirdparty_installed)
    ModelRegistry(str(tmp_path)).register("uc", "ConformanceModel", None, ConformanceModel(), {})
    assert _is_thirdparty_store(tmp_path) and not _is_filesystem_store(tmp_path)


def test_a_named_backend_beats_the_env_var(tmp_path, monkeypatch, thirdparty_installed):
    monkeypatch.setenv("UBUNYE_REGISTRY_BACKEND", thirdparty_installed)
    registry = ModelRegistry(str(tmp_path), backend="filesystem")
    registry.register("uc", "ConformanceModel", None, ConformanceModel(), {})
    assert _is_filesystem_store(tmp_path)


def test_the_filesystem_backend_ignores_the_env_var(tmp_path, monkeypatch, thirdparty_installed):
    """FilesystemRegistryBackend IS the filesystem; an env var must not redirect it."""
    monkeypatch.setenv("UBUNYE_REGISTRY_BACKEND", thirdparty_installed)
    FilesystemRegistryBackend(str(tmp_path)).register(
        use_case="uc",
        model_name="ConformanceModel",
        version=None,
        model=ConformanceModel(),
        metrics={},
    )
    assert _is_filesystem_store(tmp_path)


# -- the CLI ----------------------------------------------------------------------


def _cli(*args):
    result = runner.invoke(app, ["models", *args])
    return result.exit_code, ANSI.sub("", result.output)


def test_cli_models_takes_a_backend(tmp_path, thirdparty_installed):
    store = str(tmp_path)
    registry = ModelRegistry(store, backend=thirdparty_installed)
    registry.register(
        "uc",
        "ConformanceModel",
        None,
        ConformanceModel(),
        {"auc": 0.5},
        promotion_gates={"min_auc": 0.8},
    )
    common = ["-u", "uc", "-m", "ConformanceModel", "-s", store, "--backend", thirdparty_installed]

    code, out = _cli("list", *common, "--json")
    assert code == 0, out
    assert [v["version"] for v in json.loads(out)] == ["1.0.0"]

    code, out = _cli("promote", *common, "-v", "1.0.0", "--to", "production")
    assert code == 1 and "min_auc" in out

    code, out = _cli("promote", *common, "-v", "1.0.0", "--to", "production", "--force")
    assert code == 0, out
    code, out = _cli("info", *common, "-v", "1.0.0")
    assert code == 0 and json.loads(out)["stage"] == "production"
    assert _is_thirdparty_store(tmp_path) and not _is_filesystem_store(tmp_path)


def test_cli_models_short_flag_and_env_var(tmp_path, monkeypatch, thirdparty_installed):
    store = str(tmp_path)
    ModelRegistry(store, backend=thirdparty_installed).register(
        "uc", "ConformanceModel", "1.0.0", ConformanceModel(), {}
    )
    base = ["-u", "uc", "-m", "ConformanceModel", "-s", store, "--json"]
    assert _cli("list", *base, "-b", thirdparty_installed)[0] == 0
    monkeypatch.setenv("UBUNYE_REGISTRY_BACKEND", thirdparty_installed)
    code, out = _cli("list", *base)
    assert code == 0 and json.loads(out)[0]["version"] == "1.0.0"


def test_cli_models_unknown_backend_fails_clearly(tmp_path):
    code, out = _cli("list", "-u", "uc", "-m", "M", "-s", str(tmp_path), "-b", "nope")
    assert code == 1
    assert "nope" in out and "filesystem" in out


# -- the model transform ----------------------------------------------------------


def _transform_cfg(store: Path, action: str, **registry):
    return {
        "action": action,
        "model_class": "ConformanceModel",
        "registry": {"store": str(store), "use_case": "uc", **registry},
    }


@pytest.fixture
def model_class(monkeypatch):
    monkeypatch.setattr(
        "ubunye.plugins.transforms.model_transform.load_model_class",
        lambda model_dir, name: ConformanceModel,
    )


def test_the_model_transform_uses_the_configured_backend(
    tmp_path, model_class, thirdparty_installed
):
    cfg = _transform_cfg(tmp_path, "train", backend=thirdparty_installed, promote_to="production")
    ModelTransform().apply({"df": object()}, cfg, backend=object())
    assert _is_thirdparty_store(tmp_path) and not _is_filesystem_store(tmp_path)

    cfg = _transform_cfg(tmp_path, "predict", backend=thirdparty_installed)
    out = ModelTransform().apply({"df": "rows"}, cfg, backend=object())
    assert out == {"predictions": "rows"}
    _, mv = ModelRegistry(str(tmp_path), backend=thirdparty_installed).get_model(
        "uc", "ConformanceModel", stage=ModelStage.PRODUCTION
    )
    assert mv.version == "1.0.0"


def test_the_model_transform_defaults_to_the_filesystem(tmp_path, model_class):
    ModelTransform().apply({"df": object()}, _transform_cfg(tmp_path, "train"), backend=object())
    assert _is_filesystem_store(tmp_path)


def test_the_config_schema_accepts_a_registry_backend():
    from ubunye.config.schema import ModelTransformParams

    params = ModelTransformParams(
        action="train",
        model_class="model.M",
        registry={"store": "s", "backend": "mlflow"},
    )
    assert params.registry is not None and params.registry.backend == "mlflow"
