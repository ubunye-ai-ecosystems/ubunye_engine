"""The MLflow registry backend keeps models in the MLflow Model Registry (ADR 014).

Runs against a local MLflow: a SQLite tracking store and a file artifact store in
tmp_path. No server, no network. The MLflow stage API is forbidden in every test,
so the backend works where only aliases exist (Unity Catalog).
"""

from __future__ import annotations

import shutil
import warnings

import pytest

pytest.importorskip("mlflow")

from mlflow import MlflowClient  # noqa: E402

from ubunye.interfaces.registry import RegistryBackend  # noqa: E402
from ubunye.models.backends import resolve_registry  # noqa: E402
from ubunye.models.mlflow_registry import (  # noqa: E402
    MLflowModelRegistryBackend,
    MLflowRegistryBackend,
)
from ubunye.models.registry import ModelRegistry, ModelStage  # noqa: E402
from ubunye.testing.registry_conformance import (  # noqa: E402
    MODEL,
    USE_CASE,
    ConformanceModel,
    RegistryConformance,
)

EXPERIMENT = "ubunye-models"
NAME = f"{USE_CASE}.{MODEL}"


def _forbidden(*args, **kwargs):
    raise AssertionError("MLflow stages are deprecated and Unity Catalog has none: use aliases.")


@pytest.fixture(scope="module")
def template_db(tmp_path_factory):
    """One migrated SQLite store, copied for each test (creating one takes seconds)."""
    path = tmp_path_factory.mktemp("mlflow") / "template.db"
    MlflowClient(tracking_uri=f"sqlite:///{path}").search_experiments()
    return path


@pytest.fixture
def local_mlflow(tmp_path, monkeypatch, template_db):
    """A fresh local MLflow, found the way MLflow finds it: MLFLOW_TRACKING_URI."""
    db = tmp_path / "mlflow.db"
    shutil.copy(template_db, db)
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{db}")
    monkeypatch.delenv("MLFLOW_REGISTRY_URI", raising=False)
    monkeypatch.delenv("UBUNYE_MLFLOW_MODEL_NAME", raising=False)
    client = MlflowClient()
    client.create_experiment(EXPERIMENT, artifact_location=(tmp_path / "artifacts").as_uri())
    monkeypatch.setattr(MlflowClient, "transition_model_version_stage", _forbidden)
    monkeypatch.setattr(MlflowClient, "get_latest_versions", _forbidden)
    return client


class TestMLflowModelRegistry(RegistryConformance):
    @pytest.fixture
    def backend(self, local_mlflow):
        return MLflowModelRegistryBackend(EXPERIMENT)


@pytest.fixture
def registry(local_mlflow):
    return ModelRegistry(EXPERIMENT, backend="mlflow")


def _register(registry, version, **kwargs):
    return registry.register(
        USE_CASE, MODEL, version, ConformanceModel(version), {"auc": 0.9}, **kwargs
    )


def test_the_mlflow_name_finds_the_model_registry_backend(local_mlflow):
    backend = resolve_registry("mlflow", store=EXPERIMENT)
    assert isinstance(backend, MLflowModelRegistryBackend)
    assert isinstance(backend, RegistryBackend)


def test_a_version_is_an_mlflow_registered_model_version(registry, local_mlflow):
    _register(registry, "1.0.0", lineage_run_id="run-1")
    versions = local_mlflow.search_model_versions(f"name='{NAME}'")
    assert len(versions) == 1
    tags = versions[0].tags
    assert tags["ubunye.version"] == "1.0.0"
    assert tags["ubunye.stage"] == "development"
    assert tags["ubunye.metric.auc"] == "0.9"
    assert tags["ubunye.meta.lineage_run_id"] == "run-1"
    # The artifact sits with the MLflow run the version points at.
    assert versions[0].run_id


def test_stages_are_mlflow_aliases(registry, local_mlflow):
    _register(registry, "1.0.0")
    _register(registry, "1.0.1")
    registry.promote(USE_CASE, MODEL, "1.0.0", ModelStage.PRODUCTION)
    registry.promote(USE_CASE, MODEL, "1.0.1", ModelStage.STAGING)

    def alias(name):
        mv = local_mlflow.get_model_version_by_alias(NAME, name)
        return mv.tags["ubunye.version"]

    assert alias("production") == "1.0.0"
    assert alias("staging") == "1.0.1"

    # A new production version moves the alias; the old one is archived.
    registry.promote(USE_CASE, MODEL, "1.0.1", ModelStage.PRODUCTION)
    assert alias("production") == "1.0.1"
    assert set(local_mlflow.get_registered_model(NAME).aliases) == {"production"}
    _, old = registry.get_model(USE_CASE, MODEL, version="1.0.0")
    assert old.stage == ModelStage.ARCHIVED


def test_gates_and_a_forced_promotion_are_version_tags(registry, local_mlflow):
    _register(registry, "1.0.0", promotion_gates={"min_auc": 0.95})
    registry.promote(USE_CASE, MODEL, "1.0.0", ModelStage.PRODUCTION, force=True)
    (mv,) = local_mlflow.search_model_versions(f"name='{NAME}'")
    assert mv.tags["ubunye.meta.promotion_gates"] == '{"min_auc": 0.95}'
    assert mv.tags["ubunye.meta.promotion_forced"] == "true"


def test_the_model_name_can_follow_a_template(local_mlflow, monkeypatch):
    # Unity Catalog wants catalog.schema.model; the template gives it.
    monkeypatch.setenv("UBUNYE_MLFLOW_MODEL_NAME", "main_{use_case}_{model_name}")
    registry = ModelRegistry(EXPERIMENT, backend="mlflow")
    _register(registry, "1.0.0")
    assert local_mlflow.get_registered_model(f"main_{USE_CASE}_{MODEL}")


def test_a_backend_metadata_round_trips_without_the_lifecycle(local_mlflow):
    backend = MLflowModelRegistryBackend(EXPERIMENT)
    info = backend.register(
        use_case=USE_CASE,
        model_name=MODEL,
        version="1.0.0",
        model=ConformanceModel("x"),
        metrics={"auc": 0.9, "report": {"rows": 3}},
        metadata={"experiment": "exp-42"},
    )
    assert info.metadata["experiment"] == "exp-42"
    _, again = backend.get(use_case=USE_CASE, model_name=MODEL, version="1.0.0")
    assert again.metrics == {"auc": 0.9, "report": {"rows": 3}}
    assert again.metadata["experiment"] == "exp-42"
    assert again.metadata["library"] == "conformance"  # the model's own metadata()


def test_the_old_file_backend_still_works_and_says_it_is_deprecated(tmp_path, monkeypatch):
    monkeypatch.setattr("ubunye.models.mlflow_registry._try_log_to_mlflow", lambda **kw: None)
    with pytest.warns(DeprecationWarning, match="MLflowModelRegistryBackend"):
        backend = MLflowRegistryBackend(str(tmp_path / "store"))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        info = backend.register(
            use_case=USE_CASE,
            model_name=MODEL,
            version="1.0.0",
            model=ConformanceModel(),
            metrics={},
        )
    assert info.version == "1.0.0"
