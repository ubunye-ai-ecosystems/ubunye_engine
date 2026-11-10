"""Every shipped registry backend, and one written by a stranger, passes the same suite."""

from __future__ import annotations

import pytest

from ubunye.models.backends import resolve_registry
from ubunye.models.mlflow_registry import MLflowRegistryBackend
from ubunye.models.registry import FilesystemRegistryBackend, ModelRegistry
from ubunye.testing.registry_conformance import RegistryConformance


class TestFilesystemBackend(RegistryConformance):
    @pytest.fixture
    def backend(self, tmp_path):
        return FilesystemRegistryBackend(str(tmp_path / "store"))


class TestFilesystemDirect(RegistryConformance):
    """The default path: ModelRegistry(store) with no backend named, exactly as before."""

    @pytest.fixture
    def backend(self, tmp_path):
        return FilesystemRegistryBackend(str(tmp_path / "store"))

    @pytest.fixture
    def registry(self, backend):
        return ModelRegistry(backend._store_path)


class TestMLflowBackend(RegistryConformance):
    @pytest.fixture
    def backend(self, tmp_path, monkeypatch):
        # MLflow logging is best effort; keep it off the network and out of ./mlruns.
        monkeypatch.setattr(
            "ubunye.models.mlflow_registry._try_log_to_mlflow", lambda **kwargs: None
        )
        return MLflowRegistryBackend(str(tmp_path / "store"))


class TestThirdPartyBackend(RegistryConformance):
    """Found by name through its entry point: no edit to Ubunye."""

    @pytest.fixture
    def backend(self, tmp_path, thirdparty_installed):
        return resolve_registry(thirdparty_installed, store=str(tmp_path / "store"))
