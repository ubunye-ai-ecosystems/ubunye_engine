"""The registry writes wherever the store says. It never assumes a disk.

Three proofs, matching the framework's philosophy:

1. A DUCK-TYPED store (no base class, no registration) drives the complete model
   lifecycle. The contract is structural, like every port in this engine.
2. The same registry runs against real object-storage semantics (fsspec's memory://
   filesystem) end to end: register, promote, download, load.
3. The scheme picks the store, and a plugin registered under ubunye.artifact_stores
   beats the generic fallback, so nothing is hardcoded anywhere.
"""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest

from ubunye.models.artifact_store import (
    FsspecArtifactStore,
    LocalArtifactStore,
    resolve_store,
)
from ubunye.models.registry import ModelRegistry, ModelStage


class FileWritingModel:
    """The smallest honest UbunyeModel: its save() writes a real file, like they all do."""

    def train(self, df: Any) -> Dict[str, float]:
        return {"train_score": 1.0}

    def predict(self, df: Any) -> Any:  # pragma: no cover
        return df

    def save(self, path: str) -> None:
        Path(path, "weights.txt").write_text("the actual artifact", encoding="utf-8")

    @classmethod
    def load(cls, path: str) -> "FileWritingModel":
        assert Path(path, "weights.txt").read_text(encoding="utf-8") == "the actual artifact"
        return cls()

    def metadata(self) -> Dict[str, Any]:
        return {"framework": "test"}


class DuckStore:
    """In-memory storage. Inherits nothing. Registered nowhere. Still a store."""

    def __init__(self) -> None:
        self.blobs: Dict[str, str] = {}

    def join(self, *parts: str) -> str:
        return "/".join(p.strip("/") for p in parts)

    def exists(self, path: str) -> bool:
        return path in self.blobs or any(k.startswith(path + "/") for k in self.blobs)

    def read_text(self, path: str) -> str:
        return self.blobs[path]

    def write_text(self, path: str, text: str) -> None:
        self.blobs[path] = text

    def delete(self, path: str) -> None:
        for key in [k for k in self.blobs if k == path or k.startswith(path + "/")]:
            del self.blobs[key]

    @contextlib.contextmanager
    def staging_dir(self, target: str):
        import tempfile

        tmp = tempfile.mkdtemp()
        yield tmp
        for name in os.listdir(tmp):
            self.blobs[self.join(target, name)] = Path(tmp, name).read_text(encoding="utf-8")

    def materialize(self, path: str) -> str:
        import tempfile

        local = tempfile.mkdtemp()
        for key, value in self.blobs.items():
            if key.startswith(path + "/"):
                Path(local, key.split("/")[-1]).write_text(value, encoding="utf-8")
        return local


def test_a_duck_typed_store_runs_the_whole_lifecycle():
    """No inheritance, no entry point, no disk. Register, promote, load, roll back."""
    duck = DuckStore()
    registry = ModelRegistry("duck-bucket/models")
    registry._store = duck  # what resolve_store would do for a registered scheme

    v1 = registry.register(
        use_case="uc",
        model_name="M",
        version=None,
        model=FileWritingModel(),
        metrics={"test_mae": 1.0},
    )
    registry.promote(
        use_case="uc", model_name="M", version=v1.version, to_stage=ModelStage.PRODUCTION
    )

    local_path, mv = registry.get_model(use_case="uc", model_name="M", stage=ModelStage.PRODUCTION)
    loaded = FileWritingModel.load(local_path)  # the artifact came back intact
    assert isinstance(loaded, FileWritingModel)
    assert mv.version == v1.version

    # the registry record itself lives in the duck, not on any disk
    assert any(k.endswith("registry.json") for k in duck.blobs)


def test_object_storage_end_to_end_via_fsspec():
    """The real thing issue #28 asked for: a store path with a scheme, no disk."""
    pytest.importorskip("fsspec")

    registry = ModelRegistry("memory://models")
    assert isinstance(registry._store, FsspecArtifactStore)

    v1 = registry.register(
        use_case="uc",
        model_name="M",
        version=None,
        model=FileWritingModel(),
        metrics={"test_mae": 1.0},
    )
    registry.promote(
        use_case="uc", model_name="M", version=v1.version, to_stage=ModelStage.PRODUCTION
    )

    # materialize downloads to a real local directory, which is what load() needs
    local_path, _ = registry.get_model(use_case="uc", model_name="M", stage=ModelStage.PRODUCTION)
    assert Path(local_path, "weights.txt").exists()
    FileWritingModel.load(local_path)

    # and rollback still works against the remote record
    v2 = registry.register(
        use_case="uc",
        model_name="M",
        version=None,
        model=FileWritingModel(),
        metrics={"test_mae": 2.0},
    )
    registry.promote(
        use_case="uc", model_name="M", version=v2.version, to_stage=ModelStage.PRODUCTION
    )
    registry.rollback(use_case="uc", model_name="M", to_version=v1.version)
    _, mv = registry.get_model(use_case="uc", model_name="M", stage=ModelStage.PRODUCTION)
    assert mv.version == v1.version


def test_the_scheme_picks_the_store():
    assert isinstance(resolve_store("/models"), LocalArtifactStore)
    assert isinstance(resolve_store("file:///models"), LocalArtifactStore)
    assert isinstance(resolve_store(r"C:\models"), LocalArtifactStore)  # a drive, not a scheme
    pytest.importorskip("fsspec")
    assert isinstance(resolve_store("memory://models"), FsspecArtifactStore)


def test_a_registered_plugin_beats_the_generic_fallback():
    """Someone can add a connector. That was the whole requirement."""
    made = []

    class MyCloudStore(DuckStore):
        def __init__(self, scheme: str) -> None:
            super().__init__()
            made.append(scheme)

    with patch(
        "ubunye.models.artifact_store._registered_stores", return_value={"mycloud": MyCloudStore}
    ):
        store = resolve_store("mycloud://bucket/models")

    assert isinstance(store, MyCloudStore)
    assert made == ["mycloud"]


def test_local_writes_are_atomic(tmp_path):
    """A reader must never see half a registry.json, even if the writer dies."""
    store = LocalArtifactStore()
    target = tmp_path / "registry.json"
    store.write_text(str(target), json.dumps({"ok": True}))
    assert json.loads(target.read_text(encoding="utf-8")) == {"ok": True}
    assert not list(tmp_path.glob("*.tmp"))  # no droppings left behind
