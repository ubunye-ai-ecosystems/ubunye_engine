"""A model store written by Ubunye 0.9.0 still reads and promotes the same way.

``tests/fixtures/registry_0_9_0/store`` was written by the 0.9.0 code (the
``ModelRegistry`` filesystem path), and ``expected.json`` holds what that code
returned for :func:`scenario` run on a copy of it. The test runs the same
scenario with today's code and compares.

Timestamps a step sets are new on every run, so the snapshot keeps a timestamp
only when it is the one already in the store, and writes ``"<new>"`` otherwise.

To rebuild the fixture (only ever with the 0.9.0 code checked out)::

    python tests/unit/models/test_registry_0_9_0_store.py
"""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest

from ubunye.core.errors import UbunyeError
from ubunye.models.base import UbunyeModel
from ubunye.models.registry import FilesystemRegistryBackend, ModelRegistry, ModelStage

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "registry_0_9_0"
UC = "fraud"
TIMES = ("registered_at", "promoted_to_staging", "promoted_to_prod", "archived_at")


class FixtureModel(UbunyeModel):
    """Its artifact is one JSON file holding a tag."""

    def __init__(self, tag: str = "m", drift_ok: bool = True) -> None:
        self.tag = tag
        self.drift_ok = drift_ok

    def train(self, df: Any) -> Dict[str, Any]:
        return {}

    def predict(self, df: Any) -> Any:
        return df

    def save(self, path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)
        (Path(path) / "model.json").write_text(json.dumps({"tag": self.tag}), encoding="utf-8")

    @classmethod
    def load(cls, path: str) -> "FixtureModel":
        return cls(json.loads((Path(path) / "model.json").read_text(encoding="utf-8"))["tag"])

    def metadata(self) -> Dict[str, Any]:
        return {"library": "fixture", "library_version": "1", "drift_check_passed": self.drift_ok}


def build_store(store: Path) -> None:
    """The store as 0.9.0 wrote it. Run only with the 0.9.0 code."""
    reg = ModelRegistry(str(store))
    gates = {"min_auc": 0.8, "require_drift_check": True}
    reg.register(UC, "RiskModel", "1.0.0", FixtureModel("a"), {"auc": 0.9, "loss": 0.3},
                 lineage_run_id="run-1", registered_by="alice", promotion_gates=gates)  # fmt: skip
    reg.promote(UC, "RiskModel", "1.0.0", ModelStage.STAGING, promoted_by="bob")
    reg.promote(UC, "RiskModel", "1.0.0", ModelStage.PRODUCTION, promoted_by="carol")
    reg.register(UC, "RiskModel", None, FixtureModel("b"), {"auc": 0.7, "loss": 0.5},
                 lineage_run_id="run-2")  # fmt: skip
    reg.promote(UC, "RiskModel", "1.0.1", ModelStage.PRODUCTION, promoted_by="dave", force=True)
    reg.register(UC, "RiskModel", None, FixtureModel("c"), {"auc": 0.85, "loss": 0.4},
                 lineage_run_id="run-3", registered_by="erin")  # fmt: skip
    reg.register(UC, "RiskModel", "2.0.0", FixtureModel("d", drift_ok=False), {"auc": 0.95})
    reg.archive(UC, "RiskModel", "2.0.0")
    reg.register(UC, "NoGates", None, FixtureModel("x"), {"rmse": 2.0})
    reg.register(UC, "NoGates", None, FixtureModel("y"), {"rmse": 1.5})
    reg.promote(UC, "NoGates", "1.0.1", ModelStage.STAGING)
    # Registered through the backend protocol with extra metadata (kept nested on disk).
    FilesystemRegistryBackend(str(store)).register(
        use_case=UC,
        model_name="Legacy",
        version="0.1.0",
        model=FixtureModel("l"),
        metrics={"auc": 0.81},
        metadata={"owner": "team-a"},
    )


def _old_times(store: Path) -> Dict[str, str]:
    """Every timestamp already in the store, so a snapshot can tell old from new."""
    seen: Dict[str, str] = {}
    for path in store.rglob("registry.json"):
        for version in json.loads(path.read_text(encoding="utf-8"))["versions"].values():
            for key in TIMES:
                if version.get(key):
                    seen[version[key]] = version[key]
    return seen


def _norm(value: Any, old: Dict[str, str], store: Path) -> Any:
    if isinstance(value, dict):
        return {k: _norm(v, old, store) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_norm(v, old, store) for v in value]
    if isinstance(value, ModelStage):
        return value.value
    if isinstance(value, str) and value.startswith(str(store)):
        return Path(value).relative_to(store).as_posix()
    if isinstance(value, str) and len(value) > 20 and value[:4].isdigit() and "T" in value:
        return value if value in old else "<new>"
    return value


def scenario(store: Path) -> Dict[str, Any]:
    """Reads and moves every user makes, on a copy of the 0.9.0 store."""
    old = _old_times(store)
    reg = ModelRegistry(str(store))
    out: Dict[str, Any] = {}

    def step(name: str, call: Callable[[], Any]) -> None:
        try:
            result = call()
        except UbunyeError as exc:
            out[name] = {"raised": type(exc).__name__}
            return
        if isinstance(result, tuple):  # get_model
            path, mv = result
            result = {"path": path, "tag": FixtureModel.load(path).tag, "mv": asdict(mv)}
        elif isinstance(result, list):
            result = [asdict(mv) for mv in result]
        elif hasattr(result, "__dataclass_fields__"):
            result = asdict(result)
        out[name] = _norm(result, old, store)

    rm, ng = "RiskModel", "NoGates"
    step("list", lambda: reg.list_versions(UC, rm))
    step("get_1.0.1", lambda: reg.get_model(UC, rm, version="1.0.1"))
    step("get_production", lambda: reg.get_model(UC, rm, stage=ModelStage.PRODUCTION))
    step("get_staging_none", lambda: reg.get_model(UC, rm, stage=ModelStage.STAGING))
    step("get_missing", lambda: reg.get_model(UC, rm, version="9.9.9"))
    step("get_neither", lambda: reg.get_model(UC, rm))
    step("gates", lambda: reg.promotion_gates(UC, rm))
    step("gates_nogates", lambda: reg.promotion_gates(UC, ng))
    step("compare", lambda: reg.compare_versions(UC, rm, "1.0.0", "1.0.2"))
    step("promote_staging", lambda: reg.promote(UC, rm, "1.0.2", ModelStage.STAGING, "erin"))
    step("promote_prod", lambda: reg.promote(UC, rm, "1.0.2", ModelStage.PRODUCTION, "erin"))
    step("blocked", lambda: reg.promote(UC, rm, "1.0.1", ModelStage.PRODUCTION))
    step("blocked_drift", lambda: reg.promote(UC, rm, "2.0.0", ModelStage.STAGING))
    step(
        "gates_given",
        lambda: reg.promote(UC, rm, "2.0.0", ModelStage.STAGING, gates={"min_auc": 0.9}),
    )
    step("rollback", lambda: reg.rollback(UC, rm, "1.0.0"))
    step("demote", lambda: reg.demote(UC, rm, "1.0.0", ModelStage.STAGING))
    step("demote_up", lambda: reg.demote(UC, rm, "1.0.2", ModelStage.PRODUCTION))
    step("archive", lambda: reg.archive(UC, rm, "2.0.0"))
    step("promote_archived", lambda: reg.promote(UC, rm, "1.0.2", ModelStage.ARCHIVED))
    step("register_auto", lambda: reg.register(UC, rm, None, FixtureModel("e"), {"auc": 0.6},
                                               lineage_run_id="run-9", registered_by="frank"))  # fmt: skip
    step("register_twice", lambda: reg.register(UC, rm, "1.0.0", FixtureModel(), {}))
    step("forced", lambda: reg.promote(UC, rm, "2.0.1", ModelStage.PRODUCTION, "gus", force=True))
    step("gates_after", lambda: reg.promotion_gates(UC, rm))
    step("set_gates", lambda: reg.register(UC, ng, None, FixtureModel(), {"rmse": 1.0},
                                           promotion_gates={"max_rmse": 1.2}))  # fmt: skip
    step("gates_set", lambda: reg.promotion_gates(UC, ng))
    step("clear_gates", lambda: reg.register(UC, ng, None, FixtureModel(), {}, promotion_gates={}))
    step("gates_cleared", lambda: reg.promotion_gates(UC, ng))
    step("list_after", lambda: reg.list_versions(UC, rm))
    step("list_nogates", lambda: reg.list_versions(UC, ng))
    step("missing_model", lambda: reg.list_versions(UC, "Nope"))
    out["files"] = {
        p.relative_to(store).as_posix(): _norm(
            json.loads(p.read_text(encoding="utf-8")), old, store
        )
        for p in sorted(store.rglob("*.json"))
    }
    # Key order is part of the file format.
    out["key_order"] = {
        p.relative_to(store).as_posix(): list(json.loads(p.read_text(encoding="utf-8")))
        for p in sorted(store.rglob("registry.json"))
    }
    return out


def _copy(tmp_path: Path) -> Path:
    store = tmp_path / "store"
    shutil.copytree(FIXTURE / "store", store)
    return store


def test_a_0_9_0_store_reads_and_promotes_the_same(tmp_path: Path) -> None:
    expected = json.loads((FIXTURE / "expected.json").read_text(encoding="utf-8"))
    got = json.loads(json.dumps(scenario(_copy(tmp_path))))
    for key in expected:
        assert got[key] == expected[key], key
    assert got.keys() == expected.keys()


def test_metadata_given_through_the_backend_reads_back(tmp_path: Path) -> None:
    store = _copy(tmp_path)
    reg = ModelRegistry(str(store))
    path, mv = reg.get_model(UC, "Legacy", version="0.1.0")
    assert FixtureModel.load(path).tag == "l"
    assert mv.metadata["library"] == "fixture"
    _, info = FilesystemRegistryBackend(str(store)).get(
        use_case=UC, model_name="Legacy", version="0.1.0"
    )
    assert info.metadata["owner"] == "team-a"
    reg.promote(UC, "Legacy", "0.1.0", ModelStage.PRODUCTION)
    on_disk = json.loads((store / UC / "Legacy" / "registry.json").read_text(encoding="utf-8"))
    version = on_disk["versions"]["0.1.0"]
    assert version["stage"] == "production"
    assert version["metadata"]["registry_metadata"] == {"owner": "team-a"}


def _regenerate(target: Optional[Path] = None) -> List[str]:  # pragma: no cover - a tool
    import tempfile

    target = target or FIXTURE
    if target.exists():
        shutil.rmtree(target)
    build_store(target / "store")
    with tempfile.TemporaryDirectory() as tmp:
        store = Path(tmp) / "store"
        shutil.copytree(target / "store", store)
        snapshot = scenario(store)
    (target / "expected.json").write_text(
        json.dumps(snapshot, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    return sorted(p.relative_to(target).as_posix() for p in target.rglob("*") if p.is_file())


if __name__ == "__main__":  # pragma: no cover
    print("\n".join(_regenerate()))


pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")
