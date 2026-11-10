"""The run identity lands on every run record (ADR 015)."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from ubunye.core import identity as identity_mod
from ubunye.core.identity import RunIdentity
from ubunye.core.runtime import EngineContext
from ubunye.lineage import evidence
from ubunye.lineage.context import RECORD_VERSION
from ubunye.lineage.recorder import LineageRecorder
from ubunye.lineage.storage import FileSystemLineageStore
from ubunye.testing.lineage_conformance import full_record

_CONFIG = {"MODEL": "etl", "VERSION": "0.1.0", "CONFIG": {"inputs": {}, "outputs": {}}}
_TASK = "fraud/ingestion/claim_etl"


def _recorder(tmp_path: Path) -> tuple:
    base = str(tmp_path / "lineage")
    return LineageRecorder(store="filesystem", base_dir=base), FileSystemLineageStore(base)


def test_engine_context_carries_an_identity() -> None:
    assert EngineContext(run_id="r").identity == {}
    assert EngineContext(run_id="r", identity={"platform": "x"}).identity == {"platform": "x"}


def test_the_environment_hash_ignores_the_identity() -> None:
    env = evidence.environment()
    with_identity = {**env, "identity": {"platform": "databricks", "job_run_id": "1"}}
    other_run = {**env, "identity": {"platform": "databricks", "job_run_id": "2"}}
    assert evidence.environment_hash(with_identity) == evidence.environment_hash(env)
    assert evidence.environment_hash(other_run) == evidence.environment_hash(env)


def test_the_identity_given_by_the_entry_point_is_recorded(tmp_path: Path, monkeypatch) -> None:
    def must_not_collect(*a, **k):
        raise AssertionError("the context already has an identity")

    monkeypatch.setattr(identity_mod, "collect_identity", must_not_collect)
    recorder, store = _recorder(tmp_path)
    given = {"platform": "databricks", "job_id": "123", "git_commit": "a1b2c3"}
    ctx = EngineContext(run_id="r1", task_name=_TASK, profile="PROD", identity=given)
    recorder.task_start(context=ctx, config=_CONFIG)

    run = store.load(_TASK, "r1")
    assert run.environment["identity"] == given
    assert run.record_version == RECORD_VERSION == 2
    env_without = {k: v for k, v in run.environment.items() if k != "identity"}
    assert run.environment_hash == evidence.environment_hash(env_without)


def test_without_an_identity_the_recorder_collects_one(tmp_path: Path, monkeypatch) -> None:
    seen = {}

    def fake_collect(settings=None, *, task_dir=None):
        seen["task_dir"] = task_dir
        return RunIdentity(platform="local", git_commit="c0ffee")

    monkeypatch.setattr(identity_mod, "collect_identity", fake_collect)
    recorder, store = _recorder(tmp_path)

    class Plain:  # a context from older code: no identity attribute at all
        run_id = "r2"
        task_name = _TASK
        profile = "DEV"
        task_dir = str(tmp_path)

    recorder.task_start(context=Plain(), config=_CONFIG)
    assert store.load(_TASK, "r2").environment["identity"] == {
        "platform": "local",
        "git_commit": "c0ffee",
    }
    assert seen["task_dir"] == str(tmp_path)


def test_an_identity_that_cannot_be_read_never_stops_the_record(tmp_path, monkeypatch) -> None:
    def broken(*a, **k):
        raise RuntimeError("nothing works")

    monkeypatch.setattr(identity_mod, "collect_identity", broken)
    recorder, store = _recorder(tmp_path)
    recorder.task_start(context=EngineContext(run_id="r3", task_name=_TASK), config=_CONFIG)
    assert "identity" not in store.load(_TASK, "r3").environment


def test_a_real_run_records_who_ran_it(tmp_path: Path) -> None:
    """No fakes: the installed providers run, off any platform they need."""
    recorder, store = _recorder(tmp_path)
    recorder.task_start(context=EngineContext(run_id="r4", task_name=_TASK), config=_CONFIG)
    ident = store.load(_TASK, "r4").environment["identity"]
    assert ident["platform"] in {"local", "github-actions", "gitlab-ci", "azure-pipelines", "ci"}


def test_the_conformance_record_carries_an_identity() -> None:
    """Every store is held to round-tripping it (ubunye.testing.lineage_conformance)."""
    rec = full_record()
    assert set(rec.environment["identity"]) == {f.name for f in dataclasses.fields(RunIdentity)}


def test_an_old_record_without_identity_still_reads(tmp_path: Path) -> None:
    rec = full_record()
    rec.environment = {k: v for k, v in rec.environment.items() if k != "identity"}
    store = FileSystemLineageStore(str(tmp_path))
    store.save(rec)
    assert "identity" not in store.load(rec.task_path, rec.run_id).environment


@pytest.mark.parametrize("field", ["identity"])
def test_the_identity_is_not_a_template_variable(tmp_path: Path, field: str) -> None:
    recorder, store = _recorder(tmp_path)
    ctx = EngineContext(
        run_id="r5", task_name=_TASK, variables={"dt": "2026-11-01"}, identity={"platform": "p"}
    )
    recorder.task_start(context=ctx, config=_CONFIG)
    assert store.load(_TASK, "r5").variables == {"dt": "2026-11-01"}
