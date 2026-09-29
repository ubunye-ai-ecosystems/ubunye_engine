"""Each output is computed once, and every consumer gets that copy (ADR 009, F-040).

A fake lazy backend stands in for Spark: its ``materialise`` hands back a new
frame object (a copy), so identity says which frame each consumer acted on.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterator, List

import pytest

pd = pytest.importorskip("pandas")

from ubunye.core.capabilities import Capabilities  # noqa: E402
from ubunye.core.hooks import Hook  # noqa: E402
from ubunye.core.interfaces import Backend  # noqa: E402
from ubunye.core.runtime import Engine, EngineContext, Registry  # noqa: E402


class LazyBackend(Backend):
    """Says it is lazy, like Spark. Records what it held and released."""

    name = "fake-lazy"
    CAPABILITIES = Capabilities(lazy=True, distributed=True)

    def __init__(self, can_hold: bool = True, raises: bool = False) -> None:
        self.can_hold = can_hold
        self.raises = raises
        self.held: List[Any] = []
        self.released: List[Any] = []
        self.events: List[str] = []

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def materialise(self, frame: Any) -> Any:
        if self.raises:
            raise RuntimeError("PERSIST TABLE is not supported on serverless compute")
        if not self.can_hold:
            return None
        once = frame.copy()
        self.held.append(once)
        self.events.append("hold")
        return once

    def release(self, frame: Any) -> None:
        self.released.append(frame)
        self.events.append("release")


class Recorder(Hook):
    """A hook that acts on the outputs at task end, as the run record does."""

    reads_outputs = True

    def __init__(self, backend: LazyBackend) -> None:
        self.backend = backend
        self.outputs: Dict[str, Any] = {}
        self.basis: Dict[str, str] = {}
        self.released_before_end: bool = False
        self.failed = False

    @contextmanager
    def task(self, ctx, cfg, state) -> Iterator[None]:
        try:
            yield
        except Exception:
            self.failed = True
            raise
        finally:
            self.outputs = dict(state.get("outputs") or {})
            self.basis = dict(state.get("hash_basis") or {})
            self.released_before_end = bool(self.backend.released)
            self.backend.events.append("task_end")


WRITTEN: Dict[str, Any] = {}
FRAMES: Dict[str, Any] = {}


class Reader:
    def read(self, cfg, backend):
        return pd.DataFrame({"id": [1, 2, 3], "qty": [5, None, 7]})


class Writer:
    def write(self, df, cfg, backend):
        if cfg.get("boom"):
            raise OSError("disk full")
        WRITTEN[cfg["path"]] = df


class Transform:
    def apply(self, inputs, cfg, backend):
        src = inputs["src"]
        out = {"a": src.assign(x=1), "b": src.assign(y=2)}
        if cfg.get("twice"):
            out["b"] = out["a"]
        if cfg.get("passthrough"):
            out["a"] = src
        FRAMES.clear()
        FRAMES.update(out)
        return out


def _cfg(**extra: Any) -> dict:
    outputs = {
        "a": {"format": "w", "path": "out/a", "mode": "overwrite"},
        "b": {"format": "w", "path": "out/b", "mode": "overwrite"},
    }
    cfg = {
        "CONFIG": {
            "inputs": {"src": {"format": "r", "path": "in/src"}},
            "transform": {"type": "t", **extra.pop("transform", {})},
            "outputs": outputs,
        }
    }
    for key, value in extra.items():
        cfg["CONFIG"][key] = value
    return cfg


def _engine(backend: LazyBackend, hooks: list) -> Engine:
    reg = Registry()
    reg.register_reader("r", Reader)  # type: ignore[arg-type]
    reg.register_writer("w", Writer)  # type: ignore[arg-type]
    reg.register_transform("t", Transform)  # type: ignore[arg-type]
    WRITTEN.clear()
    return Engine(
        backend=backend,
        registry=reg,
        context=EngineContext(run_id="r1", task_name="uc/pkg/t"),
        hooks=hooks,
        manage_backend=False,
    )


@pytest.fixture(autouse=True)
def _switch_on(monkeypatch):
    monkeypatch.delenv("UBUNYE_MATERIALISE_OUTPUTS", raising=False)


# --- when it happens ------------------------------------------------------------


def test_a_plain_run_holds_nothing():
    backend = LazyBackend()
    _engine(backend, hooks=[]).run(_cfg())
    assert backend.held == []
    assert WRITTEN["out/a"] is FRAMES["a"]
    assert WRITTEN["out/b"] is FRAMES["b"]


def test_a_recorded_run_holds_every_output_and_every_consumer_gets_it():
    backend = LazyBackend()
    rec = Recorder(backend)
    engine = _engine(backend, hooks=[rec])
    engine.run(_cfg())
    assert len(backend.held) == 2
    # Holding is a timed step, so its cost is in the record.
    held_steps = [t["output"] for t in engine._timings if t["step"] == "Materialise"]
    assert held_steps == ["a", "b"]
    for name in ("a", "b"):
        once = rec.outputs[name]
        assert once is not FRAMES[name]
        assert any(once is h for h in backend.held)
        assert WRITTEN[f"out/{name}"] is once  # the writer wrote what the record hashes
    assert rec.basis == {"a": "materialised", "b": "materialised"}


def test_only_the_outputs_with_expectations_are_held_when_not_recorded(monkeypatch):
    from ubunye.core import expectations

    seen: Dict[str, Any] = {}
    real = expectations.apply

    def spy(outputs, specs):
        seen.update(outputs)
        return real(outputs, specs)

    monkeypatch.setattr(expectations, "apply", spy)
    backend = LazyBackend()
    _engine(backend, hooks=[]).run(_cfg(expectations={"a": {"rules": [{"not_null": "id"}]}}))
    assert len(backend.held) == 1
    held = backend.held[0]
    assert seen["a"] is held  # the checks saw the held frame
    assert WRITTEN["out/a"] is held  # and the writer wrote it
    assert WRITTEN["out/b"] is FRAMES["b"]  # no expectations, one consumer: left alone


def test_checks_write_and_record_see_the_same_frame(monkeypatch):
    from ubunye.core import expectations

    seen: Dict[str, Any] = {}
    real = expectations.apply

    def spy(outputs, specs):
        seen.update(outputs)
        return real(outputs, specs)

    monkeypatch.setattr(expectations, "apply", spy)
    backend = LazyBackend()
    rec = Recorder(backend)
    _engine(backend, hooks=[rec]).run(_cfg(expectations={"a": {"rules": [{"not_null": "id"}]}}))
    assert seen["a"] is WRITTEN["out/a"] is rec.outputs["a"]
    assert any(seen["a"] is h for h in backend.held)


def test_one_frame_under_two_names_is_computed_once():
    backend = LazyBackend()
    _engine(backend, hooks=[]).run(_cfg(transform={"twice": True}))
    assert len(backend.held) == 1
    assert WRITTEN["out/a"] is WRITTEN["out/b"] is backend.held[0]


def test_an_output_that_overwrites_its_input_is_held_too():
    # Held rows are computed before the overwrite deletes the source. Unheld,
    # Spark deletes the files and then fails reading them (F-047).
    backend = LazyBackend()
    rec = Recorder(backend)
    cfg = _cfg()
    cfg["CONFIG"]["outputs"]["a"]["path"] = "in/src"
    _engine(backend, hooks=[rec]).run(cfg)
    assert len(backend.held) == 2
    assert rec.basis == {"a": "materialised", "b": "materialised"}


# --- release -------------------------------------------------------------------


def test_held_frames_are_released_after_the_record_on_success():
    backend = LazyBackend()
    rec = Recorder(backend)
    _engine(backend, hooks=[rec]).run(_cfg())
    assert rec.released_before_end is False
    assert sorted(map(id, backend.released)) == sorted(map(id, backend.held))
    assert backend.events.index("task_end") < backend.events.index("release")


def test_held_frames_are_released_when_the_run_fails():
    backend = LazyBackend()
    rec = Recorder(backend)
    cfg = _cfg()
    cfg["CONFIG"]["outputs"]["b"]["boom"] = True
    with pytest.raises(OSError):
        _engine(backend, hooks=[rec]).run(cfg)
    assert rec.failed
    assert len(backend.held) == 2
    assert sorted(map(id, backend.released)) == sorted(map(id, backend.held))
    assert rec.released_before_end is False


def test_held_frames_are_released_when_an_expectation_fails():
    from ubunye.core.errors import ExpectationError

    backend = LazyBackend()
    with pytest.raises(ExpectationError):
        _engine(backend, hooks=[]).run(_cfg(expectations={"a": {"rules": [{"not_null": "qty"}]}}))
    assert len(backend.held) == 1
    assert backend.released == backend.held


def test_a_release_that_raises_does_not_fail_the_run():
    backend = LazyBackend()

    def bad(frame):
        raise RuntimeError("session gone")

    backend.release = bad  # type: ignore[method-assign]
    _engine(backend, hooks=[Recorder(backend)]).run(_cfg())


# --- fallback --------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["cannot", "raises"])
def test_a_backend_that_cannot_hold_runs_as_before_and_says_so(kind):
    backend = LazyBackend(can_hold=kind != "cannot", raises=kind == "raises")
    rec = Recorder(backend)
    _engine(backend, hooks=[rec]).run(_cfg())
    assert WRITTEN["out/a"] is FRAMES["a"]
    assert rec.basis == {"a": "recomputed", "b": "recomputed"}
    assert backend.released == []


def test_the_switch_turns_it_off(monkeypatch):
    monkeypatch.setenv("UBUNYE_MATERIALISE_OUTPUTS", "0")
    backend = LazyBackend()
    rec = Recorder(backend)
    _engine(backend, hooks=[rec]).run(_cfg())
    assert backend.held == []
    assert rec.basis == {"a": "recomputed", "b": "recomputed"}


def test_an_in_memory_backend_is_never_asked_and_its_basis_is_the_written_rows():
    class InMemory(LazyBackend):
        CAPABILITIES = Capabilities(lazy=False)

    backend = InMemory()
    rec = Recorder(backend)
    _engine(backend, hooks=[rec]).run(_cfg())
    assert backend.held == []
    assert rec.basis == {"a": "materialised", "b": "materialised"}


def test_a_test_double_backend_is_not_asked():
    from unittest.mock import MagicMock

    double = MagicMock()
    rec = Recorder(LazyBackend())
    _engine(double, hooks=[rec]).run(_cfg())  # type: ignore[arg-type]
    double.materialise.assert_not_called()
    assert rec.basis == {}  # not said: the record judges by the frame's type


# --- what the caller gets back ------------------------------------------------------


def test_the_caller_never_gets_a_held_frame():
    backend = LazyBackend()
    out = _engine(backend, hooks=[Recorder(backend)]).run(_cfg())
    assert out["a"] is FRAMES["a"]
    assert out["b"] is FRAMES["b"]


def test_the_caller_gets_the_same_quarantine_cut_of_its_own_frames():
    backend = LazyBackend()
    rules = {"a": {"rules": [{"not_null": "qty", "severity": "quarantine"}], "quarantine": "q"}}
    cfg = _cfg(expectations=rules)
    cfg["CONFIG"]["outputs"]["q"] = {"format": "w", "path": "out/q", "mode": "overwrite"}
    rec = Recorder(backend)
    out = _engine(backend, hooks=[rec]).run(cfg)
    # What was written is cut from the held frame...
    assert list(WRITTEN["out/a"]["id"]) == [1, 3]
    assert list(WRITTEN["out/q"]["id"]) == [2]
    assert rec.basis["q"] == "materialised"
    # ...and the caller gets the same cut, of the transform's own frame.
    assert list(out["a"]["id"]) == [1, 3]
    assert list(out["q"]["id"]) == [2]
    assert all(out[n] is not h for n in ("a", "q") for h in backend.held)


# --- the run record -------------------------------------------------------------------


def test_the_record_writes_the_basis_the_engine_gave(tmp_path):
    from ubunye.lineage.recorder import LineageRecorder

    rec = LineageRecorder(base_dir=str(tmp_path))
    ctx = EngineContext(run_id="r9", task_name="uc/pkg/t")
    cfg = _cfg()
    rec.task_start(context=ctx, config=cfg)
    frame = pd.DataFrame({"id": [1]})
    rec.task_end(
        context=ctx,
        config=cfg,
        outputs={"a": frame, "b": frame.copy()},
        status="success",
        duration_sec=1.0,
        inputs={"src": frame.copy()},
        hash_basis={"a": "recomputed"},
    )
    stored = rec._store.load("uc/pkg/t", "r9")  # type: ignore[attr-defined]
    basis = {s.name: s.hash_basis for s in stored.outputs + stored.inputs}
    # Given for a; judged by type for b and the input (pandas: in memory).
    assert basis == {"a": "recomputed", "b": "materialised", "src": "materialised"}


def test_the_record_calls_a_spark_like_frame_recomputed_when_not_told():
    from ubunye.lineage.recorder import _basis_of

    class SparkLike:
        pass

    SparkLike.__module__ = "pyspark.sql.classic.dataframe"
    assert _basis_of(SparkLike()) == "recomputed"
    assert _basis_of(pd.DataFrame()) == "materialised"
