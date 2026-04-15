"""Tests for the Hook abstraction and Engine integration."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, List

import pytest

from ubunye.core.hooks import Hook, HookChain
from ubunye.core.interfaces import Backend
from ubunye.core.runtime import Engine, EngineContext, Registry


class RecordingHook(Hook):
    """Hook that appends ``(event, args)`` tuples to a shared log."""

    def __init__(self, name: str, log: List[tuple]) -> None:
        self.name = name
        self.log = log

    @contextmanager
    def task(self, ctx, cfg, state):
        self.log.append((self.name, "task_enter"))
        try:
            yield
            self.log.append((self.name, "task_exit_ok", state.get("outputs")))
        except Exception as e:
            self.log.append((self.name, "task_exit_err", type(e).__name__))
            raise

    @contextmanager
    def step(self, ctx, step_name, meta):
        self.log.append((self.name, "step_enter", step_name))
        try:
            yield
            self.log.append((self.name, "step_exit_ok", step_name))
        except Exception as e:
            self.log.append((self.name, "step_exit_err", step_name, type(e).__name__))
            raise


class BrokenHook(Hook):
    @contextmanager
    def task(self, ctx, cfg, state):
        raise RuntimeError("boom on enter")
        yield  # unreachable

    @contextmanager
    def step(self, ctx, name, meta):
        raise RuntimeError("boom on enter")
        yield


class FakeBackend(Backend):
    def __init__(self):
        self.started = False
        self.stopped = False

    def start(self, **kwargs):
        self.started = True

    def stop(self):
        self.stopped = True

    def is_spark(self) -> bool:
        return False


class PassReader:
    def __init__(self, payload="data"):
        self.payload = payload

    def read(self, cfg, backend):
        return f"{self.payload}:{cfg.get('table', '?')}"


class PassTransform:
    def apply(self, sources, cfg, backend):
        return {"out": "|".join(sorted(sources.values()))}


class PassWriter:
    calls: List[tuple] = []

    def write(self, df, cfg, backend):
        PassWriter.calls.append((df, cfg.get("path")))


class FailingReader:
    def read(self, cfg, backend):
        raise ValueError("reader failed")


def _make_registry(reader_cls=PassReader) -> Registry:
    reg = Registry()
    reg.register_reader("fake", reader_cls)
    reg.register_transform("pass", PassTransform)
    reg.register_writer("fake", PassWriter)
    return reg


def _cfg() -> dict:
    return {
        "CONFIG": {
            "inputs": {"a": {"format": "fake", "table": "t1"}},
            "transform": {"type": "pass"},
            "outputs": {"out": {"format": "fake", "path": "/tmp/x"}},
        }
    }


# ---------- HookChain primitives ----------


def test_hookchain_enters_and_exits_in_order():
    log: List[tuple] = []
    chain = HookChain([RecordingHook("a", log), RecordingHook("b", log)])
    ctx = EngineContext(run_id="r", task_name="t", profile="p")
    state: Dict[str, Any] = {}

    with chain.task(ctx, {}, state):
        pass

    assert log == [
        ("a", "task_enter"),
        ("b", "task_enter"),
        ("b", "task_exit_ok", None),  # LIFO exit
        ("a", "task_exit_ok", None),
    ]


def test_broken_hook_does_not_break_chain():
    log: List[tuple] = []
    chain = HookChain([BrokenHook(), RecordingHook("good", log)])
    ctx = EngineContext(run_id="r")

    with chain.task(ctx, {}, {}):
        pass

    assert ("good", "task_enter") in log
    assert ("good", "task_exit_ok", None) in log


# ---------- Engine integration ----------


def test_engine_calls_hooks_for_each_step():
    log: List[tuple] = []
    hook = RecordingHook("h", log)
    PassWriter.calls = []

    engine = Engine(
        backend=FakeBackend(),
        registry=_make_registry(),
        context=EngineContext(run_id="r", task_name="my/task", profile="dev"),
        hooks=[hook],
    )
    outputs = engine.run(_cfg())

    assert outputs == {"out": "data:t1"}
    step_events = [e for e in log if "step" in e[1]]
    # Expect one step pair for reader, transform, writer
    step_names = [e[2] for e in step_events if e[1] == "step_enter"]
    assert step_names == ["Reader:fake", "Transform:pass", "Writer:fake"]


def test_engine_propagates_step_failure_through_hooks():
    log: List[tuple] = []
    hook = RecordingHook("h", log)
    engine = Engine(
        backend=FakeBackend(),
        registry=_make_registry(reader_cls=FailingReader),
        context=EngineContext(run_id="r", task_name="t", profile="p"),
        hooks=[hook],
    )

    with pytest.raises(ValueError, match="reader failed"):
        engine.run(_cfg())

    assert ("h", "step_exit_err", "Reader:fake", "ValueError") in log
    assert ("h", "task_exit_err", "ValueError") in log


def test_engine_dry_run_skips_backend_but_runs_task_hooks():
    log: List[tuple] = []
    backend = FakeBackend()
    engine = Engine(
        backend=backend,
        registry=_make_registry(),
        context=EngineContext(run_id="r", task_name="t", profile="p"),
        hooks=[RecordingHook("h", log)],
    )

    assert engine.run(_cfg(), dry_run=True) is None
    assert backend.started is False
    assert backend.stopped is False
    assert log == [("h", "task_enter"), ("h", "task_exit_ok", None)]


def test_engine_exposes_outputs_to_hooks_via_state():
    log: List[tuple] = []
    engine = Engine(
        backend=FakeBackend(),
        registry=_make_registry(),
        context=EngineContext(run_id="r", task_name="t", profile="p"),
        hooks=[RecordingHook("h", log)],
    )
    engine.run(_cfg())

    task_exit = [e for e in log if e[1] == "task_exit_ok"][0]
    assert task_exit[2] == {"out": "data:t1"}
