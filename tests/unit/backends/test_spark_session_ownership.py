"""The Spark backend stops only a session it started.

`start()` asks Spark for a session with `getOrCreate()`, which hands back a
session that is already running if there is one: the user's, or a notebook's.
`stop()` (and the garbage collector, through `__del__`) then stopped it anyway,
so `run_task(..., backend="spark")` in a process that already had a session
ended that session when the run finished. Found by the backend conformance
suite, whose Spark tests lost their shared session to a collected backend.

pyspark is faked here so the unit tier needs no Spark; the integration tier
checks the same thing against a real session.
"""

from __future__ import annotations

import gc
import sys
import types

import pytest

from ubunye.backends.spark_backend import SparkBackend


class FakeSession:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class FakeBuilder:
    def __init__(self, spark_session_cls) -> None:
        self._cls = spark_session_cls

    def appName(self, _name):
        return self

    def config(self, _key, _value):
        return self

    def getOrCreate(self):
        if self._cls.active is None:
            self._cls.active = FakeSession()
        return self._cls.active


@pytest.fixture
def fake_spark(monkeypatch):
    """A fake `pyspark.sql` whose SparkSession tracks one active session."""

    class SparkSession:
        active = None

        @classmethod
        def getActiveSession(cls):
            return cls.active

    SparkSession.builder = FakeBuilder(SparkSession)
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = SparkSession
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(SparkBackend, "_platform_master", staticmethod(lambda: None))
    return SparkSession


def test_a_session_it_started_is_stopped(fake_spark):
    backend = SparkBackend(app_name="t")
    backend.start()
    session = fake_spark.active
    backend.stop()
    assert session.stopped


def test_a_session_that_was_already_running_is_left_running(fake_spark):
    users = fake_spark.active = FakeSession()
    backend = SparkBackend(app_name="t")
    backend.start()
    assert backend.spark is users  # it attached, as before
    backend.stop()
    assert not users.stopped


def test_nor_does_the_garbage_collector_stop_it(fake_spark):
    users = fake_spark.active = FakeSession()
    backend = SparkBackend(app_name="t")
    backend.start()
    del backend
    gc.collect()
    assert not users.stopped
