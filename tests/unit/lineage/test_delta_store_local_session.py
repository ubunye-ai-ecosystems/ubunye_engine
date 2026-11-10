"""The Delta run record store tunes only the local session it starts itself (F-116).

With no SparkSession (a CLI or pandas run), the store starts a local one. Delta then
read its small log in 50 tasks (`spark.databricks.delta.snapshotPartitions`), about
4.5 s per save. The session it starts reads it in one. A session someone else
started, the user's or the platform's, is never changed.

pyspark is faked: the builder records the options it was given.
"""

from __future__ import annotations

import sys
import types

import pytest

from ubunye.lineage.delta_store import DeltaLineageBackend


@pytest.fixture
def fake_spark(monkeypatch):
    class Builder:
        def __init__(self):
            self.options = {}

        def appName(self, _name):
            return self

        def config(self, key, value):
            self.options[key] = value
            return self

        def getOrCreate(self):
            return "session"

    class SparkSession:
        active = None
        builder = Builder()

        @classmethod
        def getActiveSession(cls):
            return cls.active

    class SparkContext:
        _active_spark_context = None

    pyspark = types.ModuleType("pyspark")
    pyspark.SparkContext = SparkContext
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = SparkSession
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "delta", None)  # no configure_spark_with_delta_pip
    return types.SimpleNamespace(session=SparkSession, context=SparkContext)


def _store():
    store = DeltaLineageBackend.__new__(DeltaLineageBackend)
    store._table_or_path = "/tmp/runs"
    return store


def test_the_local_session_it_starts_reads_the_delta_log_in_one_task(fake_spark):
    assert _store()._session() == "session"
    options = fake_spark.session.builder.options
    assert options["spark.databricks.delta.snapshotPartitions"] == "1"
    assert options["spark.ui.enabled"] == "false"


def test_a_running_spark_context_is_not_retuned(fake_spark):
    """getOrCreate would hand back the user's session: no tuning goes onto it."""
    fake_spark.context._active_spark_context = object()
    _store()._session()
    options = fake_spark.session.builder.options
    assert "spark.databricks.delta.snapshotPartitions" not in options
    assert "spark.ui.enabled" not in options


def test_an_active_session_is_used_as_given(fake_spark):
    fake_spark.session.active = "users"
    assert _store()._session() == "users"
    assert fake_spark.session.builder.options == {}
