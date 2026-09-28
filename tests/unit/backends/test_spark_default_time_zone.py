"""A Spark session the engine creates works in UTC unless the task says otherwise.

Found by the proving ground (finding F-021, workload C01): the same Narwhals transform
truncated `2024-01-02 10:15 UTC` to a day as `2024-01-01 22:00 UTC` on local Spark in
Johannesburg (the JVM's zone) and as `2024-01-02 00:00 UTC` on pandas (UTC by default),
so a laptop disagreed with pandas and with the same task on a UTC cloud. Every Spark
test pinned the zone itself, which is why none saw it.

pyspark is faked here: on a CI machine the JVM is in UTC already, so a real session
could not tell the fix from the bug.
"""

from __future__ import annotations

import sys
import types

import pytest

from ubunye.backends.pandas_backend import PandasBackend
from ubunye.backends.spark_backend import DEFAULT_TIME_ZONE, TIME_ZONE_KEY, SparkBackend


class FakeSession:
    def stop(self) -> None:
        pass


@pytest.fixture
def fake_spark(monkeypatch):
    """A fake `pyspark.sql` whose builder remembers the options it was given."""

    class Builder:
        def __init__(self) -> None:
            self.options = {}

        def appName(self, _name):
            return self

        def config(self, key, value):
            self.options[key] = value
            return self

        def getOrCreate(self):
            if SparkSession.active is None:
                SparkSession.active = FakeSession()
            return SparkSession.active

    class SparkSession:
        active = None

        @classmethod
        def getActiveSession(cls):
            return cls.active

    SparkSession.builder = Builder()
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = SparkSession
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(SparkBackend, "_platform_master", staticmethod(lambda: None))
    return SparkSession


def test_a_session_it_creates_is_in_utc(fake_spark):
    SparkBackend(app_name="t").start()
    assert fake_spark.builder.options[TIME_ZONE_KEY] == "UTC" == DEFAULT_TIME_ZONE


def test_the_task_can_choose_another_zone(fake_spark):
    SparkBackend(app_name="t", conf={TIME_ZONE_KEY: "Africa/Johannesburg"}).start()
    assert fake_spark.builder.options[TIME_ZONE_KEY] == "Africa/Johannesburg"


def test_a_session_someone_else_started_is_not_changed(fake_spark):
    fake_spark.active = FakeSession()  # a notebook's or the user's own session
    SparkBackend(app_name="t").start()
    assert TIME_ZONE_KEY not in fake_spark.builder.options


def test_pandas_and_spark_share_the_default():
    assert PandasBackend().timezone == DEFAULT_TIME_ZONE
