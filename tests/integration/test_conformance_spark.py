"""The Spark backends pass the conformance suite every backend must pass.

The suite (``ubunye.testing.backend_conformance``) reads a reference CSV and
requires the same run record hash as a reference computed from plain Python
values, so this also proves Spark and the reference agree.

Marked integration (needs a JVM).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pandas")  # the reference fingerprint is engine free, the suite is not
from pyspark.sql import SparkSession  # noqa: E402

from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.backends.spark_backend import SparkBackend  # noqa: E402
from ubunye.testing.backend_conformance import BackendConformance  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture
def utc_session():
    """The shared session, in UTC for the reference timestamps, restored after."""
    session = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    before = session.conf.get("spark.sql.session.timeZone")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.conf.set("spark.sql.session.timeZone", before)


class TestSparkBackend(BackendConformance):
    @pytest.fixture
    def backend(self, utc_session):
        backend = SparkBackend(app_name="conformance")
        backend.start()  # attaches to the shared session
        yield backend
        backend.stop()  # it did not start the session, so it leaves it running


def test_a_spark_backend_leaves_a_session_it_did_not_start_running(utc_session):
    """It used to stop it, on stop() and when garbage collected (a user's own session too)."""
    import gc

    backend = SparkBackend(app_name="borrower")
    backend.start()
    assert backend.spark is utc_session
    backend.stop()
    del backend
    gc.collect()
    assert utc_session.range(3).count() == 3


class TestDatabricksBackend(BackendConformance):
    @pytest.fixture
    def backend(self, utc_session):
        backend = DatabricksBackend(spark=utc_session)
        backend.start()
        yield backend
        backend.stop()  # an ambient session is never stopped by its backend
