"""A config must not be able to override a master the platform already chose.

The most expensive silent failure the engine had.

Under `spark-submit` — how AWS EMR Serverless and GCP Dataproc Serverless start every
job — the platform puts its own `spark.master` into the default SparkConf. A task that
also set `spark.master` would win, and the job would run **entirely in the driver**:
ignoring every executor, finishing, reporting success, and billing for a cluster it
never touched.

Nothing warned. The output was correct; there was simply far less of it per minute than
there should have been, forever.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ubunye.backends.spark_backend import SparkBackend
from ubunye.core.errors import SparkSessionError


def _platform_says(master):
    """Fake the master spark-submit would have put in the default SparkConf.

    Patched at the seam, not at `pyspark.SparkConf`: the unit tier does not install
    Spark, and a test that gets skipped is a test that does not run.
    """
    return patch.object(SparkBackend, "_platform_master", staticmethod(lambda: master))


def test_a_config_cannot_hijack_the_platforms_master():
    with _platform_says("k8s://https://kubernetes.default.svc"):
        backend = SparkBackend(app_name="t", conf={"spark.master": "local[*]"})
        with pytest.raises(SparkSessionError, match="already chose one"):
            backend.start()


def test_the_error_says_what_it_would_have_cost():
    """A message that only says "conflict" makes this look like pedantry.

    It is not pedantry: it is a cluster's worth of money, silently unused.
    """
    with _platform_says("yarn"):
        backend = SparkBackend(app_name="t", conf={"spark.master": "local[*]"})
        with pytest.raises(SparkSessionError) as exc:
            backend.start()

    message = str(exc.value)
    assert "yarn" in message and "local[*]" in message
    assert "driver" in message.lower()  # explains WHY it matters


def test_agreeing_with_the_platform_is_fine():
    """Setting the same master the platform set is redundant, not wrong."""
    with _platform_says("local[*]"):
        SparkBackend(app_name="t", conf={"spark.master": "local[*]"})._check_master_not_hijacked()


def test_no_platform_master_means_the_config_may_choose():
    """On a laptop nobody else has chosen, so the config is the only voice in the room."""
    with _platform_says(None):
        SparkBackend(app_name="t", conf={"spark.master": "local[*]"})._check_master_not_hijacked()


def test_not_setting_a_master_is_always_fine():
    """Which is what every config SHOULD do — let the platform decide."""
    with _platform_says("yarn"):
        SparkBackend(
            app_name="t", conf={"spark.sql.shuffle.partitions": "8"}
        )._check_master_not_hijacked()
