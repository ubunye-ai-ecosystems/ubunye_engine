"""ENGINE.spark_conf must reach a session the engine did not create.

Before 0.4.0 it did not. ``DatabricksBackend`` took no ``conf`` argument at all, and
``api.py`` computed ``merged_spark_conf(mode)`` and then threw it away — so every
``ENGINE.spark_conf`` and every ``ENGINE.profiles`` block was **silently discarded**
the moment a session already existed. Which is always, on Databricks.

The bug was not that the setting was wrong. The bug was that it was *ignored, quietly*:
the config said one thing, the runtime did another, and nothing anywhere said so. Every
one of these tests exists to make silence impossible.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ubunye.backends.databricks_backend import AmbientSessionBackend, DatabricksBackend
from ubunye.core.errors import SparkSessionError


def _session(static_keys=()):
    """A session that rejects static keys the way Spark really does."""
    spark = MagicMock()

    def _set(key, value):
        if key in static_keys:
            raise Exception(f"Cannot modify the value of a static config: {key}")

    spark.conf.set.side_effect = _set
    return spark


def test_runtime_conf_reaches_a_session_we_did_not_create():
    """The whole point. This is the assertion that did not exist, and the bug it missed."""
    spark = _session()
    backend = DatabricksBackend(spark=spark, conf={"spark.sql.shuffle.partitions": "8"})
    backend.start()

    spark.conf.set.assert_any_call("spark.sql.shuffle.partitions", "8")


def test_every_key_is_applied_not_just_the_first():
    spark = _session()
    DatabricksBackend(
        spark=spark,
        conf={"spark.sql.shuffle.partitions": "8", "spark.sql.adaptive.enabled": "true"},
    ).start()

    assert spark.conf.set.call_count == 2


def test_a_static_key_raises_instead_of_being_ignored():
    """`spark.master` in a config is a request the engine cannot honour.

    Ignoring it is what the old code did — and on EMR or Dataproc that means the
    pipeline believes it chose the cluster while the platform quietly chose a different
    one. A job that runs single-node on compute you are paying for, successfully, is
    worse than a job that fails.
    """
    spark = _session(static_keys={"spark.master"})
    backend = DatabricksBackend(spark=spark, conf={"spark.master": "local[*]"})

    with pytest.raises(SparkSessionError, match="cannot be set on a session"):
        backend.start()


def test_the_error_names_every_rejected_key_and_why():
    """One error listing all of them, not a stack trace for the first one.

    Someone reading it has to be able to fix their config in one pass.
    """
    spark = _session(static_keys={"spark.master", "spark.sql.extensions"})
    backend = DatabricksBackend(
        spark=spark,
        conf={
            "spark.master": "local[*]",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.shuffle.partitions": "8",  # this one is fine
        },
    )

    with pytest.raises(SparkSessionError) as exc:
        backend.start()

    message = str(exc.value)
    assert "spark.master" in message
    assert "spark.sql.extensions" in message
    assert "static" in message.lower()

    # ...and the settable key was still applied, rather than the whole thing aborting
    # before it got there.
    spark.conf.set.assert_any_call("spark.sql.shuffle.partitions", "8")


def test_no_conf_is_not_an_error():
    """A task with no ENGINE block must not suddenly start failing."""
    spark = _session(static_keys={"spark.master"})
    DatabricksBackend(spark=spark).start()
    spark.conf.set.assert_not_called()


def test_the_backend_has_no_databricks_in_it():
    """It attaches to a session somebody else made and declines to stop it.

    That is equally true of Glue, of spark-submit on EMR or Dataproc, of a notebook and
    of pytest. The name has convinced people the engine has a Databricks dependency
    here. It does not.
    """
    assert AmbientSessionBackend is DatabricksBackend

    spark = _session()
    backend = AmbientSessionBackend(spark=spark, conf={"spark.sql.shuffle.partitions": "4"})
    backend.start()
    backend.stop()

    # stop() must NOT stop a session we do not own — doing so on Databricks would kill
    # the notebook's session out from under the user.
    spark.stop.assert_not_called()
