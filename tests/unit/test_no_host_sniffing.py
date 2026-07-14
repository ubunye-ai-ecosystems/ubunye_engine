"""The engine must not change what it does because of WHERE it is running.

Two places did.

`writers/unity.py::_is_databricks()` string-matched the Spark conf for "databricks" and
used the guess to decide whether to run OPTIMIZE and VACUUM. It was wrong twice: a
connector should ask the TARGET what it can do rather than detect its host, and the guess
was factually wrong — OPTIMIZE, ZORDER and VACUUM are Delta features, and open-source
Delta has had them for years. Anyone running Delta off Databricks silently did not get
the maintenance they had explicitly asked for.

`_internal/auto_detect.py` picked the model registry backend from
`DATABRICKS_RUNTIME_VERSION`, with no way to override. The same pipeline registered
models to MLflow on Databricks and to a directory anywhere else, and said nothing.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from ubunye._internal.auto_detect import detect_registry_backend
from ubunye.plugins.writers import unity


def test_the_unity_writer_no_longer_sniffs_its_host():
    assert not hasattr(unity, "_is_databricks"), "host sniffing is back"


def test_optimize_is_attempted_wherever_it_runs():
    """Not gated behind a guess. The user asked for it; ask the target for it."""
    spark = MagicMock()
    unity.UnityTableWriter._maintain(
        spark, "cat.sch.tbl", {"optimize": {"enabled": True, "zorder_by": ["id"]}}
    )
    executed = [c.args[0] for c in spark.sql.call_args_list]
    assert any("OPTIMIZE cat.sch.tbl ZORDER BY (id)" in q for q in executed)


def test_a_target_that_cannot_optimize_is_reported_not_swallowed():
    """The write already succeeded — the data is safe. Failing the task over housekeeping
    would throw away a good write. But skipping in SILENCE is how somebody believes their
    table has been compacted for a year while it has not."""
    spark = MagicMock()
    spark.sql.side_effect = Exception("OPTIMIZE is not supported")

    with patch.object(unity.logger, "warning") as warned:
        unity.UnityTableWriter._maintain(spark, "cat.sch.tbl", {"optimize": {"enabled": True}})

    assert warned.called, "an unsupported OPTIMIZE was skipped without a word"
    assert "could not do it" in str(warned.call_args)


def test_nothing_runs_when_nothing_was_asked_for():
    spark = MagicMock()
    unity.UnityTableWriter._maintain(spark, "cat.sch.tbl", {})
    spark.sql.assert_not_called()


def test_the_registry_backend_can_be_chosen_explicitly():
    """The whole point: the same pipeline must be able to mean the same thing everywhere."""
    with patch.dict(
        os.environ, {"UBUNYE_REGISTRY_BACKEND": "filesystem", "DATABRICKS_RUNTIME_VERSION": "15.4"}
    ):
        assert detect_registry_backend() == "filesystem"


def test_the_host_is_only_a_fallback():
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "15.4"}, clear=True):
        with patch("ubunye._internal.auto_detect._has_package", return_value=True):
            assert detect_registry_backend() == "mlflow"

    with patch.dict(os.environ, {}, clear=True):
        assert detect_registry_backend() == "filesystem"
