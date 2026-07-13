"""`python -m ubunye` must exist, because a cloud cannot call a console script.

AWS EMR Serverless and GCP Dataproc Serverless hand a Python file to spark-submit. No
shell, no PATH, no `ubunye` command. An engine reachable only through its CLI cannot run
on either of them -- and you find out after wiring up IAM, a bucket and billing, which is
the most expensive possible moment.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ubunye.__main__ import main


def test_it_runs_a_task_with_the_arguments_a_cloud_passes():
    with patch("ubunye.run_task") as run_task:
        rc = main(["--task-dir", "/code/pipelines/sales/etl/daily", "--mode", "PROD", "--dt", "2026-07-13"])

    assert rc == 0
    run_task.assert_called_once()
    kwargs = run_task.call_args.kwargs
    assert kwargs["task_dir"] == "/code/pipelines/sales/etl/daily"
    assert kwargs["mode"] == "PROD"
    assert kwargs["dt"] == "2026-07-13"


def test_task_dir_is_required():
    """Failing with a usage message beats failing inside a Spark job on someone's bill."""
    with pytest.raises(SystemExit):
        main([])


def test_it_does_not_build_a_spark_session():
    """spark-submit already made one, with the platform's master and executors.

    Building a second here would either fail, or -- far worse -- quietly ignore the
    cluster and run the whole job in the driver.
    """
    import inspect

    import ubunye.__main__ as entry

    # Check for the CALL, not the word: the module docstring explains at length why it
    # does not build a session, and an assertion that trips over its own prose is a
    # test of the comments.
    source = inspect.getsource(entry)
    assert "getOrCreate" not in source
    assert "SparkSession.builder" not in source
