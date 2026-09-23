"""The task `ubunye init` makes runs on Spark too, with the same result as pandas.

The default scaffold's transform, ``people[people["age"] >= 18]``, means the same
in pandas and PySpark. Run it on both and the run records must carry the same
data hash: the portability promise, checked on the first thing a user runs.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.cli.main import app  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

pytestmark = pytest.mark.integration


def test_the_scaffold_gives_the_same_receipt_on_spark_and_pandas(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(
        app, ["init", "-d", "pipelines", "-u", "demo", "-p", "starter", "-t", "filter_adults"]
    )
    assert result.exit_code == 0, result.output
    task = "pipelines/demo/starter/filter_adults"

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    ubunye.run_task(task, backend=DatabricksBackend(spark=spark), lineage=True)
    ubunye.run_task(task, backend="pandas", lineage=True)

    records = FileSystemLineageStore(str(tmp_path / "pipelines" / ".ubunye" / "lineage")).list_runs(
        "demo/starter/filter_adults"
    )
    by_backend = {r.backend: r.outputs[0] for r in records}
    assert set(by_backend) == {"databricks", "pandas"}
    assert by_backend["databricks"].row_count == by_backend["pandas"].row_count == 5
    assert by_backend["databricks"].data_hash == by_backend["pandas"].data_hash
    assert by_backend["pandas"].data_hash.startswith("sha256:")
