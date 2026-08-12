"""#38 acceptance: the same task on Spark and on pandas produces the same data.

"You don't know a door is a door until something other than the original key
opens it." This runs one passthrough task (read csv, write parquet, no
transform) end-to-end through the engine — once on ``SparkBackend``, once on
``PandasBackend`` — and asserts the written output is identical. That is the
proof that ``Backend`` is a real port, not a Spark alias.

Marked integration (the Spark half needs a JVM); skips where pandas is absent.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from pyspark.sql import SparkSession  # noqa: E402

from ubunye.api import run_task  # noqa: E402

# The ambient-session backend reuses the shared integration SparkSession and its
# stop() is a no-op — a SparkBackend here would stop the session-scoped fixture
# and break every test that runs after this one.
from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402

pytestmark = pytest.mark.integration

_TRANSFORM = '''\
from ubunye.core.interfaces import Task


class Passthrough(Task):
    """Read one input, write it out unchanged — backend-agnostic on purpose."""

    def transform(self, sources):
        return {"out": sources["in"]}
'''


def _write_task(root: Path, in_uri: str) -> Path:
    task = root / "parity" / "io" / "copy"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(_TRANSFORM, encoding="utf-8")
    config = f"""\
MODEL: "etl"
VERSION: "1.0.0"
ENGINE:
  spark_conf:
    spark.sql.shuffle.partitions: "1"
CONFIG:
  inputs:
    in:
      format: s3
      path: "{in_uri}"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{{{{ env.PARITY_OUT }}}}"
      file_format: parquet
      mode: overwrite
"""
    (task / "config.yaml").write_text(config, encoding="utf-8")
    return task


def _canon(frame: "pd.DataFrame"):
    """Order-independent, dtype-cosmetic-independent view of the data."""
    frame = frame.reindex(sorted(frame.columns), axis=1)
    frame = frame.sort_values(by=list(frame.columns)).reset_index(drop=True)
    return [tuple(str(v) for v in row) for row in frame.itertuples(index=False)]


def test_same_task_same_data_on_spark_and_pandas(tmp_path, monkeypatch):
    src = tmp_path / "in.csv"
    src.write_text("id,klass,city\n1,a,jhb\n2,b,cpt\n3,a,pta\n", encoding="utf-8")
    task = _write_task(tmp_path, src.as_uri())

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    monkeypatch.setenv("PARITY_OUT", (tmp_path / "spark_out").as_uri())
    run_task(str(task), mode="DEV", backend=DatabricksBackend(spark=spark))

    monkeypatch.setenv("PARITY_OUT", (tmp_path / "pandas_out").as_uri())
    run_task(str(task), mode="DEV", backend=PandasBackend())

    spark_data = _canon(pd.read_parquet(tmp_path / "spark_out"))
    pandas_data = _canon(pd.read_parquet(tmp_path / "pandas_out"))

    assert len(spark_data) == 3
    assert spark_data == pandas_data
