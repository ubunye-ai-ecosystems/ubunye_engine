"""The three-step notebook records into a Delta table on live Spark (ADR 016).

What a Databricks job does, on a laptop: the run-record location is a table name
(here from ``$UBUNYE_RUN_RECORDS``; on Databricks the ``ubunye_run_records`` job
parameter), the first save creates the table, a success and a failed read each
leave a record, and the failed one names its stage.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

import ubunye  # noqa: E402

pytestmark = pytest.mark.integration

COPY = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


@pytest.fixture(scope="module")
def spark(_spark_jvm_with_delta):
    if not _spark_jvm_with_delta:
        pytest.skip("Delta jars could not be resolved")
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


@pytest.fixture
def table(spark, tmp_path):
    db = f"ubunye_nb_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {db} LOCATION '{(tmp_path / db).as_posix()}'")
    yield f"{db}.ubunye_runs"
    spark.sql(f"DROP DATABASE {db} CASCADE")


def _task(root: Path, source: str) -> Path:
    task = root / "uc" / "pkg" / "copy"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id,city\n1,jhb\n2,cpt\n3,pta\n", encoding="utf-8")
    (task / "transformations.py").write_text(COPY, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{(root / source).as_posix()}"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / "out").as_posix()}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


def test_the_notebook_creates_the_table_and_records_success_and_failure(
    spark, table, tmp_path, monkeypatch
):
    monkeypatch.setenv("UBUNYE_RUN_RECORDS", table)
    monkeypatch.delenv("UBUNYE_LINEAGE_BACKEND", raising=False)
    assert not spark.catalog.tableExists(table)

    good = _task(tmp_path / "good", "in.csv")
    with ubunye.notebook(str(good), dt="202611", spark=spark) as run:
        assert run.run_records == f"delta {table}"
        run.write(run.transform(run.read()))

    bad = _task(tmp_path / "bad", "missing.csv")
    with pytest.raises(Exception):
        with ubunye.notebook(str(bad), dt="202611", spark=spark) as run:
            run.read()

    assert spark.catalog.tableExists(table)
    rows = spark.sql(
        # A subquery, not QUALIFY: QUALIFY needs Spark 4, and CI also runs Spark 3.5.
        "SELECT run_id, status, record FROM ("
        f"SELECT *, row_number() OVER (PARTITION BY run_id ORDER BY written_at DESC) AS n "
        f"FROM {table}) WHERE n = 1"
    ).collect()
    latest = {r["status"]: json.loads(r["record"]) for r in rows}
    assert set(latest) == {"success", "error"}
    assert latest["success"]["variables"]["dt"] == "202611"
    assert latest["success"]["outputs"][0]["data_hash"]
    assert latest["error"]["failed_stage"] == "read"
    assert "missing.csv" in latest["error"]["error"]
