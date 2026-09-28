"""The proving ground's first workload passes on Spark and pandas, every dimension.

Runs the committed example ``examples/proving/c01_portable_etl`` unchanged (a copy of
its folder, so the repository is not written to) on a real Spark session and on the
pandas backend, turns both run records into observations, and asks
``ubunye.proving.compare`` for the verdict. This is the local baseline every cloud run
of C01 is compared with.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402
from ubunye.proving import compare, observe_record  # noqa: E402
from ubunye.proving.matrix import PASS  # noqa: E402

pytestmark = pytest.mark.integration

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "proving" / "c01_portable_etl"
TASK = ("proving", "c01", "etl")
GOLDEN_DIGEST = "bb08a7d7a9fd"


@pytest.fixture
def session_zone():
    """Set the shared session's time zone for one test, then put it back."""
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    before = spark.conf.get("spark.sql.session.timeZone")

    def set_zone(zone):
        spark.conf.set("spark.sql.session.timeZone", zone)
        return spark

    yield set_zone
    spark.conf.set("spark.sql.session.timeZone", before)


def _run_c01(tmp_path, spark):
    root = tmp_path / "c01"
    shutil.copytree(EXAMPLE, root, ignore=shutil.ignore_patterns("output*", "evidence", ".ubunye"))
    task = root / "pipelines" / Path(*TASK)
    for backend, out in (
        (DatabricksBackend(spark=spark), "output_spark"),
        ("pandas", "output_pandas"),
    ):
        ubunye.run_task(
            str(task), backend=backend, lineage=True, variables={"out_dir": (root / out).as_posix()}
        )
    store = FileSystemLineageStore(str(root / "pipelines" / ".ubunye" / "lineage"))
    records = {r.backend: r.to_dict() for r in store.list_runs("/".join(TASK))}
    assert set(records) == {"databricks", "pandas"}
    return compare(
        [
            observe_record(records["databricks"], workload="c01", environment="spark"),
            observe_record(records["pandas"], workload="c01", environment="pandas"),
        ],
        reference="spark",
    )


def test_c01_passes_on_spark_and_pandas(tmp_path, session_zone):
    # An ambient session in UTC, as on Databricks and the clouds.
    matrix = _run_c01(tmp_path, session_zone("UTC"))
    pandas = matrix["environments"]["pandas"]
    assert pandas["dimensions"] == {d: PASS for d in pandas["dimensions"]}, pandas
    assert pandas["digest"] == matrix["environments"]["spark"]["digest"]
    # The golden digest, the same on every machine and engine version this runs on
    # (first measured on Windows, Spark 4.2, pandas 3; CI checks the rest).
    assert pandas["digest"] == GOLDEN_DIGEST
    # The workload's semantics, not only equality: 10 paid lines, and the null country
    # kept as its own group (plain pandas would drop it).
    outs = pandas["outputs"]
    assert outs["order_lines"]["row_count"] == 10
    assert outs["country_summary"]["row_count"] == 5


def test_a_session_in_another_zone_is_reported_not_silent(tmp_path, session_zone):
    # A session the engine did not start is never changed (ADR 007). Its zone differs
    # from pandas' UTC, so days differ; the report must say why.
    matrix = _run_c01(tmp_path, session_zone("Africa/Johannesburg"))
    pandas = matrix["environments"]["pandas"]
    assert pandas["dimensions"]["identity"] == "FAIL"
    assert pandas["dimensions"]["data"] == "FAIL"
    assert "time zone UTC, reference Africa/Johannesburg" in pandas["reason"]
