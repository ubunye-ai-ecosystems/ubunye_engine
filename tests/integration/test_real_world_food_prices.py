"""The food price monitor gives the same rows on Spark and pandas, both steps.

Runs `examples/real-world/food_prices_africa` on the committed WFP sample on a real Spark
session (in UTC, as on the clouds) and on the pandas backend, and compares the run
records with `ubunye.proving`: every dimension PASS, and the golden digests the unit
test pins for pandas. Four traps (rounding modes, float sums, a lost cast, str.replace)
were fixed to get here; this keeps them fixed.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
nw = pytest.importorskip("narwhals")
if not hasattr(nw.col("x"), "floor"):
    pytest.skip("the example needs narwhals>=2.9 (Expr.floor)", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402
from ubunye.proving import compare, observe_record  # noqa: E402
from ubunye.proving.matrix import PASS  # noqa: E402

pytestmark = pytest.mark.integration

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "real-world" / "food_prices_africa"
GOLDEN = {"clean": "f61e0f0544f5", "monitor": "021cc19ca2b6"}


@pytest.fixture
def utc_spark():
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    before = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark
    spark.conf.set("spark.sql.session.timeZone", before)


def test_both_steps_give_the_same_rows_on_spark_and_pandas(tmp_path, utc_spark):
    root = tmp_path / "food"
    shutil.copytree(
        EXAMPLE, root, ignore=shutil.ignore_patterns("output*", "data", "evidence", ".ubunye")
    )
    for backend, out in (
        (DatabricksBackend(spark=utc_spark), "out_spark"),
        ("pandas", "out_pandas"),
    ):
        for step in ("clean", "monitor"):
            ubunye.run_task(
                str(root / "pipelines" / "food" / "prices" / step),
                backend=backend,
                lineage=True,
                variables={"out_dir": (root / out).as_posix()},
            )

    store = FileSystemLineageStore(str(root / "pipelines" / ".ubunye" / "lineage"))
    for step, golden in GOLDEN.items():
        records = {r.backend: r.to_dict() for r in store.list_runs(f"food/prices/{step}")}
        assert set(records) == {"databricks", "pandas"}, step
        matrix = compare(
            [
                observe_record(records["databricks"], workload=step, environment="spark"),
                observe_record(records["pandas"], workload=step, environment="pandas"),
            ],
            reference="spark",
        )
        pandas = matrix["environments"]["pandas"]
        assert pandas["dimensions"] == {d: PASS for d in pandas["dimensions"]}, (step, pandas)
        assert pandas["digest"] == matrix["environments"]["spark"]["digest"] == golden, step
