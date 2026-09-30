"""The Olist sales pipeline gives the same rows on Spark and pandas, all three steps.

Runs `examples/real-world/olist_ecommerce` on the committed synthetic sample on a real
Spark session (in UTC, as on the clouds) and on the pandas backend, and compares the run
records with `ubunye.proving`: every dimension PASS, and the golden digests the unit
test pins for pandas. The sample holds the traps of the real files (a byte order mark,
review text over several lines with doubled quotes, zip prefixes with a leading zero),
and the steps use input contracts, quarantine, reconcile and a warning, so this also
checks those give the same verdicts and the same set aside rows on both engines.
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

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "real-world" / "olist_ecommerce"
STEPS = ("clean", "orders_fact", "monthly")
GOLDEN = {"clean": "6affe2f5382c", "orders_fact": "6dcf270328d8", "monthly": "a0bd206029ca"}


@pytest.fixture
def utc_spark():
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    before = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark
    spark.conf.set("spark.sql.session.timeZone", before)


def test_all_three_steps_give_the_same_rows_on_spark_and_pandas(tmp_path, utc_spark):
    root = tmp_path / "olist"
    shutil.copytree(
        EXAMPLE, root, ignore=shutil.ignore_patterns("output*", "data", "evidence", ".ubunye")
    )
    for backend, out in (
        (DatabricksBackend(spark=utc_spark), "out_spark"),
        ("pandas", "out_pandas"),
    ):
        for step in STEPS:
            ubunye.run_task(
                str(root / "pipelines" / "olist" / "sales" / step),
                backend=backend,
                lineage=True,
                variables={"out_dir": (root / out).as_posix()},
            )

    store = FileSystemLineageStore(str(root / "pipelines" / ".ubunye" / "lineage"))
    for step, golden in GOLDEN.items():
        runs = store.list_runs(f"olist/sales/{step}")
        records = {r.backend: r.to_dict() for r in runs}
        assert set(records) == {"databricks", "pandas"}, step
        # The same checks, with the same counts, on both engines.
        verdicts = {
            b: sorted((e["output"], e["rule"], e["failed"], e["passed"]) for e in r["expectations"])
            for b, r in records.items()
        }
        assert verdicts["databricks"] == verdicts["pandas"], step
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
