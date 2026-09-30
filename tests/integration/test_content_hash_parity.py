"""The run record's data hash is the same on Spark and on pandas (ADR 006).

The same rows must give the same ``rows-v1`` hash whichever engine computed it,
whatever the partitioning, and in a session timezone other than UTC. A different
cell must give a different hash. Checked against a real SparkSession.
"""

from __future__ import annotations

import datetime as dt
import decimal
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")

from pyspark.sql import SparkSession  # noqa: E402

from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter  # noqa: E402
from ubunye.adapters.spark.content_hash import fingerprint_spark  # noqa: E402
from ubunye.api import run_task  # noqa: E402
from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.write_modes import ResolvedWriteMode  # noqa: E402
from ubunye.lineage.content_hash import fingerprint, fingerprint_arrow  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

pytestmark = pytest.mark.integration
ZONE = "Africa/Johannesburg"
UTC = dt.timezone.utc


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    before = session.conf.get("spark.sql.session.timeZone")
    session.conf.set("spark.sql.session.timeZone", ZONE)
    yield session
    session.conf.set("spark.sql.session.timeZone", before)


def _table():
    """Every type a record is likely to meet, with nulls, NaN and awkward text."""
    return pa.table(
        {
            "i32": pa.array([1, None, -3, 7], pa.int32()),
            "i64": pa.array([3000000000, 5, None, -1], pa.int64()),
            "f64": pa.array([0.1, float("nan"), None, 1e21]),
            "f32": pa.array([0.1, 2.5, None, -0.0], pa.float32()),
            "flag": pa.array([True, None, False, True]),
            "txt": pa.array(['say "hi"', "", None, "tab\té ü \u2028 ctl\x01"]),
            "day": pa.array(
                [dt.date(2024, 1, 2), None, dt.date(1999, 12, 31), dt.date(1970, 1, 1)]
            ),
            "ts": pa.array(
                [
                    dt.datetime(2024, 1, 2, 1, 4, 5, 123456, tzinfo=UTC),
                    None,
                    dt.datetime(1999, 12, 31, 23, 59, 59, tzinfo=UTC),
                    dt.datetime(1970, 1, 1, tzinfo=UTC),
                ],
                pa.timestamp("us", tz="UTC"),
            ),
            "dec": pa.array(
                [decimal.Decimal("1.50"), None, decimal.Decimal("-2.25"), decimal.Decimal("0.00")],
                pa.decimal128(10, 2),
            ),
            "arr": pa.array([[1.0, None], [], None, [float("inf")]], pa.list_(pa.float64())),
            "st": pa.array(
                [{"k": None, "j": 2}, None, {"k": 1.5, "j": None}, {"k": 0.0, "j": 0}],
                pa.struct([("k", pa.float64()), ("j", pa.int32())]),
            ),
            "bin": pa.array([b"\x00\x01", None, b"", b"abc"]),
        }
    )


@pytest.fixture
def written(tmp_path):
    """The table written by the pandas backend and read back by Spark."""
    out = str(tmp_path / "t")
    PandasBackend(timezone=ZONE).execute_write(
        _table().to_pandas(types_mapper=pd.ArrowDtype),
        ResolvedWriteMode(mode="overwrite", save_mode="overwrite"),
        connector="s3",
        file_format="parquet",
        path=out,
    )
    return out


def test_spark_and_pandas_agree(spark, written):
    pandas_fp = fingerprint(PandasDataFrameAdapter(_table().to_pandas(types_mapper=pd.ArrowDtype)))
    spark_fp = fingerprint_spark(spark.read.parquet(written))
    assert spark_fp.is_complete and pandas_fp.is_complete, (spark_fp.error, pandas_fp.error)
    assert spark_fp.row_count == pandas_fp.row_count == 4
    assert spark_fp.schema_hash == pandas_fp.schema_hash
    assert spark_fp.data_hash == pandas_fp.data_hash


def test_maps_hash_the_same_whatever_their_entry_order(spark):
    """Maps are unordered: sorted by key before hashing, null values kept (ADR 006)."""
    from pyspark.sql import types as T

    inner = T.MapType(T.StringType(), T.LongType())
    spark_schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("m", T.MapType(T.StringType(), T.StringType())),
            T.StructField("mm", T.MapType(T.StringType(), inner)),
            T.StructField("lm", T.ArrayType(inner)),
            T.StructField("st", T.StructType([T.StructField("x", inner)])),
        ]
    )
    rows = [
        (
            1,
            {"zip": "0152", "city": None, "area": "5"},
            {"b": {"y": 2, "x": None}, "a": {}},
            [{"q": 1, "p": None}, None],
            ({"k": 1, "j": 2},),
        ),
        (2, {}, None, [], None),
    ]
    df = spark.createDataFrame(rows, spark_schema)

    s, i = pa.string(), pa.map_(pa.string(), pa.int64())
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "m": pa.array([[("area", "5"), ("city", None), ("zip", "0152")], []], pa.map_(s, s)),
            "mm": pa.array([[("a", []), ("b", [("x", None), ("y", 2)])], None], pa.map_(s, i)),
            "lm": pa.array([[[("p", None), ("q", 1)], None], []], pa.list_(i)),
            "st": pa.array([{"x": [("j", 2), ("k", 1)]}, None], pa.struct([("x", i)])),
        }
    )
    spark_fp, pandas_fp = fingerprint_spark(df), fingerprint_arrow(table)
    assert spark_fp.schema_hash == pandas_fp.schema_hash
    assert spark_fp.data_hash == pandas_fp.data_hash


def test_the_decimal_fallback_gives_the_same_digest(spark, written, monkeypatch):
    # Past about 2.1 billion rows, ANSI Spark raises on the long half lane sums
    # (F-041) and the hash is taken with decimal sums instead.
    from ubunye.adapters.spark import content_hash as sch

    df = spark.read.parquet(written)
    fast = fingerprint_spark(df)

    def overflow(*_):
        raise ArithmeticError("[ARITHMETIC_OVERFLOW] long overflow")

    monkeypatch.setattr(sch, "_half_lane_sums", overflow)
    assert fingerprint_spark(df).data_hash == fast.data_hash


def test_a_failing_job_is_not_hashed_a_second_time(spark, written, monkeypatch):
    from ubunye.adapters.spark import content_hash as sch

    calls = []

    def failing(*_):
        calls.append("half")
        raise RuntimeError("Job aborted: a UDF raised")

    monkeypatch.setattr(sch, "_half_lane_sums", failing)
    monkeypatch.setattr(sch, "_decimal_lane_sums", lambda *_: calls.append("decimal"))
    with pytest.raises(RuntimeError, match="Job aborted"):
        fingerprint_spark(spark.read.parquet(written))
    assert calls == ["half"]


def test_the_dispatcher_picks_spark(spark, written):
    df = spark.read.parquet(written)
    assert fingerprint(df).data_hash == fingerprint_spark(df).data_hash


def test_partitioning_and_order_do_not_matter(spark, written):
    df = spark.read.parquet(written)
    shuffled = df.repartition(7).orderBy("i64", ascending=False)
    assert fingerprint_spark(df).is_complete
    assert fingerprint_spark(shuffled).data_hash == fingerprint_spark(df).data_hash


def test_one_changed_cell_changes_it(spark, written):
    from pyspark.sql import functions as F

    df = spark.read.parquet(written)
    changed = df.withColumn("i32", F.when(F.col("i32") == 7, F.lit(8)).otherwise(F.col("i32")))
    assert fingerprint_spark(changed).data_hash != fingerprint_spark(df).data_hash


def test_a_spark_built_frame_matches_arrow(spark):
    df = spark.createDataFrame(
        [(1, "a", 1.5), (2, None, float("nan"))], "id bigint, s string, d double"
    )
    table = pa.table(
        {"id": pa.array([1, 2], pa.int64()), "s": ["a", None], "d": [1.5, float("nan")]}
    )
    assert fingerprint_spark(df).data_hash == fingerprint_arrow(table).data_hash


def test_an_empty_frame(spark):
    df = spark.createDataFrame([], "id bigint")
    assert (
        fingerprint_spark(df).data_hash
        == fingerprint_arrow(pa.table({"id": pa.array([], pa.int64())})).data_hash
    )


TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def test_the_same_task_leaves_the_same_receipt_on_both_engines(spark, tmp_path, monkeypatch):
    task = tmp_path / "uc" / "pkg" / "copy"
    task.mkdir(parents=True)
    (tmp_path / "in.csv").write_text(
        "id,city,amount,day\n1,jhb,1.5,2024-01-02\n2,cpt,,2024-02-03\n3,pta,2.0,\n",
        encoding="utf-8",
    )
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
ENGINE:
  spark_conf:
    spark.sql.session.timeZone: "{ZONE}"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{(tmp_path / "in.csv").as_uri()}"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{{{{ env.OUT }}}}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OUT", (tmp_path / "spark_out").as_uri())
    run_task(str(task), backend=DatabricksBackend(spark=spark), lineage=True)
    monkeypatch.setenv("OUT", (tmp_path / "pandas_out").as_uri())
    run_task(str(task), backend="pandas", lineage=True)

    records = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage")).list_runs("uc/pkg/copy")
    assert {r.backend for r in records} == {"databricks", "pandas"}
    hashes = {r.backend: r.outputs[0].data_hash for r in records}
    assert all(h and h.startswith("sha256:") for h in hashes.values()), records
    assert hashes["databricks"] == hashes["pandas"]
    assert all(r.outputs[0].row_count == 3 for r in records)
    assert Path(tmp_path / "pandas_out" / "_SUCCESS").exists()
