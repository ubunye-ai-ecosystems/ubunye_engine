"""The rest_api connector gives the same rows on Spark and on pandas (F-015).

One local JSON API (``tests/rest_api_server.py``) is read on live Spark and on
the pandas backend, with each pagination, and the two frames are compared:
the same columns in the same order, the same types, the same values (a map
compared as a set of entries: its order is not stable on Spark), and the same
``rows-v1`` hash (ADR 006), which covers every value and type and sorts maps.
Records Spark refuses are refused on both. The writer POSTs the same payloads
from both.

``tests/`` is on ``sys.path`` (its conftest puts it there), so the server
module imports by name.
"""

from __future__ import annotations

import json

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pytest.importorskip("requests")

from pyspark.sql import SparkSession  # noqa: E402
from rest_api_server import RECORDS, read_cfg, served  # noqa: E402

from ubunye.adapters.spark.content_hash import fingerprint_spark  # noqa: E402
from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.errors import SourceReadError  # noqa: E402
from ubunye.lineage.content_hash import fingerprint  # noqa: E402
from ubunye.plugins.readers.rest_api import RestApiReader  # noqa: E402
from ubunye.plugins.writers.rest_api import RestApiWriter  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark():
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


@pytest.fixture
def backends(spark):
    return DatabricksBackend(spark=spark), PandasBackend()


def _rows(backend, frame):
    return list(backend.iter_records(frame))


def _unordered_maps(value):
    """Nested dicts (maps: REST records hold no structs) with sorted keys.

    A map has no entry order, and Spark's differs between its JVMs and between
    classic and Connect, so maps are compared as sets of entries. A row's own
    keys (the columns) keep their order.
    """
    if isinstance(value, dict):
        return {k: _unordered_maps(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_unordered_maps(v) for v in value]
    return value


def _canonical(rows):
    return json.dumps([{k: _unordered_maps(v) for k, v in row.items()} for row in rows])


def _assert_same(spark_backend, spark_df, pandas_backend, pandas_frame):
    spark_rows = _rows(spark_backend, spark_df)
    pandas_rows = _rows(pandas_backend, pandas_frame)
    assert pandas_rows == spark_rows
    # Order sensitive for the columns; maps compared as sets of entries.
    assert [list(r) for r in pandas_rows] == [list(r) for r in spark_rows]
    assert _canonical(pandas_rows) == _canonical(spark_rows)
    ours, theirs = fingerprint(pandas_frame), fingerprint_spark(spark_df)
    assert ours.row_count == theirs.row_count
    assert ours.schema_hash == theirs.schema_hash, spark_df.schema.simpleString()
    assert ours.data_hash == theirs.data_hash


@pytest.mark.parametrize("how", ["offset", "cursor", "next_link"])
def test_read_gives_the_same_frame(backends, how):
    spark_backend, pandas_backend = backends
    with served() as api:
        cfg = read_cfg(api.base, how)
        spark_df = RestApiReader().read(cfg, spark_backend)
        pandas_frame = RestApiReader().read(cfg, pandas_backend)
    assert spark_df.count() == len(RECORDS)
    _assert_same(spark_backend, spark_df, pandas_backend, pandas_frame)


def test_read_with_a_schema_gives_the_same_frame(backends):
    spark_backend, pandas_backend = backends
    schema = [
        {"name": "id", "type": "integer"},
        {"name": "code", "type": "string"},
        {"name": "score", "type": "double"},
        {"name": "active", "type": "boolean"},
        {"name": "late", "type": "string"},
        {"name": "missing", "type": "long"},
    ]
    with served() as api:
        cfg = {**read_cfg(api.base), "schema": schema}
        spark_df = RestApiReader().read(cfg, spark_backend)
        pandas_frame = RestApiReader().read(cfg, pandas_backend)
    _assert_same(spark_backend, spark_df, pandas_backend, pandas_frame)


def test_no_records_give_the_same_empty_frame(backends):
    spark_backend, pandas_backend = backends
    with served() as api:
        cfg = {"url": f"{api.base}/empty", "response": {"root_key": "data"}}
        spark_df = RestApiReader().read(cfg, spark_backend)
        pandas_frame = RestApiReader().read(cfg, pandas_backend)
    assert spark_df.columns == [] and spark_df.count() == 0
    assert pandas_frame.native.shape == (0, 0)


@pytest.mark.parametrize(
    "records, schema, error",
    [
        ([{"price": 1}, {"price": 2.5}], None, "CANNOT_MERGE_TYPE"),
        ([{"id": 1, "note": None}, {"id": 2, "note": None}], None, "CANNOT_DETERMINE_TYPE"),
        ([{"flag": True}, {"flag": 1}], None, "CANNOT_MERGE_TYPE"),
        ([{"x": {"a": 1}}, {"x": 5}], None, "CANNOT_MERGE_TYPE"),
        ([{"price": 2**53 + 1}], [{"name": "price", "type": "double"}], "can not accept object"),
    ],
    ids=[
        "long-and-double",
        "null-everywhere",
        "bool-and-long",
        "object-and-number",
        "inexact-int-as-double",
    ],
)
def test_records_spark_refuses_are_refused_on_both(backends, records, schema, error):
    spark_backend, pandas_backend = backends
    with served(records) as api:
        cfg = {"url": f"{api.base}/all", **({"schema": schema} if schema else {})}
        for backend in (spark_backend, pandas_backend):
            with pytest.raises(SourceReadError, match=error):
                RestApiReader().read(cfg, backend).count()


def test_whole_numbers_in_a_double_column_read_the_same(backends):
    spark_backend, pandas_backend = backends
    schema = [{"name": "price", "type": "double"}, {"name": "rate", "type": "float"}]
    with served([{"price": 1, "rate": 2}, {"price": 2.5, "rate": None}]) as api:
        cfg = {"url": f"{api.base}/all", "schema": schema}
        spark_df = RestApiReader().read(cfg, spark_backend)
        pandas_frame = RestApiReader().read(cfg, pandas_backend)
    _assert_same(spark_backend, spark_df, pandas_backend, pandas_frame)
    assert [r["price"] for r in _rows(spark_backend, spark_df)] == [1.0, 2.5]


def test_nan_timestamps_decimals_are_posted_the_same(backends, spark):
    """F-051: one JSON form per value, the same from Spark and pandas."""
    import datetime as dt
    import decimal

    from pyspark.sql import types as T

    spark_backend, pandas_backend = backends
    instant = dt.datetime(2024, 1, 2, 1, 4, 5, 123456, tzinfo=dt.timezone.utc)
    spark_df = spark.createDataFrame(
        [
            (
                float("nan"),
                instant,
                dt.date(2024, 1, 2),
                decimal.Decimal("12.50"),
                bytearray(b"\x00\x01"),
            )
        ],
        T.StructType(
            [
                T.StructField("x", T.DoubleType()),
                T.StructField("ts", T.TimestampType()),
                T.StructField("day", T.DateType()),
                T.StructField("dec", T.DecimalType(10, 2)),
                T.StructField("bin", T.BinaryType()),
            ]
        ),
    )
    table = pa.table(
        {
            "x": pa.array([float("nan")]),
            "ts": pa.array([instant], pa.timestamp("us", tz="UTC")),
            "day": pa.array([dt.date(2024, 1, 2)]),
            "dec": pa.array([decimal.Decimal("12.50")], pa.decimal128(10, 2)),
            "bin": pa.array([b"\x00\x01"]),
        }
    )
    pandas_frame = table.to_pandas(types_mapper=pd.ArrowDtype)
    payloads = []
    for backend, frame in ((spark_backend, spark_df), (pandas_backend, pandas_frame)):
        with served() as api:
            RestApiWriter().write(frame, {"url": f"{api.base}/sink"}, backend)
            payloads.append(api.posted)
    assert payloads[0] == payloads[1]
    assert payloads[0][0]["records"][0]["ts"] == "2024-01-02T01:04:05.123456Z"


def test_write_posts_the_same_payloads(backends):
    spark_backend, pandas_backend = backends
    payloads = []
    for backend in (spark_backend, pandas_backend):
        with served() as api:
            frame = RestApiReader().read(read_cfg(api.base), backend)
            RestApiWriter().write(frame, {"url": f"{api.base}/sink", "batch_size": 2}, backend)
            payloads.append(api.posted)
    spark_posted, pandas_posted = payloads
    assert [len(p["records"]) for p in spark_posted] == [2, 2, 1]
    for spark_body, pandas_body in zip(spark_posted, pandas_posted):
        assert _canonical(pandas_body["records"]) == _canonical(spark_body["records"])
    assert len(pandas_posted) == len(spark_posted)
