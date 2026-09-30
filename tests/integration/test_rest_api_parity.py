"""The rest_api connector gives the same rows on Spark and on pandas (F-015).

One local JSON API (``tests/rest_api_server.py``) is read on live Spark and on
the pandas backend, with each pagination, and the two frames are compared:
the same columns in the same order, the same types, the same values, the same
map entry order (Spark's comes from a Java HashMap), and the same ``rows-v1``
hash (ADR 006), which covers every value and type. Records Spark refuses are
refused on both. The writer POSTs the same payloads from both.

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


def _assert_same(spark_backend, spark_df, pandas_backend, pandas_frame):
    spark_rows = _rows(spark_backend, spark_df)
    pandas_rows = _rows(pandas_backend, pandas_frame)
    assert pandas_rows == spark_rows
    # Order sensitive: columns, and the entries of every map.
    assert json.dumps(pandas_rows) == json.dumps(spark_rows)
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
        ([{"price": 1}], [{"name": "price", "type": "double"}], "can not accept object"),
    ],
    ids=[
        "long-and-double",
        "null-everywhere",
        "bool-and-long",
        "object-and-number",
        "int-as-double",
    ],
)
def test_records_spark_refuses_are_refused_on_both(backends, records, schema, error):
    spark_backend, pandas_backend = backends
    with served(records) as api:
        cfg = {"url": f"{api.base}/all", **({"schema": schema} if schema else {})}
        for backend in (spark_backend, pandas_backend):
            with pytest.raises(SourceReadError, match=error):
                RestApiReader().read(cfg, backend).count()


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
    assert json.dumps(pandas_posted) == json.dumps(spark_posted)
