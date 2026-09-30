"""Awkward data: the pandas backend against live Spark (experiment E-08).

Wide tables, long text, nested and conflicting JSON, messy CSV, time zones,
many small files, empty inputs, special numbers and odd column names. Each case
reads the same bytes on both engines and asks for the same columns, types and
values (Spark decides), or, where Spark refuses the input, for the pandas
backend to refuse it too. Findings F-060 onwards; see
tasks/hardening/experiments/E-08-awkward-data.md.

Marked integration (the Spark half needs a JVM). CI runs it on Spark 4 and 3.5.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")

from pyspark.sql import SparkSession  # noqa: E402

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402

from .test_pandas_backend_parity import assert_same  # noqa: E402

pytestmark = pytest.mark.integration

ZONE = "America/New_York"  # has daylight saving time, so gaps and folds exist


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    keys = {
        "spark.sql.session.timeZone": ZONE,
        # Spark 3.5 refuses dates before 1582 in parquet unless told how; Spark 4
        # reads them as written. Pin Spark 4's behaviour on both.
        "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
        "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    }
    before = {k: session.conf.get(k, None) for k in keys}
    for k, v in keys.items():
        session.conf.set(k, v)
    yield session
    for k, v in before.items():
        if v is None:
            session.conf.unset(k)
        else:
            session.conf.set(k, v)


@pytest.fixture
def pandas_backend():
    return PandasBackend(timezone=ZONE)


def _file(tmp_path: Path, name: str, data: bytes) -> str:
    path = tmp_path / name
    path.write_bytes(data)
    return str(path)


def _read_both(spark, pandas_backend, fmt, path, options=None, schema=None):
    reader = spark.read.format(fmt).options(**(options or {}))
    if schema:
        reader = reader.schema(schema)
    spark_df = reader.load(path)
    frame = pandas_backend.read_frame(fmt, path, options=options, schema=schema)
    return spark_df, frame


# --------------------------------------------------------------------------- #
# Messy CSV
# --------------------------------------------------------------------------- #

HEADERS = [
    b"a,a,,A,b,Col,col\n1,2,3,4,5,6,7\n",
    b"id,NA\n1,2\n",
    "naïve,名前,with space,a.b,select,Größe\n1,2,3,4,5,6\n".encode("utf-8"),
]


@pytest.mark.parametrize("data", HEADERS, ids=["dupes-blank-case", "null-value", "unicode"])
def test_csv_header_names(spark, pandas_backend, tmp_path, data):
    """F-060: blank and duplicate header names are renamed as Spark renames them."""
    path = _file(tmp_path, "h.csv", data)
    options = {"header": "true", "nullValue": "NA"}
    assert_same(*_read_both(spark, pandas_backend, "csv", path, options))
