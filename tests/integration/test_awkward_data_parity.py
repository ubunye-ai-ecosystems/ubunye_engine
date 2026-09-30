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


CP1252 = "name,price\nCafé,€5\nTea – green,€3\n".encode("cp1252")


@pytest.mark.parametrize(
    "data, options",
    [
        (CP1252, {"header": "true"}),
        (CP1252, {"header": "true", "encoding": "cp1252"}),
        ("name,city\nJosé,São Paulo\n".encode("latin-1"), {"header": "true", "encoding": "latin1"}),
        (b'n\xe9me,v\n"a\nb\xff",1\n', {"header": "true", "multiLine": "true"}),
    ],
    ids=["cp1252-read-as-utf8", "cp1252", "latin1", "bad-bytes-multiline"],
)
def test_csv_bytes_not_in_the_encoding(spark, pandas_backend, tmp_path, data, options):
    """F-061: a byte that is not valid in the encoding is U+FFFD, as Java decodes it."""
    path = _file(tmp_path, "enc.csv", data)
    assert_same(*_read_both(spark, pandas_backend, "csv", path, options))


def _long_text(n: int) -> str:
    """``n`` characters with line breaks (LF and CRLF), a NUL, a tab and non-ASCII."""
    unit = "abc é€漢\n" + "q" * 40 + "\r\n" + "x\x00y\t"
    return (unit * (n // len(unit) + 1))[:n]


@pytest.mark.parametrize("multiline", ["false", "true"])
def test_csv_values_longer_than_a_megabyte(spark, pandas_backend, tmp_path, multiline):
    """F-062: no limit on a value's length (Spark's maxCharsPerColumn is -1)."""
    big = "z" * 3_000_000
    long = _long_text(1_200_000).replace('"', "")
    data = f'id,t\n1,{big}\n2,"{long}"\n3,b\n'.encode()
    path = _file(tmp_path, "long.csv", data)
    options = {"header": "true", "multiLine": multiline}
    assert_same(*_read_both(spark, pandas_backend, "csv", path, options))


def test_csv_invalid_utf8_fuzz(spark, pandas_backend, tmp_path):
    """F-061: seeded runs of broken UTF-8 (cut, overlong, surrogate, stray bytes)."""
    import random

    rng = random.Random(61)
    pieces = [
        b"a",
        b"b",
        b" ",
        b"\xc3\xa9",
        b"\xe2\x82\xac",
        b"\xe2\x82",
        b"\xf0\x9f\x98",
        b"\xf0\x9f\x98\x80",
        b"\xc0\x80",
        b"\xed\xa0\x80",
        b"\xff",
        b"\x80",
        b"\xe9",
    ]
    lines = [b"id,txt"]
    for i in range(300):
        lines.append(str(i).encode() + b"," + b"".join(rng.choice(pieces) for _ in range(6)))
    path = _file(tmp_path, "fuzz.csv", b"\n".join(lines) + b"\n")
    assert_same(*_read_both(spark, pandas_backend, "csv", path, {"header": "true"}))
