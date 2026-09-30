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


INFER_CASES = {
    "space-after-comma": "a, b, c\n1, 2, 3\n4, 5, 6\n",
    "past-64-bits": (
        "big,huge,edge\n"
        "9223372036854775807,12345678901234567890123,-9223372036854775808\n"
        "9223372036854775808,1,-9223372036854775809\n"
        "1,2," + "9" * 40 + "\n"
    ),
    "java-number-forms": (
        "sign,suffix,hex,tok,lower,spaced,exp\n"
        "+5,1.5d,0x1.8p1,Inf,inf,1.5 ,1e400\n"
        "-7,2f,1,-Inf,1, 2,1E5\n"
        "007,3D,2.5,NaN,2,3,-1e-3\n"
    ),
    "arrow-only-forms": "h,n,big\n0x1F,nan,INF\n0x20,1,1\n",
    "booleans": "b,c\ntRuE,true\nFALSE,1\n",
    "dates-and-timestamps": (
        "t,m,d\n"
        "2024-01-02 03:04:05,2024-01-02,2024-01-02\n"
        "2024-01-02 03:04:05+02:00,2024-01-02 00:00:01,2024-02-29\n"
        "2024-01-02T03:04:05.123456Z,2024-01-03T01:02,1999-12-31\n"
    ),
    "int-long-double-mix": "a,b,c\n1,2147483648,1\n2,1,1.5\n,,\n",
    "all-null": "a,b\n,1\n,2\n",
}


@pytest.mark.parametrize("name", sorted(INFER_CASES))
def test_csv_infer_schema_numbers(spark, pandas_backend, tmp_path, name):
    """F-067: inferSchema follows Spark's CSVInferSchema."""
    path = _file(tmp_path, "infer.csv", INFER_CASES[name].encode())
    options = {"header": "true", "inferSchema": "true"}
    assert_same(*_read_both(spark, pandas_backend, "csv", path, options))


def test_csv_infer_schema_over_many_files(spark, pandas_backend, tmp_path):
    """F-067: one schema for every file of a folder, inferred over all of them."""
    folder = tmp_path / "parts"
    folder.mkdir()
    (folder / "a.csv").write_bytes(b"v,w\n1,x\n")
    (folder / "b.csv").write_bytes(b"v,w\n1.5,2\n")
    options = {"header": "true", "inferSchema": "true"}
    assert_same(*_read_both(spark, pandas_backend, "csv", str(folder), options))


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


def _same_digest(spark_df, frame):
    """The run record's rows-v1 fingerprint is the same on both engines."""
    from ubunye.adapters.spark.content_hash import fingerprint_spark
    from ubunye.lineage.content_hash import fingerprint

    left, right = fingerprint_spark(spark_df), fingerprint(frame)
    assert left.is_complete and right.is_complete, (left.error, right.error)
    assert (left.row_count, left.schema_hash, left.data_hash) == (
        right.row_count,
        right.schema_hash,
        right.data_hash,
    )


# --------------------------------------------------------------------------- #
# Special numbers
# --------------------------------------------------------------------------- #


def _special_numbers():
    import decimal

    D = decimal.Decimal
    return pa.table(
        {
            "f": [
                float("nan"),
                float("inf"),
                float("-inf"),
                -0.0,
                0.0,
                5e-324,
                1.7976931348623157e308,
                None,
            ],
            "f32": pa.array(
                [float("nan"), float("inf"), -0.0, 0.1, 3.4028235e38, 1e-45, 1.0, None],
                pa.float32(),
            ),
            "dec": pa.array(
                [
                    D("12345678901234567890.123456789012345678"),
                    D("-0.000000000000000001"),
                    D(0),
                    None,
                    D("99999999999999999999.999999999999999999"),
                    D(1),
                    D(-1),
                    D("0.5"),
                ],
                pa.decimal128(38, 18),
            ),
            "i64": pa.array([2**63 - 1, -(2**63), 0, None, 1, -1, 2**53 + 1, 2**31], pa.int64()),
        }
    )


def test_parquet_special_numbers(spark, pandas_backend, tmp_path):
    """E-08 shape 9: NaN, infinities, -0.0, 38 digit decimals, int64 limits."""
    import pyarrow.parquet as pq

    path = str(tmp_path / "special.parquet")
    pq.write_table(_special_numbers(), path)
    spark_df, frame = _read_both(spark, pandas_backend, "parquet", path)
    assert_same(spark_df, frame)
    _same_digest(spark_df, frame)


def test_parquet_unsigned_integers(spark, pandas_backend, tmp_path):
    """F-066: unsigned parquet columns read with Spark's types, and hash the same."""
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "u8": pa.array([255, 0, None], pa.uint8()),
            "u16": pa.array([65535, 0, 1], pa.uint16()),
            "u32": pa.array([2**32 - 1, 0, 1], pa.uint32()),
            "u64": pa.array([2**64 - 1, 0, 2**63], pa.uint64()),
            "nested": pa.array([[1, 255], [], None], pa.list_(pa.uint8())),
        }
    )
    path = str(tmp_path / "unsigned.parquet")
    pq.write_table(table, path)
    spark_df, frame = _read_both(spark, pandas_backend, "parquet", path)
    assert_same(spark_df, frame)
    _same_digest(spark_df, frame)


# --------------------------------------------------------------------------- #
# Nested and conflicting JSON
# --------------------------------------------------------------------------- #


def _deep(n: int) -> dict:
    node: dict = {"leaf": n}
    for i in range(n):
        node = {"lvl": i, "child": node}
    return node


def _nested_lines() -> str:
    import json

    rows = []
    for i in range(40):
        row = {"id": i, "deep": _deep(30), "obj": {"z": 1, "a": {"y": [1, 2], "b": None}}}
        if i % 2:
            row["obj"] = {"m": "x"}
            row["opt"] = None
        if i % 3 == 0:
            row[f"extra_{i % 4}"] = i
        row["items"] = [{"sku": f"s{j}", "qty": j} if j % 2 else {"qty": j} for j in range(i % 4)]
        rows.append(json.dumps(row))
    return "\n".join(rows) + "\n"


JSON_CASES = {
    "nested-per-row-keys": _nested_lines(),
    "number-and-text": '{"a":1}\n{"a":"x"}\n{"a":1.50}\n{"a":true}\n{"a":1e2}\n',
    "object-and-text": '{"a":{"k":1,"n":null,"s":"q\\u000b\\u001f/"}}\n{"a":"x"}\n{"a":[1,{"b":2}]}\n',
    "mixed-arrays": '{"a":[1,"x",{"q":1},[2],null,true]}\n{"a":[1.5,2]}\n',
    "big-whole-numbers": (
        '{"a":9223372036854775808,"b":123456789012345678901234567890,"c":1}\n'
        '{"a":1,"b":2,"c":9223372036854775807}\n'
        '{"d":' + "9" * 40 + "}\n"
    ),
    "long-and-double": '{"a":1}\n{"a":1.5}\n{"a":NaN}\n{"a":-Infinity}\n',
    "decimal-and-double": '{"a":123456789012345678901234567890}\n{"a":0.5}\n',
    "empty-names-and-objects": '{"":1,"a":1,"e":{},"l":[{}],"o":{"":2,"k":3},"n":[]}\n',
    "empty-strings": '{"a":""}\n{"a":""}\n{"b":"","c":"x"}\n',
    "array-lines": '[{"a":1},{"a":2,"b":"x"}]\n{"a":3}\n',
    "unicode-key-order": '{"\\ud83d\\ude00":1,"\\uff21":2,"a":3,"B":4}\n',
}


@pytest.mark.parametrize("name", sorted(JSON_CASES))
def test_json_inference(spark, pandas_backend, tmp_path, name):
    """F-064: JSON typed as Spark's JsonInferSchema types it."""
    path = _file(tmp_path, "in.json", JSON_CASES[name].encode("utf-8"))
    assert_same(*_read_both(spark, pandas_backend, "json", path))


def test_json_multiline_document(spark, pandas_backend, tmp_path):
    import json

    rows = [json.loads(line) for line in _nested_lines().splitlines()]
    path = _file(tmp_path, "doc.json", json.dumps(rows, indent=2).encode())
    assert_same(*_read_both(spark, pandas_backend, "json", path, {"multiLine": "true"}))


def test_json_records_with_no_fields_are_rows(spark, pandas_backend, tmp_path):
    """F-065: {} is a row with no columns; the pandas reader lost every such row."""
    path = _file(tmp_path, "empty.json", b'{}\n{}\n{"":1,"e":{}}\n')
    spark_df, frame = _read_both(spark, pandas_backend, "json", path)
    assert spark_df.columns == [] and list(frame.native.columns) == []
    assert spark_df.count() == frame.count() == 3


def test_json_empty_string_in_a_number_field(spark, pandas_backend, tmp_path):
    """F-064: "" merges as null; Spark reads it as null in a number column (partial row)."""
    path = _file(tmp_path, "e.json", b'{"c":5,"d":"k"}\n{"c":"","d":"j"}\n')
    assert_same(*_read_both(spark, pandas_backend, "json", path))


# --------------------------------------------------------------------------- #
# Time zones
# --------------------------------------------------------------------------- #

# In New York 02:xx on 2024-03-10 does not exist and 01:xx on 2024-11-03 happens
# twice; Europe/London and Lord Howe (a 30 minute change) for good measure.
DST_TEXT = (
    "id,t\n"
    "1,2024-03-10 02:30:00\n"
    "2,2024-03-10 02:00:00\n"
    "3,2024-03-10 02:59:59.999999\n"
    "4,2024-11-03 01:30:00\n"
    "5,2024-11-03 01:00:00\n"
    "6,2024-03-31 01:30:00\n"
    "7,2024-10-27 01:30:00\n"
    "8,2024-06-01 12:00:00\n"
)


@pytest.mark.parametrize("zone", [ZONE, "Europe/London", "Australia/Lord_Howe"])
@pytest.mark.parametrize("schema", [None, "id INT, t TIMESTAMP"])
def test_csv_daylight_saving_gaps_and_folds(spark, tmp_path, zone, schema):
    """F-063: wall clock times in a gap or a fold are read with Java's rule."""
    path = _file(tmp_path, "dst.csv", DST_TEXT.encode())
    options = {"header": "true", "inferSchema": "true"}
    before = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", zone)
    try:
        spark_df, frame = _read_both(
            spark, PandasBackend(timezone=zone), "csv", path, options, schema
        )
        assert_same(spark_df, frame)
    finally:
        spark.conf.set("spark.sql.session.timeZone", before)


def test_json_daylight_saving_with_a_schema(spark, pandas_backend, tmp_path):
    """F-063: JSON text read into a TIMESTAMP column follows the same rule."""
    lines = [
        f'{{"id":{i},"t":"{t}"}}'
        for i, t in enumerate(["2024-03-10T02:30:00", "2024-11-03T01:30:00", "2024-06-01T12:00:00"])
    ]
    path = _file(tmp_path, "dst.json", ("\n".join(lines) + "\n").encode())
    assert_same(*_read_both(spark, pandas_backend, "json", path, {}, "id INT, t TIMESTAMP"))


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
