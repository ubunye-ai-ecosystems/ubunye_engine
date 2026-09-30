"""Awkward data: the pandas backend against live Spark (experiment E-09).

Wide tables, long text, nested and conflicting JSON, messy CSV, time zones,
many small files, empty inputs, special numbers and odd column names. Each case
reads the same bytes on both engines and asks for the same columns, types and
values (Spark decides), or, where Spark refuses the input, for the pandas
backend to refuse it too. Findings F-060 onwards; see
tasks/hardening/experiments/E-09-awkward-data.md.

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
    "whole-number-decimal-forms": "a,b,c,d\n5.,1.5E1,0E0,1\n6.,12.0E1,5e0,5.\n",
    "unicode-digits": "a,b\n１２３,٤٥\n7,1\n",
    "decimal-order": "a,b\n9223372036854775808,3000000000\n3000000000,9223372036854775808\n",
    "decimal-and-int": "a,b\n9223372036854775808,9223372036854775808\n1,3000000000\n",
}


@pytest.mark.parametrize("name", sorted(INFER_CASES))
def test_csv_infer_schema_numbers(spark, pandas_backend, tmp_path, name):
    """F-067: inferSchema follows Spark's CSVInferSchema."""
    path = _file(tmp_path, "infer.csv", INFER_CASES[name].encode())
    options = {"header": "true", "inferSchema": "true"}
    assert_same(*_read_both(spark, pandas_backend, "csv", path, options))


def test_csv_empty_field_with_a_null_value(spark, pandas_backend, tmp_path):
    """F-087: with nullValue set, an empty unquoted field is null too (univocity)."""
    path = _file(tmp_path, "nv.csv", b"a,b\n1,x\n,y\nNA,z\n2,\n")
    options = {"header": "true", "inferSchema": "true", "nullValue": "NA"}
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
        (CP1252, {"header": "true", "encoding": "ISO-8859-1"}),
        (
            "name,city\nJosé,São Paulo\n".encode("latin-1"),
            {"header": "true", "encoding": "iso-8859-1"},
        ),
        (b'n\xe9me,v\n"a\nb\xff",1\n', {"header": "true", "multiLine": "true"}),
    ],
    ids=["cp1252-read-as-utf8", "cp1252-read-as-iso-8859-1", "latin1", "bad-bytes-multiline"],
)
def test_csv_bytes_not_in_the_encoding(spark, pandas_backend, tmp_path, data, options):
    """F-061: a byte that is not valid in the encoding is U+FFFD, as Java decodes it."""
    path = _file(tmp_path, "enc.csv", data)
    assert_same(*_read_both(spark, pandas_backend, "csv", path, options))


@pytest.mark.parametrize("fmt", ["csv", "json"])
@pytest.mark.parametrize("name", ["cp1252", "latin1", "utf8", "windows-1252"])
def test_encoding_names_spark_4_refuses(spark, pandas_backend, tmp_path, fmt, name):
    """F-085: Spark 4 takes seven encoding names only; pandas refuses the rest too."""
    if int(spark.version.split(".")[0]) < 4:
        pytest.skip("Spark 3.5 takes any Java charset name; pandas follows Spark 4")
    path = _file(tmp_path, f"enc.{fmt}", b'{"a":1}\n' if fmt == "json" else b"a\n1\n")
    options = {"encoding": name}
    assert _refused(lambda: spark.read.format(fmt).options(**options).load(path).collect())
    assert _refused(lambda: pandas_backend.read_frame(fmt, path, options=options))


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
    """E-09 shape 9: NaN, infinities, -0.0, 38 digit decimals, int64 limits."""
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
    "names-differing-by-case": (
        '{"Id":1,"Name":"x"}\n{"id":2,"name":"y"}\n{"k":1,"s":{"Id":1}}\n{"k":2,"s":{"id":2.5}}\n'
    ),
    "array-lines": '[{"a":1},{"a":2,"b":"x"}]\n{"a":3}\n',
    "unicode-key-order": '{"\\ud83d\\ude00":1,"\\uff21":2,"a":3,"B":4}\n',
}


#: Cases where a value that is not a string lands in a text column. Spark 4 keeps
#: its exact source text (JSON lines); Spark 3.5 writes it back through Jackson.
#: The pandas backend follows Spark 4 (F-084).
SOURCE_TEXT_CASES = {"number-and-text", "object-and-text"}


def _spark_major(spark) -> int:
    return int(spark.version.split(".")[0])


@pytest.mark.parametrize("name", sorted(JSON_CASES))
def test_json_inference(spark, pandas_backend, tmp_path, name):
    """F-064: JSON typed as Spark's JsonInferSchema types it."""
    if name in SOURCE_TEXT_CASES and _spark_major(spark) < 4:
        pytest.skip("Spark 3.5 writes the value back through Jackson; pandas follows Spark 4")
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
# Column names
# --------------------------------------------------------------------------- #


def test_awkward_column_names_read_the_same(spark, pandas_backend, tmp_path):
    """E-09 shape 10: unicode, spaces, dots, reserved words; parquet and CSV."""
    import pyarrow.parquet as pq

    names = ["naïve", "名前", "with space", "a.b", "select", "from", "Größe", "tab\tname"]
    path = str(tmp_path / "names.parquet")
    pq.write_table(pa.table({n: pa.array([i], pa.int64()) for i, n in enumerate(names)}), path)
    spark_df, frame = _read_both(spark, pandas_backend, "parquet", path)
    assert_same(spark_df, frame)
    _same_digest(spark_df, frame)


def _refused(action) -> bool:
    try:
        action()
    except Exception:  # noqa: BLE001
        return True
    return False


@pytest.mark.parametrize("fmt", ["parquet", "json"])
def test_names_that_differ_only_by_case_are_refused(spark, pandas_backend, tmp_path, fmt):
    """F-069: Spark (caseSensitive false) refuses the read; so does pandas."""
    import pyarrow.parquet as pq

    path = str(tmp_path / f"case.{fmt}")
    if fmt == "parquet":
        pq.write_table(pa.table({"Col": [1], "col": [2]}), path)
    else:
        Path(path).write_bytes(b'{"Col":1,"col":2}\n')
    assert _refused(lambda: spark.read.format(fmt).load(path).collect())
    assert _refused(lambda: pandas_backend.read_frame(fmt, path))


def test_a_json_record_with_a_repeated_key_is_refused(spark, pandas_backend, tmp_path):
    """F-088: {"a":1,"a":"x"} gives two columns a on Spark, then a refusal."""
    path = _file(tmp_path, "dup.json", b'{"a":1,"a":"x"}\n{"a":2}\n')
    assert _refused(lambda: spark.read.json(path).collect())
    assert _refused(lambda: pandas_backend.read_frame("json", path))


def test_a_parquet_schema_read_of_names_differing_by_case(spark, pandas_backend, tmp_path):
    """F-069 (skeptic): a schema field matching two file columns is refused on both."""
    import pyarrow.parquet as pq

    path = str(tmp_path / "case.parquet")
    pq.write_table(pa.table({"id": [1], "ID": [10], "k": [5]}), path)
    assert _refused(lambda: spark.read.schema("id INT").parquet(path).collect())
    assert _refused(lambda: pandas_backend.read_frame("parquet", path, schema="id INT"))
    assert_same(*_read_both(spark, pandas_backend, "parquet", path, {}, "k BIGINT"))


def test_a_frame_with_names_that_differ_only_by_case_is_not_written(
    spark, pandas_backend, tmp_path
):
    """F-069: Spark refuses to write such a frame; so does pandas."""
    from ubunye.core.write_modes import ResolvedWriteMode

    df = spark.createDataFrame([(1, 2)], "Col INT, col INT")
    assert _refused(lambda: df.write.parquet(str(tmp_path / "spark")))
    assert _refused(
        lambda: pandas_backend.execute_write(
            pd.DataFrame({"Col": [1], "col": [2]}),
            ResolvedWriteMode(mode="overwrite", save_mode="overwrite"),
            connector="s3",
            file_format="parquet",
            path=str(tmp_path / "pandas"),
        )
    )


# --------------------------------------------------------------------------- #
# Empty inputs
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_empty_file_with_a_schema(spark, pandas_backend, tmp_path, fmt):
    """F-068: an empty file read with a schema is zero rows of that schema."""
    path = _file(tmp_path, f"zero.{fmt}", b"")
    assert_same(*_read_both(spark, pandas_backend, fmt, path, {}, "id INT, t TIMESTAMP"))


def test_empty_folder_with_a_schema(spark, pandas_backend, tmp_path):
    folder = tmp_path / "none"
    folder.mkdir()
    (folder / "_SUCCESS").write_bytes(b"")
    assert_same(*_read_both(spark, pandas_backend, "parquet", str(folder), {}, "id BIGINT"))


def test_zero_rows_with_a_schema_and_a_header_only_csv(spark, pandas_backend, tmp_path):
    import pyarrow.parquet as pq

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    path = str(tmp_path / "zero_rows.parquet")
    pq.write_table(schema.empty_table(), path)
    assert_same(*_read_both(spark, pandas_backend, "parquet", path))
    header = _file(tmp_path, "header.csv", b"id,name\n")
    options = {"header": "true", "inferSchema": "true"}
    assert_same(*_read_both(spark, pandas_backend, "csv", header, options))


@pytest.mark.xfail(
    strict=False,
    reason="F-068, not fixed: Spark is expected to read an empty CSV or JSON file "
    "with no schema as zero rows and no columns; the pandas backend refuses it",
)
@pytest.mark.parametrize("fmt", ["csv", "json"])
def test_empty_file_without_a_schema(spark, pandas_backend, tmp_path, fmt):
    path = _file(tmp_path, f"zero.{fmt}", b"")
    try:
        spark_df = spark.read.format(fmt).load(path)
        spark_shape = (spark_df.columns, spark_df.count())
    except Exception:  # noqa: BLE001  Spark refuses too: then pandas must refuse
        spark_shape = "refused"
    try:
        frame = pandas_backend.read_frame(fmt, path)
        pandas_shape = (list(frame.native.columns), frame.count())
    except Exception:  # noqa: BLE001
        pandas_shape = "refused"
    assert spark_shape == pandas_shape


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


# --------------------------------------------------------------------------- #
# Open questions: filed, not fixed. xfail(strict=False) so CI shows Spark's side.
# --------------------------------------------------------------------------- #


@pytest.mark.xfail(strict=False, reason="F-077: Spark reads looser date and time forms")
@pytest.mark.parametrize(
    "text",
    [
        "d\n2024-1-2\n2024-12-31\n",
        "d\n2024-01\n2024-02\n",
        "t\n2024-01-02 03:04:05.123456789\n",
        "t\n2024-01-02 3:04:05\n",
    ],
    ids=["single-digit-month", "year-month", "nanoseconds", "single-digit-hour"],
)
def test_csv_looser_date_and_time_forms(spark, pandas_backend, tmp_path, text):
    path = _file(tmp_path, "loose.csv", text.encode())
    options = {"header": "true", "inferSchema": "true"}
    assert_same(*_read_both(spark, pandas_backend, "csv", path, options))


@pytest.mark.xfail(strict=False, reason="F-078: Spark matches CSV columns by position")
def test_csv_folder_with_headers_in_different_orders(spark, pandas_backend, tmp_path):
    folder = tmp_path / "parts"
    folder.mkdir()
    (folder / "a.csv").write_bytes(b"x,y\n1,2\n")
    (folder / "b.csv").write_bytes(b"y,x\n3,4\n")
    assert_same(*_read_both(spark, pandas_backend, "csv", str(folder), {"header": "true"}))


@pytest.mark.xfail(strict=False, reason="F-079: Spark takes one file's parquet schema")
def test_parquet_folder_with_different_schemas(spark, pandas_backend, tmp_path):
    import pyarrow.parquet as pq

    folder = tmp_path / "parts"
    folder.mkdir()
    pq.write_table(pa.table({"a": [1], "b": ["x"]}), folder / "part-0.parquet")
    pq.write_table(pa.table({"a": [2], "c": [1.5]}), folder / "part-1.parquet")
    assert_same(*_read_both(spark, pandas_backend, "parquet", str(folder)))


@pytest.mark.xfail(strict=False, reason="F-080: pandas writes a timestamp_ntz as an instant")
def test_timestamp_ntz_round_trip(spark, pandas_backend, tmp_path):
    import datetime as dt

    import pyarrow.parquet as pq

    from ubunye.core.write_modes import ResolvedWriteMode

    source = str(tmp_path / "ntz.parquet")
    moments = [dt.datetime(2024, 3, 10, 2, 30), dt.datetime(1850, 1, 1), dt.datetime(2300, 1, 1)]
    pq.write_table(pa.table({"t": pa.array(moments, pa.timestamp("us"))}), source)
    spark.read.parquet(source).write.parquet(str(tmp_path / "spark"))
    pandas_backend.execute_write(
        pandas_backend.read_frame("parquet", source),
        ResolvedWriteMode(mode="overwrite", save_mode="overwrite"),
        connector="s3",
        file_format="parquet",
        path=str(tmp_path / "pandas"),
    )
    from .test_pandas_backend_parity import _rows, _schema, spark_arrow

    left = spark_arrow(spark.read.parquet(str(tmp_path / "spark")))
    right = spark_arrow(spark.read.parquet(str(tmp_path / "pandas")))
    assert _schema(left) == _schema(right)
    assert _rows(left) == _rows(right)


@pytest.mark.xfail(strict=False, reason="F-081: pandas cannot hash an instant past year 9999")
def test_digest_of_instants_outside_python_years(spark, pandas_backend, tmp_path):
    import pyarrow.parquet as pq

    # 9999-12-31 23:59:59 in New York is 10000-01-01 04:59:59 UTC.
    micros = [253402318799 * 10**6 + 5 * 3600 * 10**6, 0]
    path = str(tmp_path / "far.parquet")
    pq.write_table(pa.table({"t": pa.array(micros, pa.timestamp("us", tz="UTC"))}), path)
    spark_df, frame = _read_both(spark, pandas_backend, "parquet", path)
    _same_digest(spark_df, frame)


# --------------------------------------------------------------------------- #
# End to end: one task, both engines, the run records' digests
# --------------------------------------------------------------------------- #

TRANSFORM = """\
import narwhals as nw

from ubunye.core.interfaces import Task


class Touch(Task):
    def transform(self, sources):
        frame = nw.from_native(sources["src"])
        return {"out": frame.with_columns(e08_one=nw.lit(1, dtype=nw.Int64))}
"""


def _e2e_inputs(root: Path) -> dict:
    """Small versions of each E-09 shape: (file_format, path, options)."""
    import pyarrow.parquet as pq

    data = root / "data"
    data.mkdir()
    cases = {}
    wide = pa.table({f"c{i:04d}": pa.array([i, i + 1, None], pa.int64()) for i in range(1200)})
    pq.write_table(wide, data / "wide.parquet")
    cases["wide"] = ("parquet", data / "wide.parquet", {})
    text = _long_text(300_000) + "\x0b\x1f\x7f "
    pq.write_table(pa.table({"id": [1, 2], "txt": [text, "x\x00y"]}), data / "long.parquet")
    cases["long-text"] = ("parquet", data / "long.parquet", {})
    (data / "nested.jsonl").write_text(_nested_lines(), encoding="utf-8")
    cases["nested-json"] = ("json", data / "nested.jsonl", {})
    (data / "conflicts.jsonl").write_text(
        JSON_CASES["number-and-text"] + JSON_CASES["big-whole-numbers"], encoding="utf-8"
    )
    cases["conflicting-json"] = ("json", data / "conflicts.jsonl", {})
    (data / "tz.csv").write_text(DST_TEXT, encoding="utf-8")
    cases["time-zones"] = ("csv", data / "tz.csv", {"header": "true", "inferSchema": "true"})
    pq.write_table(_special_numbers(), data / "special.parquet")
    cases["special-numbers"] = ("parquet", data / "special.parquet", {})
    (data / "messy.csv").write_bytes(
        b"id,amount,amount,,spaced\n1,9223372036854775808,5, x,1\n2,1, 6,y, 2\n"
    )
    cases["messy-csv"] = ("csv", data / "messy.csv", {"header": "true", "inferSchema": "true"})
    many = data / "many"
    many.mkdir()
    for i in range(300):
        pq.write_table(pa.table({"id": [i], "v": [i / 7]}), many / f"part-{i:05d}.parquet")
    cases["many-files"] = ("parquet", many, {})
    part = data / "partitioned"
    for i in range(60):
        leaf = part / f"p={i % 6}" / f"q={i // 6}"
        leaf.mkdir(parents=True)
        pq.write_table(pa.table({"id": [i]}), leaf / "part-0.parquet")
    cases["partitioned"] = ("parquet", part, {})
    (data / "empty.csv").write_bytes(b"id,name\n")
    cases["header-only"] = ("csv", data / "empty.csv", {"header": "true", "inferSchema": "true"})
    return cases


@pytest.fixture(scope="module")
def e2e(tmp_path_factory, spark):
    """Every E-09 shape run as a task on both engines, with --lineage."""
    import ubunye
    from ubunye.backends.databricks_backend import DatabricksBackend
    from ubunye.lineage.storage import FileSystemLineageStore

    root = tmp_path_factory.mktemp("e08")
    results = {}
    for name, (fmt, path, options) in _e2e_inputs(root).items():
        task = root / "uc" / "e08" / name.replace("-", "_")
        task.mkdir(parents=True)
        (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
        opts = "".join(f'        {k}: "{v}"\n' for k, v in options.items())
        (task / "config.yaml").write_text(
            "MODEL: etl\n"
            'VERSION: "1.0.0"\n'
            "ENGINE:\n  spark_conf:\n"
            f'    spark.sql.session.timeZone: "{ZONE}"\n'
            "CONFIG:\n  inputs:\n    src:\n      format: s3\n"
            f'      path: "{path.as_posix()}"\n      file_format: {fmt}\n'
            + (f"      options:\n{opts}" if opts else "")
            + "  transform: {}\n  outputs:\n    out:\n      format: s3\n"
            '      path: "{{ task_dir }}/output/{{ backend }}"\n'
            "      file_format: parquet\n      mode: overwrite\n",
            encoding="utf-8",
        )
        ubunye.run_task(
            str(task),
            backend=DatabricksBackend(spark=spark),
            lineage=True,
            variables={"backend": "spark"},
        )
        ubunye.run_task(
            str(task),
            backend=PandasBackend(timezone=ZONE),
            lineage=True,
            variables={"backend": "pandas"},
        )
        store = FileSystemLineageStore(str(root / ".ubunye" / "lineage"))
        records = store.list_runs(f"uc/e08/{task.name}")
        results[name] = {r.backend: r for r in records}
    return results


E2E_SHAPES = [
    "wide",
    "long-text",
    "nested-json",
    "conflicting-json",
    "time-zones",
    "special-numbers",
    "messy-csv",
    "many-files",
    "partitioned",
    "header-only",
]


@pytest.mark.parametrize("shape", E2E_SHAPES)
def test_the_same_task_gives_the_same_run_record(e2e, shape):
    """E-09: rows, schema hash and rows-v1 digest match, input and output."""
    if shape == "conflicting-json" and _spark_major(SparkSession.getActiveSession()) < 4:
        pytest.skip("Spark 3.5 writes a number in a text column back through Jackson (F-084)")
    by_backend = e2e[shape]
    assert set(by_backend) == {"databricks", "pandas"}
    spark_run, pandas_run = by_backend["databricks"], by_backend["pandas"]
    assert spark_run.status == pandas_run.status == "success"
    for side in ("inputs", "outputs"):
        for left, right in zip(getattr(spark_run, side), getattr(pandas_run, side)):
            assert left.data_hash and left.data_hash.startswith("sha256:"), (side, left)
            assert (left.row_count, left.schema_hash, left.data_hash) == (
                right.row_count,
                right.schema_hash,
                right.data_hash,
            ), (shape, side)
