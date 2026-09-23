"""The pandas backend against real Spark: same types, same values, same files.

"The same folder runs on Spark or on pandas" is only true if, for the same
input and the same config, both engines produce the same columns, the same
types and the same values, and each can read what the other wrote. This module
checks exactly that against a real SparkSession, with nothing compared as
strings: schemas are compared as Arrow types and rows as Python values.

1. Reads: every reader case, Spark's DataFrame vs the pandas backend's frame.
2. Interchange: what pandas writes, Spark reads identically, and the reverse.
3. Text: CSV and JSON files are byte for byte what Spark writes.
4. The engine: one task run end to end on both backends, outputs equal.

Marked integration (the Spark half needs a JVM).
"""

from __future__ import annotations

import datetime as dt
import decimal
import glob
import math
import os
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")

from pyspark.sql import SparkSession  # noqa: E402

from ubunye.api import run_task  # noqa: E402

# The ambient-session backend reuses the shared integration SparkSession, and its
# stop() is a no-op: a SparkBackend here would stop the session-scoped fixture.
from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.write_modes import ResolvedWriteMode  # noqa: E402

pytestmark = pytest.mark.integration

ZONE = "Africa/Johannesburg"  # not UTC, so a timezone slip cannot hide
OVERWRITE = ResolvedWriteMode(mode="overwrite", save_mode="overwrite")


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    before = session.conf.get("spark.sql.session.timeZone")
    session.conf.set("spark.sql.session.timeZone", ZONE)
    yield session
    session.conf.set("spark.sql.session.timeZone", before)


@pytest.fixture
def pandas_backend():
    return PandasBackend(timezone=ZONE)


# --------------------------------------------------------------------------- #
# Typed comparison
# --------------------------------------------------------------------------- #


def _canon_type(t):
    """Arrow types with the cosmetic differences between producers removed."""
    if pa.types.is_timestamp(t):
        return pa.timestamp("us", tz="UTC" if t.tz else None)
    if pa.types.is_large_string(t):
        return pa.string()
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return pa.list_(_canon_type(t.value_type))
    if pa.types.is_struct(t):
        return pa.struct([pa.field(f.name, _canon_type(f.type)) for f in t])
    return t


def _canon(table):
    fields = [pa.field(f.name, _canon_type(f.type)) for f in table.schema]
    return table.cast(pa.schema(fields)) if fields else table


def _schema(table):
    return [(f.name, f.type) for f in _canon(table).schema]


def _key(value):
    if value is None:
        return (0, "")
    if isinstance(value, float) and math.isnan(value):
        return (1, "nan")
    return (2, repr(value))


def _rows(table):
    rows = [tuple(r.values()) for r in _canon(table).to_pylist()]
    return sorted(rows, key=lambda r: tuple(_key(v) for v in r))


def _same_values(a, b):
    assert len(a) == len(b)
    for x, y in zip(a, b):
        for u, v in zip(x, y):
            if isinstance(u, float) and isinstance(v, float) and math.isnan(u):
                assert math.isnan(v)
            else:
                assert u == v, (x, y)


def spark_arrow(df):
    return df.toArrow()


def pandas_arrow(frame):
    native = frame.native if hasattr(frame, "native") else frame
    return pa.Table.from_pandas(native, preserve_index=False)


def assert_same(spark_df, pandas_frame):
    left, right = spark_arrow(spark_df), pandas_arrow(pandas_frame)
    assert _schema(left) == _schema(right)
    _same_values(_rows(left), _rows(right))


# --------------------------------------------------------------------------- #
# 1. Reads
# --------------------------------------------------------------------------- #

CSV = (
    "small,big,dbl,flag,txt,empty,day,ts,mixed\n"
    "1,3000000000,1.5,true,a,,2024-01-02,2024-01-02 03:04:05,1\n"
    "2,4000000000,2.0,False,b,,2024-02-03,2024-02-03T01:02:03,x\n"
    ",5,,,,,,,\n"
    '-7,6,0.1,TRUE,"quoted, text",,2024-03-04,2024-03-04 00:00:00,y\n'
)
JSON_LINES = (
    '{"z":1,"a":"x","f":1.5,"b":true,"n":null,"arr":[1,2],'
    '"obj":{"k":1,"c":"q"},"ts":"2024-01-02T03:04:05"}\n'
    '{"z":3000000000,"a":"y","arr":[],"obj":{"k":2}}\n'
    '{"f":2,"a":"z"}\n'
)
JSON_ARRAY = '[{"b": 1, "a": "x"}, {"a": "y", "c": [1.5, 2.5]}]'
BAD_ROWS = "a,b\n1,2\n3,4,5\n6\n7,8\n"  # one row too long, one too short
BAD_JSON = '{"a":1}\nnot json\n{"a":2}\n'


def test_failfast_stops_on_both_engines(spark, pandas_backend, tmp_path):
    from ubunye.core.errors import SourceReadError

    src = tmp_path / "bad.csv"
    src.write_text(BAD_ROWS, encoding="utf-8")
    options = {"header": "true", "mode": "FAILFAST"}
    with pytest.raises(Exception):
        spark.read.options(**options).csv(str(src)).collect()
    with pytest.raises(SourceReadError):
        pandas_backend.read_frame("csv", str(src), options=options)


READ_CASES = [
    ("csv", CSV, {}, None),
    ("csv", CSV, {"header": "true"}, None),
    ("csv", CSV, {"header": "true", "inferSchema": "true"}, None),
    (
        "csv",
        CSV,
        {"header": "true"},
        "small INT, big BIGINT, dbl DOUBLE, flag BOOLEAN, txt STRING, "
        "empty STRING, day DATE, ts TIMESTAMP, mixed STRING",
    ),
    (
        "csv",
        "a;b\n1;NA\nNA;2\n",
        {"header": "true", "sep": ";", "nullValue": "NA"},
        None,
    ),
    ("json", JSON_LINES, {}, None),
    ("json", JSON_ARRAY, {"multiLine": "true"}, None),
    ("json", JSON_LINES, {}, "a STRING, z BIGINT, f DOUBLE, missing STRING"),
    # Rows with too many and too few fields, in each parse mode Spark offers.
    ("csv", BAD_ROWS, {"header": "true"}, None),
    ("csv", BAD_ROWS, {"header": "true", "mode": "PERMISSIVE", "inferSchema": "true"}, None),
    ("csv", BAD_ROWS, {"header": "true", "mode": "DROPMALFORMED"}, None),
    ("json", BAD_JSON, {"mode": "DROPMALFORMED"}, None),
]


@pytest.mark.parametrize(
    "fmt, text, options, schema",
    READ_CASES,
    ids=[
        "csv-plain",
        "csv-header",
        "csv-infer",
        "csv-schema",
        "csv-sep-null",
        "json-lines",
        "json-multiline",
        "json-schema",
        "csv-permissive-default",
        "csv-permissive-infer",
        "csv-dropmalformed",
        "json-dropmalformed",
    ],
)
def test_reads_match_spark(spark, pandas_backend, tmp_path, fmt, text, options, schema):
    src = tmp_path / f"in.{fmt}"
    src.write_text(text, encoding="utf-8")
    reader = spark.read.format(fmt).options(**options)
    if schema:
        reader = reader.schema(schema)
    spark_df = reader.load(str(src))
    frame = pandas_backend.read_frame(fmt, str(src), options=options, schema=schema)
    assert_same(spark_df, frame)


def test_reads_a_folder_spark_wrote(spark, pandas_backend, tmp_path):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(_write_text(tmp_path / "in.csv", CSV))
    )
    for fmt in ("parquet", "json", "csv"):
        out = str(tmp_path / f"spark_{fmt}")
        df.repartition(3).write.format(fmt).save(out)
        options = {"inferSchema": "true"} if fmt == "csv" else {}
        assert_same(
            spark.read.format(fmt).options(**options).load(out),
            pandas_backend.read_frame(fmt, out, options=options),
        )


def _write_text(path: Path, text: str) -> str:
    path.write_text(text, encoding="utf-8")
    return str(path)


# --------------------------------------------------------------------------- #
# 2. Interchange: each reads what the other wrote
# --------------------------------------------------------------------------- #


def _typed_frame():
    """One of every scalar type a task commonly produces, with nulls."""
    table = pa.table(
        {
            "i32": pa.array([1, None, -3], pa.int32()),
            "i64": pa.array([3000000000, 5, None], pa.int64()),
            "dbl": pa.array([0.1, 2.0, None], pa.float64()),
            "flag": pa.array([True, None, False]),
            "txt": pa.array(["plain", "a,b", None]),
            "day": pa.array([dt.date(2024, 1, 2), None, dt.date(1999, 12, 31)]),
            "ts": pa.array(
                [
                    dt.datetime(2024, 1, 2, 1, 4, 5, 123000, tzinfo=dt.timezone.utc),
                    None,
                    dt.datetime(1999, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                ],
                pa.timestamp("us", tz="UTC"),
            ),
            "dec": pa.array(
                [decimal.Decimal("1.50"), None, decimal.Decimal("-2.25")],
                pa.decimal128(10, 2),
            ),
        }
    )
    return table.to_pandas(types_mapper=pd.ArrowDtype)


@pytest.mark.parametrize("fmt", ["parquet", "csv", "json"])
def test_spark_reads_what_pandas_wrote(spark, pandas_backend, tmp_path, fmt):
    frame = _typed_frame()
    if fmt != "parquet":
        frame = frame.drop(columns=["dec"])  # text formats: Spark reads it back as double
    out = str(tmp_path / "out")
    options = {"header": "true"} if fmt == "csv" else None
    pandas_backend.execute_write(
        frame, OVERWRITE, connector="s3", file_format=fmt, path=out, options=options
    )
    read_options = {"header": "true", "inferSchema": "true"} if fmt == "csv" else {}
    assert_same(
        spark.read.format(fmt).options(**read_options).load(out),
        pandas_backend.read_frame(fmt, out, options=read_options),
    )
    if fmt == "parquet":  # a binary format keeps every type exactly
        assert_same(spark.read.parquet(out), frame)


@pytest.mark.parametrize("fmt", ["parquet", "csv", "json"])
def test_pandas_reads_what_spark_wrote(spark, pandas_backend, tmp_path, fmt):
    source = str(tmp_path / "source")
    pandas_backend.execute_write(
        _typed_frame(), OVERWRITE, connector="s3", file_format="parquet", path=source
    )
    df = spark.read.parquet(source)
    if fmt != "parquet":
        df = df.drop("dec")
    out = str(tmp_path / "out")
    writer = df.write.format(fmt)
    if fmt == "csv":
        writer = writer.option("header", "true")
    writer.save(out)
    read_options = {"header": "true", "inferSchema": "true"} if fmt == "csv" else {}
    assert_same(
        spark.read.format(fmt).options(**read_options).load(out),
        pandas_backend.read_frame(fmt, out, options=read_options),
    )


# --------------------------------------------------------------------------- #
# 3. Text files are what Spark writes
# --------------------------------------------------------------------------- #


def _part_text(folder: str) -> str:
    (part,) = glob.glob(os.path.join(folder, "part-*"))
    with open(part, encoding="utf-8", newline="") as handle:
        return handle.read().replace("\r\n", "\n")  # Spark uses CRLF on Windows


@pytest.mark.parametrize("fmt", ["csv", "json"])
def test_text_is_byte_for_byte_spark(spark, pandas_backend, tmp_path, fmt):
    table = pa.table(
        {
            "s": ["plain", "a,b", 'say "hi"', "", None, "back\\slash", " pad "],
            "d": [2.0, 1e10, 1e-5, float("nan"), None, 1 / 3, -0.0],
            "b": [True, False, None, True, False, None, True],
            "n": pa.array([1, None, 3, 4, 5, 6, 7], pa.int64()),
            "ts": pa.array(
                [dt.datetime(2024, 1, 2, 1, 4, 5, 123456, tzinfo=dt.timezone.utc)] * 7,
                pa.timestamp("us", tz="UTC"),
            ),
            "day": pa.array([dt.date(2024, 1, 2)] * 7),
        }
    )
    frame = table.to_pandas(types_mapper=pd.ArrowDtype)
    source = str(tmp_path / "source")
    pandas_backend.execute_write(
        frame, OVERWRITE, connector="s3", file_format="parquet", path=source
    )
    spark_out, pandas_out = str(tmp_path / "spark"), str(tmp_path / "pandas")
    spark.read.parquet(source).coalesce(1).write.format(fmt).save(spark_out)
    pandas_backend.execute_write(frame, OVERWRITE, connector="s3", file_format=fmt, path=pandas_out)
    assert _part_text(pandas_out) == _part_text(spark_out)


# --------------------------------------------------------------------------- #
# 4. The engine end to end
# --------------------------------------------------------------------------- #

_TRANSFORM = '''\
from ubunye.core.interfaces import Task


class Passthrough(Task):
    """Read one input, write it out unchanged: backend-agnostic on purpose."""

    def transform(self, sources):
        return {"out": sources["in"]}
'''


def _task(root: Path, in_uri: str) -> Path:
    task = root / "parity" / "io" / "copy"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(_TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
MODEL: "etl"
VERSION: "1.0.0"
ENGINE:
  spark_conf:
    spark.sql.shuffle.partitions: "1"
    spark.sql.session.timeZone: "{ZONE}"
CONFIG:
  inputs:
    in:
      format: s3
      path: "{in_uri}"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{{{{ env.PARITY_OUT }}}}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


def test_same_task_same_data_on_spark_and_pandas(spark, tmp_path, monkeypatch):
    src = tmp_path / "in.csv"
    src.write_text(CSV, encoding="utf-8")
    task = _task(tmp_path, src.as_uri())

    monkeypatch.setenv("PARITY_OUT", (tmp_path / "spark_out").as_uri())
    run_task(str(task), mode="DEV", backend=DatabricksBackend(spark=spark))
    monkeypatch.setenv("PARITY_OUT", (tmp_path / "pandas_out").as_uri())
    run_task(str(task), mode="DEV", backend=PandasBackend(timezone=ZONE))

    spark_side = spark.read.parquet(str(tmp_path / "spark_out"))
    pandas_side = spark.read.parquet(str(tmp_path / "pandas_out"))
    assert spark_side.count() == 4
    assert _schema(spark_arrow(spark_side)) == _schema(spark_arrow(pandas_side))
    _same_values(_rows(spark_arrow(spark_side)), _rows(spark_arrow(pandas_side)))
