"""CSV quoting: the pandas backend splits every line the way Spark splits it.

Spark reads CSV with the univocity parser, which treats quotes it did not
expect in its own way (a doubled quote is not an escape, text after a closing
quote is kept). The pandas backend reads such files with a port of that parser
(ubunye.adapters.spark_csv). This module fuzzes the whole read path against a
live Spark session: random lines built from quotes, backslashes, delimiters and
blanks, for each escape setting and line ending, one line per record and
multi-line. The seeds are fixed, so a failure reproduces.

Marked integration (needs a JVM).
"""

from __future__ import annotations

import random

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from pyspark.sql import SparkSession  # noqa: E402

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402

pytestmark = pytest.mark.integration

WIDTH = 8
SCHEMA = ", ".join(f"c{i} STRING" for i in range(WIDTH))
ESCAPES = {"backslash": "\\", "quote": '"', "none": ""}
ENDINGS = {"lf": "\n", "crlf": "\r\n"}
CASES = [(e, n) for e in sorted(ESCAPES) for n in sorted(ENDINGS)]


@pytest.fixture(scope="module")
def spark():
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _lines(seed: int, count: int, newlines: bool) -> list:
    rnd = random.Random(seed)
    alphabet = ["a", "b", ",", '"', '"', '"', "\\", " ", "\t"] + (["\n"] if newlines else [])
    return ["".join(rnd.choice(alphabet) for _ in range(rnd.randint(1, 12))) for _ in range(count)]


def _write(path, lines, ending):
    # Bytes, so the line ending is the one asked for on every OS.
    path.write_bytes((ending.join(lines) + ending).replace("\n", ending).encode("utf-8"))
    return path


def _spark_rows(spark, path, options):
    reader = spark.read.schema(SCHEMA)
    for key, value in options.items():
        reader = reader.option(key, value)
    return [tuple(r) for r in reader.csv(str(path)).collect()]


def _pandas_rows(path, options):
    frame = PandasBackend().read_frame("csv", str(path), options=options, schema=SCHEMA).native
    return [tuple(None if pd.isna(v) else v for v in row) for row in frame.itertuples(index=False)]


@pytest.mark.parametrize("escape,ending", CASES)
def test_random_lines_split_as_spark_splits_them(spark, tmp_path, escape, ending):
    seed = CASES.index((escape, ending))
    path = _write(tmp_path / "lines.csv", _lines(seed, 1500, False), ENDINGS[ending])
    options = {"escape": ESCAPES[escape]}
    assert _pandas_rows(path, options) == _spark_rows(spark, path, options)


@pytest.mark.parametrize("escape,ending", CASES)
def test_random_multiline_records_split_as_spark_splits_them(spark, tmp_path, escape, ending):
    # Many small files: one stray quote swallows the rest of a multi-line file.
    options = {"escape": ESCAPES[escape], "multiLine": "true"}
    for n in range(30):
        seed = 1000 * CASES.index((escape, ending)) + n
        path = _write(tmp_path / f"multi{n}.csv", _lines(seed, 4, True), ENDINGS[ending])
        assert _pandas_rows(path, options) == _spark_rows(spark, path, options), path.read_bytes()


FIELDS = [
    "1",
    "a b",
    '"a, b"',
    '"x"',
    "",
    '""',
    '"a""b"',
    '"a\\"b"',
    "a\\b",
    '"q" ',
    ' "q"',
    '"q"r',
]


@pytest.mark.parametrize("escape,ending", CASES)
def test_rows_of_mostly_plain_fields(spark, tmp_path, escape, ending):
    # Real files: most fields plain, now and then one Spark reads its own way.
    # Plain lines are passed through as they are and odd ones split, so the
    # two kinds must mix without shifting a row.
    rnd = random.Random(CASES.index((escape, ending)))
    weights = [30, 30, 20, 20, 10, 3, 3, 3, 3, 2, 2, 2]
    lines = [",".join(rnd.choices(FIELDS, weights, k=rnd.randint(1, WIDTH))) for _ in range(3000)]
    path = _write(tmp_path / "rows.csv", lines, ENDINGS[ending])
    options = {"escape": ESCAPES[escape]}
    assert _pandas_rows(path, options) == _spark_rows(spark, path, options)


def test_titanic_style_names_with_header_and_inferred_types(spark, tmp_path):
    # The shape that exposed the bug: quoted names with doubled quotes inside.
    lines = [
        "PassengerId,Survived,Name,Age",
        '23,1,"McGowan, Miss. Anna ""Annie""",15',
        '102,0,"Petroff, Mr. Pastcho (""Pentcho"")",',
        '1,0,"Braund, Mr. Owen Harris",22',
    ]
    path = _write(tmp_path / "titanic.csv", lines, "\n")
    options = {"header": "true", "inferSchema": "true"}
    expected = spark.read.options(**options).csv(str(path))
    got = PandasBackend().read_frame("csv", str(path), options=options).native
    assert list(got.columns) == expected.columns
    assert [tuple(None if pd.isna(v) else v for v in r) for r in got.itertuples(index=False)] == [
        tuple(r) for r in expected.collect()
    ]
