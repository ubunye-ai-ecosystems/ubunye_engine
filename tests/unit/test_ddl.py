"""Spark DDL schema strings read into Arrow schemas (ubunye.adapters.ddl)."""

from __future__ import annotations

import pytest

pa = pytest.importorskip("pyarrow")

from ubunye.adapters import ddl  # noqa: E402


@pytest.mark.parametrize(
    "spark_type, arrow",
    [
        ("INT", pa.int32()),
        ("integer", pa.int32()),
        ("BIGINT", pa.int64()),
        ("long", pa.int64()),
        ("SMALLINT", pa.int16()),
        ("TINYINT", pa.int8()),
        ("DOUBLE", pa.float64()),
        ("FLOAT", pa.float32()),
        ("BOOLEAN", pa.bool_()),
        ("STRING", pa.string()),
        ("VARCHAR(20)", pa.string()),
        ("CHAR(3)", pa.string()),
        ("BINARY", pa.binary()),
        ("DATE", pa.date32()),
        ("TIMESTAMP", pa.timestamp("us", tz="UTC")),
        ("TIMESTAMP_NTZ", pa.timestamp("us")),
        ("DECIMAL(10,2)", pa.decimal128(10, 2)),
        ("DECIMAL", pa.decimal128(10, 0)),  # Spark's default precision
        ("decimal( 38 , 18 )", pa.decimal128(38, 18)),
    ],
)
def test_scalar_types(spark_type, arrow):
    assert ddl.arrow_type(spark_type) == arrow


def test_a_full_schema_keeps_order():
    schema = ddl.parse("id INT, name STRING, amount DECIMAL(10,2)")
    assert schema.names == ["id", "name", "amount"]
    assert schema.field("amount").type == pa.decimal128(10, 2)


def test_backticks_allow_spaces_and_commas_in_names():
    schema = ddl.parse("`first name` STRING, `a,b` INT")
    assert schema.names == ["first name", "a,b"]


def test_colon_form():
    assert ddl.parse("id: INT").names == ["id"]


@pytest.mark.parametrize(
    "text, message",
    [
        ("", "empty"),
        ("id", "no type"),
        ("a MAP<STRING,INT>", "MAP<STRING,INT>"),
        ("a ARRAY<INT>", "ARRAY<INT>"),
        ("a STRUCT<x: INT>", "STRUCT"),
        ("a INT NOT NULL", "NOT NULL"),
        ("a INT, a STRING", "duplicate"),
        ("a WIBBLE", "unsupported type WIBBLE"),
    ],
)
def test_refusals_name_the_problem(text, message):
    with pytest.raises(ValueError, match=message):
        ddl.parse(text)
