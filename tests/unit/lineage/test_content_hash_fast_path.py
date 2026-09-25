"""The column-at-a-time hash must equal the row-at-a-time reference, byte for byte.

``fingerprint_arrow`` builds each column's text once (the fast path);
``fingerprint_rows`` + ``canonical_line`` is the original, row at a time. If they
ever differ, pandas and Spark would disagree about the same data, so every run
record's data hash would be wrong.
"""

from __future__ import annotations

import datetime as dt
import math

import pytest

pa = pytest.importorskip("pyarrow")
hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from ubunye.lineage.content_hash import (  # noqa: E402
    arrow_kind,
    fingerprint_arrow,
    fingerprint_rows,
)


def _reference(table):
    schema = [(f.name, arrow_kind(f.type)) for f in table.schema]
    return fingerprint_rows(table.to_pylist(), schema)


def test_awkward_values_hash_the_same_both_ways():
    table = pa.table(
        {
            "i8": pa.array([1, None, -128], pa.int8()),
            "i64": pa.array([2**62, -(2**63), None], pa.int64()),
            "s": pa.array(['plain "quoted"', "éé \\ back\nline", None]),
            "b": pa.array([True, False, None]),
            "f": pa.array([0.1, -0.0, float("nan")]),
            "big": pa.array([1e7, 1.5e-4, float("inf")]),
            "d": pa.array([dt.date(2024, 1, 31), None, dt.date(1970, 1, 1)]),
            "ts": pa.array(
                [dt.datetime(2024, 1, 1, 12, 0, 0, 123456), None, dt.datetime(1999, 12, 31)],
                pa.timestamp("us", tz="UTC"),
            ),
            "f32": pa.array([0.1, None, 3.5], pa.float32()),
            "lst": pa.array([[1, 2], None, []], pa.list_(pa.int64())),
            "st": pa.array([{"a": 1, "b": "x"}, None, {"a": None, "b": "y"}]),
            "dec": pa.array([None, None, None], pa.decimal128(10, 2)),
        }
    )
    assert fingerprint_arrow(table) == _reference(table)


columns = st.fixed_dictionaries(
    {
        "n": st.lists(
            st.one_of(st.none(), st.integers(-(2**63), 2**63 - 1)), min_size=5, max_size=5
        ),
        "s": st.lists(st.one_of(st.none(), st.text()), min_size=5, max_size=5),
        "f": st.lists(st.one_of(st.none(), st.floats()), min_size=5, max_size=5),
        "b": st.lists(st.one_of(st.none(), st.booleans()), min_size=5, max_size=5),
    }
)


@settings(max_examples=150, deadline=None)
@given(columns)
def test_any_values_hash_the_same_both_ways(cols):
    table = pa.table(
        {
            "n": pa.array(cols["n"], pa.int64()),
            "s": pa.array(cols["s"], pa.string()),
            "f": pa.array(cols["f"], pa.float64()),
            "b": pa.array(cols["b"], pa.bool_()),
        }
    )
    assert fingerprint_arrow(table) == _reference(table)


def test_nan_is_still_not_null():
    with_nan = pa.table({"f": pa.array([math.nan], pa.float64())})
    with_null = pa.table({"f": pa.array([None], pa.float64())})
    assert fingerprint_arrow(with_nan).data_hash != fingerprint_arrow(with_null).data_hash
