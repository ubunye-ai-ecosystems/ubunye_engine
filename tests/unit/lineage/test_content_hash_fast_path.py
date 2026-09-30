"""The vectorised hash must equal the row-at-a-time reference, byte for byte.

``fingerprint_arrow`` builds each slice's canonical lines with Arrow compute and
hashes them from Arrow's buffer (the fast path); ``fingerprint_rows`` +
``canonical_line`` is the original, row at a time. If they ever differ, pandas
and Spark would disagree about the same data, and every recorded ``rows-v1``
digest would move.
"""

from __future__ import annotations

import datetime as dt
import decimal
import math

import pytest

pa = pytest.importorskip("pyarrow")
hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from ubunye.lineage import content_hash as ch  # noqa: E402
from ubunye.lineage.content_hash import (  # noqa: E402
    arrow_kind,
    canonical_line,
    fingerprint_arrow,
    fingerprint_rows,
)


def _reference(table):
    schema = [(f.name, arrow_kind(f.type)) for f in table.schema]
    return fingerprint_rows(table.to_pylist(), schema)


@pytest.fixture
def small_slices(monkeypatch):
    """Slices of 3 rows, so every table crosses slices."""
    monkeypatch.setattr(ch, "_SLICE_ROWS", 3)


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


# --------------------------------------------------------------------------- #
# Every kind the engine meets, with the values that break text encoders
# --------------------------------------------------------------------------- #

ROWS = 7
_UTC = dt.timezone.utc
_awkward_text = st.one_of(
    st.text(),
    st.text(
        alphabet=st.sampled_from(['"', "\\", "\n", "\t", "\x00", "\x1f", "\x7f", "é", "{", ","])
    ),
    st.just(""),
)
_awkward_float = st.one_of(
    st.floats(),
    st.sampled_from(
        [0.0, -0.0, 1e-3, -1e-3, 9.999999e6, 1e7, 1e-4, 5e-324, 1.7976931348623157e308, 0.1]
    ),
    st.floats(min_value=-1e7, max_value=1e7),
)
_int64 = st.integers(-(2**63), 2**63 - 1)
_naive = st.datetimes(min_value=dt.datetime(1, 1, 1), max_value=dt.datetime(9999, 12, 31))


def _col(values, strategy):
    return st.lists(st.one_of(st.none(), strategy), min_size=ROWS, max_size=ROWS).map(values)


_KINDS = {
    "i8": _col(lambda v: pa.array(v, pa.int8()), st.integers(-128, 127)),
    "i16": _col(lambda v: pa.array(v, pa.int16()), st.integers(-(2**15), 2**15 - 1)),
    "i32": _col(lambda v: pa.array(v, pa.int32()), st.integers(-(2**31), 2**31 - 1)),
    "i64": _col(lambda v: pa.array(v, pa.int64()), _int64),
    "u64": _col(lambda v: pa.array(v, pa.uint64()), st.integers(0, 2**64 - 1)),
    "f64": _col(lambda v: pa.array(v, pa.float64()), _awkward_float),
    "f32": _col(lambda v: pa.array(v, pa.float32()), st.floats(width=32)),
    "b": _col(lambda v: pa.array(v, pa.bool_()), st.booleans()),
    "s": _col(lambda v: pa.array(v, pa.string()), _awkward_text),
    "ls": _col(lambda v: pa.array(v, pa.large_string()), _awkward_text),
    "d": _col(lambda v: pa.array(v, pa.date32()), st.dates()),
    "ts_us": _col(lambda v: pa.array(v, pa.timestamp("us")), _naive),
    "ts_ms": _col(
        lambda v: pa.array(v, pa.timestamp("ms")),
        _naive.map(lambda x: x.replace(microsecond=x.microsecond // 1000 * 1000)),
    ),
    "ts_s": _col(
        lambda v: pa.array(v, pa.timestamp("s")), _naive.map(lambda x: x.replace(microsecond=0))
    ),
    "ts_utc": _col(
        lambda v: pa.array(v, pa.timestamp("us", tz="UTC")),
        _naive.map(lambda x: x.replace(tzinfo=_UTC)),
    ),
    "ts_jhb": _col(
        lambda v: pa.array(v, pa.timestamp("us", tz="Africa/Johannesburg")),
        st.datetimes(min_value=dt.datetime(1900, 1, 2), max_value=dt.datetime(9999, 12, 30)).map(
            lambda x: x.replace(tzinfo=_UTC)
        ),
    ),
    "dec": _col(
        lambda v: pa.array(v, pa.decimal128(12, 3)),
        st.decimals(min_value=-(10**8), max_value=10**8, places=3, allow_nan=False),
    ),
    "bin": _col(lambda v: pa.array(v, pa.binary()), st.binary(max_size=8)),
    "lst": _col(
        lambda v: pa.array(v, pa.list_(pa.float64())),
        st.lists(st.one_of(st.none(), _awkward_float), max_size=3),
    ),
    "st": _col(
        lambda v: pa.array(v, pa.struct([("a", pa.int64()), ("b", pa.string())])),
        st.fixed_dictionaries(
            {"a": st.one_of(st.none(), _int64), "b": st.one_of(st.none(), _awkward_text)}
        ),
    ),
    "mp": _col(
        lambda v: pa.array(v, pa.map_(pa.string(), pa.int64())),
        st.lists(st.tuples(st.text(max_size=3), _int64), max_size=3, unique_by=lambda kv: kv[0]),
    ),
}


@st.composite
def tables(draw):
    """A table of any mix of kinds (and odd column names), in one or two chunks."""
    kinds = draw(st.lists(st.sampled_from(sorted(_KINDS)), min_size=1, max_size=6, unique=True))
    columns = {}
    for kind in kinds:
        name = draw(st.one_of(st.just(kind), st.text(min_size=1, max_size=4))) or kind
        if name in columns:
            name = kind
        columns[name] = draw(_KINDS[kind])
    table = pa.table(columns)
    if draw(st.booleans()):
        table = pa.concat_tables([table.slice(0, 3), table.slice(3)])
    return table


@settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(tables())
def test_any_table_hashes_the_same_both_ways(table):
    assert fingerprint_arrow(table) == _reference(table)


@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(tables())
def test_any_table_hashes_the_same_across_slices(small_slices, table):
    assert fingerprint_arrow(table) == _reference(table)


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(tables())
def test_every_fast_line_is_the_canonical_line(table):
    """Line by line, not only the sums: the lines Arrow builds are the reference's."""
    schema = [(f.name, arrow_kind(f.type)) for f in table.schema]
    kinds = dict(schema)
    names = sorted(table.column_names)
    want = [canonical_line(row, kinds) for row in table.to_pylist()]
    got = []
    for piece in ch._slices(table, names):
        lines = ch._slice_lines(names, kinds, piece)
        assert lines is not None
        got.extend(lines.to_pylist())
    assert got == want


def test_the_common_kinds_take_the_fast_path():
    """Ints, strings, bools, dates, doubles and timestamps are built by Arrow."""
    table = pa.table(
        {
            "i": pa.array([1, None], pa.int64()),
            "s": pa.array(['a "q"', None]),
            "b": pa.array([True, None]),
            "d": pa.array([dt.date(2024, 1, 1), None]),
            "f": pa.array([0.5, None]),
            "t": pa.array([dt.datetime(2024, 1, 1), None], pa.timestamp("us", tz="UTC")),
        }
    )
    for i, field in enumerate(table.schema):
        assert (
            ch._arrow_members(field.name, arrow_kind(field.type), table.column(i).chunk(0))
            is not None
        )


def test_lane_sums_wrap_like_python_ints():
    """numpy's uint64 sum wraps modulo 2**64; the Python sum, masked, is the same."""
    lines = pa.array([canonical_line({"i": i}) for i in range(5000)])
    a = b = 0
    for line in lines.to_pylist():
        x, y = ch.lanes(line)
        a, b = a + x, b + y
    assert a > 2**64 and b > 2**64  # the sums really did overflow
    assert ch._arrow_lanes(lines) == (a & ch._MASK, b & ch._MASK)


def test_a_table_with_no_columns_keeps_its_old_digest():
    table = pa.table({"a": [1, 2]}).drop(["a"])
    assert fingerprint_arrow(table).data_hash == ch.data_hash([], 2, (0, 0))


def _sample_table():
    return pa.table(
        {
            "id": pa.array(range(500), pa.int64()),
            "s": pa.array([f'row "{i}" \\ é' for i in range(500)]),
            "f": pa.array([i / 7 for i in range(500)]),
        }
    )


def test_without_the_builtin_sha256_hashlib_gives_the_same_digest(monkeypatch):
    """_sha2 is private: when it is missing, hashlib is used, and nothing moves."""
    import hashlib
    import sys

    table = _sample_table()
    want = fingerprint_arrow(table)
    monkeypatch.setitem(sys.modules, "_sha2", None)  # import now fails
    picked = ch._pick_line_sha256()
    assert picked is hashlib.sha256
    monkeypatch.setattr(ch, "_line_sha256", picked)
    assert fingerprint_arrow(table) == want == _reference(table)


def test_a_builtin_sha256_that_behaves_differently_is_not_used(monkeypatch):
    import hashlib
    import sys
    import types

    def refuses_memoryview(data):
        if isinstance(data, memoryview):
            raise TypeError("a bytes-like object is required")
        return hashlib.sha256(data)

    fake = types.ModuleType("_sha2")
    fake.sha256 = refuses_memoryview
    monkeypatch.setitem(sys.modules, "_sha2", fake)
    assert ch._pick_line_sha256() is hashlib.sha256

    fake.sha256 = lambda data: hashlib.sha1(data)  # another digest altogether
    assert ch._pick_line_sha256() is hashlib.sha256


def test_golden_digests_do_not_move():
    """Digests recorded before the vectorised path, on fixed tables."""
    table = pa.table(
        {
            "id": pa.array(range(1000), pa.int64()),
            "v": pa.array([i % 97 for i in range(1000)], pa.int64()),
            "s": pa.array(["x" * 20] * 1000),
            "batch": pa.array(["2"] * 1000),
        }
    )
    assert fingerprint_arrow(table).data_hash == GOLDEN_E01
    mixed = pa.table(
        {
            "f": pa.array([0.1, -0.0, math.nan, math.inf, 1e-5, 123.0, None]),
            "t": pa.array(
                [dt.datetime(2024, 1, 1, 12, 0, 5, 123456, tzinfo=dt.timezone.utc)] * 6 + [None],
                pa.timestamp("us", tz="UTC"),
            ),
            "d": pa.array([dt.date(999, 1, 1)] * 7),
            "s": pa.array(['q"', "b\\", "c\n", "", "é", None, "{,"]),
            "m": pa.array([decimal.Decimal("1.500")] * 7, pa.decimal128(10, 3)),
        }
    )
    assert fingerprint_arrow(mixed).data_hash == GOLDEN_MIXED


GOLDEN_E01 = "sha256:095c7263bd8669fe3d841084fb0b7f286e153015d6461fb341daa470141b83c4"
GOLDEN_MIXED = "sha256:e4f62146893d1369edd0bc6e28982fc6a6bc0d3f6f1b1016a56e06ae686dd06b"


def test_invalid_utf8_records_the_error_not_a_digest():
    """A string column cast from bytes unchecked: Spark would not hash these
    bytes the same way, so no digest is recorded, as before the fast path."""
    bad = pa.array([b"ok", b"\xff\xfe"], pa.binary()).cast(pa.string(), safe=False)
    print_ = ch.fingerprint(pa.table({"s": bad, "i": pa.array([1, 2])}))
    assert print_.data_hash is None
    assert "UnicodeDecodeError" in print_.error


def test_nan_is_still_not_null():
    with_nan = pa.table({"f": pa.array([math.nan], pa.float64())})
    with_null = pa.table({"f": pa.array([None], pa.float64())})
    assert fingerprint_arrow(with_nan).data_hash != fingerprint_arrow(with_null).data_hash


class TestManySmallChunks:
    """F-076: a table of many small chunks (a folder of small files) is hashed in few slices."""

    def _table(self):
        pieces = [
            pa.table({"id": pa.array([2 * i, 2 * i + 1], pa.int64()), "v": [0.5, None]})
            for i in range(2000)
        ]
        return pa.concat_tables(pieces)

    def test_the_rows_are_put_together_before_hashing(self, monkeypatch):
        table = self._table()
        assert table.column(0).num_chunks == 2000
        calls = []
        real = ch._slice_lanes
        monkeypatch.setattr(ch, "_slice_lanes", lambda *a: calls.append(1) or real(*a))
        fingerprint_arrow(table)
        assert len(calls) == 1  # was one slice per chunk: 2000

    def test_the_digest_is_unchanged(self):
        table = self._table()
        assert fingerprint_arrow(table) == fingerprint_arrow(table.combine_chunks())
