"""The parallel hash (F-038) must give the same digest as one process, byte for byte.

A large table is hashed by helper processes, one per core. Each hashes a run of
rows with the same code and sends back two lane sums. These tests shrink the
thresholds so small tables go through real helpers, and hold every digest to the
one process path (the code before F-038, unchanged) and to the row at a time
reference.
"""

from __future__ import annotations

import datetime as dt

import pytest

pa = pytest.importorskip("pyarrow")
np = pytest.importorskip("numpy")
hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402

from ubunye.lineage import content_hash as ch  # noqa: E402
from ubunye.lineage.content_hash import fingerprint_arrow  # noqa: E402

from .test_content_hash_fast_path import _reference, tables  # noqa: E402


def _serial(table, monkeypatch):
    monkeypatch.setenv(ch.HASH_WORKERS_ENV, "1")
    try:
        return fingerprint_arrow(table)
    finally:
        monkeypatch.delenv(ch.HASH_WORKERS_ENV)


@pytest.fixture
def parallel(monkeypatch):
    """Helpers for any table of 2 rows or more, and a record of every parallel result."""
    monkeypatch.setattr(ch, "_PARALLEL_MIN_ROWS", 2)
    monkeypatch.setattr(ch, "_ROWS_PER_WORKER", 1)
    monkeypatch.setenv(ch.HASH_WORKERS_ENV, "3")
    seen = []
    real = ch._parallel_lanes

    def spy(*args, **kwargs):
        out = real(*args, **kwargs)
        seen.append(out)
        return out

    monkeypatch.setattr(ch, "_parallel_lanes", spy)
    return seen


@settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(tables())
def test_any_table_hashes_the_same_in_helpers(parallel, monkeypatch, table):
    got = fingerprint_arrow(table)
    extension = not all(ch._plain_type(f.type) for f in table.schema)
    if not extension:
        assert parallel and parallel[-1] is not None, "the helpers did not run"
    assert got == _serial(table, monkeypatch)
    assert got == _reference(table)


def test_the_e06_shape_hashes_the_same_in_helpers(parallel, monkeypatch):
    rng = np.random.default_rng(606)
    n = 200_000
    cats = pa.array([f"cat_{i:02d}" for i in range(20)])
    table = pa.table(
        {
            "id": np.arange(n, dtype=np.int64),
            "region": rng.integers(0, 1000, n, dtype=np.int64),
            "cat": cats.take(pa.array(rng.integers(0, 20, n))),
            "amount": rng.integers(1, 100_000, n, dtype=np.int64),
            "qty": rng.integers(-2, 20, n, dtype=np.int64),
            "big": rng.integers(0, 2, n).astype(bool),
            "price": rng.normal(0, 1e6, n),
            "ts": pa.array(rng.integers(0, 2**40, n), pa.timestamp("us", tz="UTC")),
        }
    )
    got = fingerprint_arrow(table)
    assert parallel == [parallel[0]] and parallel[0] is not None
    assert got == _serial(table, monkeypatch)


def test_golden_digest_does_not_move_in_helpers(parallel):
    table = pa.table(
        {
            "i": pa.array([1, None, 3, 4], pa.int64()),
            "s": pa.array(["a", 'q"uote', None, "é"]),
            "d": pa.array([dt.date(2024, 1, 31), None, dt.date(1, 1, 1), dt.date(1970, 1, 1)]),
        }
    )
    reference = _reference(table)
    assert fingerprint_arrow(table) == reference
    assert parallel[-1] is not None


def test_a_failed_helper_falls_back_to_one_process(parallel, monkeypatch):
    table = pa.table({"x": list(range(10)), "s": [str(i) for i in range(10)]})
    monkeypatch.setattr(ch.sys, "executable", ch.sys.executable + "-missing")
    assert fingerprint_arrow(table) == _reference(table)
    assert parallel == [None]


def test_a_helper_that_exits_non_zero_falls_back(parallel, monkeypatch):
    table = pa.table({"x": list(range(10))})
    monkeypatch.setattr(ch, "_WORKER_BOOT", "import sys\nsys.exit(4)\n")
    assert fingerprint_arrow(table) == _reference(table)
    assert parallel == [None]


def test_invalid_utf8_still_records_the_error(parallel):
    raw = pa.array([b"ok", b"\xff\xfe", b"fine"], pa.binary())
    table = pa.table({"s": raw.cast(pa.string(), safe=False)})
    got = ch.fingerprint(table)
    assert got.data_hash is None
    assert "UnicodeDecodeError" in (got.error or "")


def test_worker_count_setting(monkeypatch):
    monkeypatch.setattr(ch, "_cores", lambda: 16)
    monkeypatch.delenv(ch.HASH_WORKERS_ENV, raising=False)
    assert ch._worker_count(ch._PARALLEL_MIN_ROWS - 1) == 1
    assert ch._worker_count(10_000_000) == ch._DEFAULT_MAX_WORKERS
    assert ch._worker_count(ch._PARALLEL_MIN_ROWS) == ch._PARALLEL_MIN_ROWS // ch._ROWS_PER_WORKER
    for raw, want in (("1", 1), ("0", 1), ("-3", 1), ("six", 1), ("12", 12)):
        monkeypatch.setenv(ch.HASH_WORKERS_ENV, raw)
        assert ch._worker_count(10_000_000) == want


def test_one_worker_starts_no_process(monkeypatch):
    monkeypatch.setattr(ch, "_PARALLEL_MIN_ROWS", 2)
    monkeypatch.setattr(ch, "_ROWS_PER_WORKER", 1)
    monkeypatch.setenv(ch.HASH_WORKERS_ENV, "1")

    def boom(*a, **k):
        raise AssertionError("no helper should start")

    monkeypatch.setattr(ch, "_parallel_lanes", boom)
    table = pa.table({"x": list(range(10))})
    assert fingerprint_arrow(table) == _reference(table)
