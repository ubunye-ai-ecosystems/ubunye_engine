"""E-10 correctness: every candidate gives the same rows-v1 digest as fingerprint_arrow.

Generated tables come from the engine's own property test strategy
(tests/unit/lineage/test_content_hash_fast_path.py::tables: every kind, nulls,
NaN, control characters, unicode, nested lists and structs, maps, zones, one or
two chunks), plus fixed tables. For each table the digest with the candidate in
place of _arrow_lanes must equal the unchanged digest and the row at a time
reference (fingerprint_rows). Half the examples use 3 row slices so every table
crosses slice boundaries. Helpers are off (one process), so what differs is only
the lane function.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ["UBUNYE_HASH_WORKERS"] = "1"

import candidates as C  # noqa: E402
import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402

ch = C.load_content_hash()
# The test module imports ubunye.lineage.content_hash; give it this file alone.
for name in ("ubunye", "ubunye.lineage"):
    mod = types.ModuleType(name)
    mod.__path__ = []
    sys.modules[name] = mod
sys.modules["ubunye.lineage.content_hash"] = ch
sys.modules["ubunye.lineage"].content_hash = ch

root = os.environ["E10_WORKTREE"]
spec = importlib.util.spec_from_file_location(
    "fast_path", os.path.join(root, "tests", "unit", "lineage", "test_content_hash_fast_path.py")
)
fp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fp)

from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

REAL = ch._arrow_lanes
lib = C.c_dll()


def c_scalar(lines):
    lib.rows_v1_set_mode(0)
    try:
        return C.lanes_c(lines)
    finally:
        lib.rows_v1_set_mode(-1)


CANDS = dict(C.CANDIDATES)
CANDS["c_plain"] = c_scalar
CANDS["numba_parallel"] = C.lanes_numba_parallel
CANDS["duckdb_4threads"] = lambda lines: C.lanes_duckdb(lines, threads=4)

os.environ.setdefault("E10_DLL", os.path.join(HERE, "rows_v1_lanes.dll"))
import e10_native  # noqa: E402

stats = {
    "tables": 0,
    "rows": 0,
    "lane_calls": 0,
    "mismatch": {k: 0 for k in list(CANDS) + ["e10_native_threads"]},
    "features": {"nulls": 0, "non_ascii": 0, "nested": 0, "map": 0, "chunked": 0, "multi_slice": 0},
}
calls = {"n": 0}


def fingerprint_with(table, fn):
    def counted(lines):
        calls["n"] += 1
        a, b = fn(lines)
        return a & C.MASK, b & C.MASK

    ch._arrow_lanes = counted
    try:
        return ch.fingerprint_arrow(table)
    finally:
        ch._arrow_lanes = REAL


def features(table):
    f = stats["features"]
    if any(c.null_count for c in table.columns):
        f["nulls"] += 1
    if any(pa.types.is_nested(t.type) for t in table.schema):
        f["nested"] += 1
    if any(pa.types.is_map(t.type) for t in table.schema):
        f["map"] += 1
    if any(c.num_chunks > 1 for c in table.columns):
        f["chunked"] += 1
    text = json.dumps(table.to_pylist(), default=str, ensure_ascii=False)
    if any(ord(c) > 127 for c in text):
        f["non_ascii"] += 1


def check(table, label=""):
    stats["tables"] += 1
    stats["rows"] += table.num_rows
    features(table)
    base = ch.fingerprint_arrow(table)
    ref = fp._reference(table)
    assert base == ref, (label, "the unchanged path disagrees with the reference")
    # The whole prototype (threads + kernel) as E-06 runs it.
    got = e10_native.fingerprint_arrow_native(table, threads=3, ch=ch)
    if got != base:
        stats["mismatch"]["e10_native_threads"] += 1
        print("MISMATCH e10_native_threads", label, table.schema)
    for name, fn in CANDS.items():
        before = calls["n"]
        got = fingerprint_with(table, fn)
        stats["lane_calls"] += calls["n"] - before
        if got != base:
            stats["mismatch"][name] += 1
            print("MISMATCH", name, label, table.schema, got, base)


@settings(
    max_examples=60,
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=list(HealthCheck),
)
@given(fp.tables(), st.booleans())
def run_generated(table, small):
    old = ch._SLICE_ROWS
    if small:
        ch._SLICE_ROWS = 3
        if table.num_rows > 3:
            stats["features"]["multi_slice"] += 1
    try:
        check(table, "generated")
    finally:
        ch._SLICE_ROWS = old


def fixed_tables():
    yield "awkward", pa.table(
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
            "mp": pa.array([[("k", 1), ("a", None)], None, []], pa.map_(pa.string(), pa.int64())),
            "dec": pa.array([None, None, None], pa.decimal128(10, 2)),
        }
    )
    yield "golden", pa.table(
        {
            "i": pa.array([1, None, 3, 4], pa.int64()),
            "s": pa.array(["a", 'q"uote', None, "é"]),
            "d": pa.array([dt.date(2024, 1, 31), None, dt.date(1, 1, 1), dt.date(1970, 1, 1)]),
        }
    )
    yield "empty", pa.table({"a": pa.array([], pa.int64())})
    yield "all_null_row", pa.table(
        {"a": pa.array([None, 1], pa.int64()), "b": pa.array([None, "x"])}
    )
    # Long lines: many blocks per line, and lines of every length 0 to 200 bytes.
    yield "long_text", pa.table({"s": pa.array(["é" * k + "x" * (k % 7) for k in range(400)])})
    rng = np.random.default_rng(606)
    n = 200_000
    cats = pa.array([f"cat_{i:02d}" for i in range(20)])
    yield "e06_shape_200k", pa.table(
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


if __name__ == "__main__":
    run_generated()
    for label, table in fixed_tables():
        check(table, label)
    stats["sha_ni"] = int(lib.rows_v1_has_shani())
    print(json.dumps(stats, indent=1))
    bad = sum(stats["mismatch"].values())
    print("RESULT:", "all identical" if not bad else f"{bad} mismatches")
    sys.exit(1 if bad else 0)
