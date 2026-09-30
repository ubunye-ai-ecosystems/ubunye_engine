"""The ``numpy_keys`` snippet in the pandas backend docs does what the docs say (F-042).

The block between ``<!-- numpy-keys:begin -->`` and ``<!-- numpy-keys:end -->`` in
docs/deployment/anywhere.md is read from the page and run, so the page and this test
cannot drift. The docs promise: values do not change, a whole number key with no
nulls becomes NumPy ``int64``, a key with a null is left alone (Arrow or pandas'
nullable ``Int64``), and a merge gives the same rows either way.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")

PAGE = Path(__file__).resolve().parents[2] / "docs" / "deployment" / "anywhere.md"
BLOCK = re.compile(
    r"<!-- numpy-keys:begin -->\s*```python\n(.*?)```\s*<!-- numpy-keys:end -->", re.S
)


def _numpy_keys():
    (code,) = BLOCK.findall(PAGE.read_text(encoding="utf-8"))
    assert len(code.strip().splitlines()) == 3, "the docs promise three lines"
    scope: dict = {}
    exec(compile(code, str(PAGE), "exec"), scope)  # noqa: S102 - our own docs
    return scope["numpy_keys"]


numpy_keys = _numpy_keys()


def _arrow_frame(**columns):
    # What the pandas backend hands a transform (ubunye.adapters.pandas_io).
    return pa.table(columns).to_pandas(types_mapper=pd.ArrowDtype)


def test_a_key_with_no_nulls_becomes_numpy_int64_with_the_same_values():
    frame = _arrow_frame(k=[3, 1, 2], v=["a", "b", "c"])
    out = numpy_keys(frame, ["k"])
    assert out["k"].dtype == "int64"
    assert out["k"].tolist() == [3, 1, 2]
    assert out["v"].dtype == frame["v"].dtype  # other columns untouched
    assert frame["k"].dtype == pd.ArrowDtype(pa.int64())  # the input is not changed


def test_a_key_with_a_null_is_left_alone():
    frame = _arrow_frame(k=[1, None, 3])
    out = numpy_keys(frame, ["k"])
    assert out["k"].dtype == pd.ArrowDtype(pa.int64())
    assert out["k"].isna().tolist() == [False, True, False]


def test_pandas_nullable_int64_works_too():
    frame = pd.DataFrame(
        {
            "full": pd.array([1, 2, 3], dtype="Int64"),
            "gappy": pd.array([1, None, 3], dtype="Int64"),
        }
    )
    out = numpy_keys(frame, ["full", "gappy"])
    assert out["full"].dtype == "int64" and out["full"].tolist() == [1, 2, 3]
    assert out["gappy"].dtype == "Int64" and out["gappy"].isna().sum() == 1


def test_a_merge_gives_the_same_rows_either_way():
    events = _arrow_frame(id=[1, 2, 3, 4], region=[10, 20, 10, 30])
    regions = _arrow_frame(region=[10, 20], name=["north", "south"])
    arrow = events.merge(regions, on="region").sort_values("id", ignore_index=True)
    fast = (
        numpy_keys(events, ["region"])
        .merge(numpy_keys(regions, ["region"]), on="region")
        .sort_values("id", ignore_index=True)
    )
    assert fast["region"].dtype == "int64"
    assert fast["id"].tolist() == arrow["id"].tolist() == [1, 2, 3]
    assert fast["region"].tolist() == arrow["region"].tolist()
    assert fast["name"].tolist() == arrow["name"].tolist()
