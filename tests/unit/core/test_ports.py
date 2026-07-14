"""The data port, proved with real data — not mocks asserting on mocks.

The port is structural (`runtime_checkable` Protocol): whatever has the members
satisfies it, no inheritance. That is the design's central rule, and the reason a Spark
DataFrame passes natively while pandas needs a thirty-line adapter.
"""

from __future__ import annotations

import pytest

from ubunye.core.ports import DataFramePort

pd = pytest.importorskip("pandas", reason="the adapter wraps pandas")

from ubunye.adapters import PandasDataFrameAdapter  # noqa: E402


@pytest.fixture
def frame():
    return PandasDataFrameAdapter(pd.DataFrame({"id": [1, 2, 3], "fare": [10.5, 22.0, 7.25]}))


def test_the_adapter_satisfies_the_port_structurally(frame):
    """isinstance against a Protocol — no inheritance anywhere in the chain."""
    assert isinstance(frame, DataFramePort)
    assert not isinstance(object(), DataFramePort)


def test_real_schema_real_counts_real_rows(frame):
    """The point of the adapter: engine tests on REAL data with no JVM.

    Before this, Spark-free tests meant MagicMocks with hard-coded return values —
    a test of the mock's obedience, not of anything true.
    """
    assert frame.count() == 3
    assert frame.schema == {"id": "int64", "fare": "float64"}
    assert frame.collect()[1] == {"id": 2, "fare": 22.0}


def test_pandas_needed_the_adapter_for_a_reason():
    """`df.count()` EXISTS on pandas and means something else (non-null per column).

    A structural check would happily pass the raw frame while every count came back
    as a Series. The adapter is not ceremony — it is the difference between a number
    and a surprise.
    """
    raw = pd.DataFrame({"id": [1, None, 3]})
    assert not isinstance(raw.count(), int)  # the trap
    assert PandasDataFrameAdapter(raw).count() == 3  # the truth
