"""C01 runs on pandas without Narwhals' "complex group-by expression" warning (F-106).

The warning means Narwhals fell back to a slow per-group apply; a cast inside the
aggregation caused it.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

import ubunye  # noqa: E402

TASK = (
    Path(__file__).resolve().parents[3]
    / "examples/proving/c01_portable_etl/pipelines/proving/c01/etl"
)


def test_c01_on_pandas_gives_no_complex_group_by_warning(tmp_path):
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        ubunye.run_task(str(TASK), backend="pandas", variables={"out_dir": str(tmp_path)})
    assert not [w for w in seen if "complex group-by" in str(w.message)]
    assert (tmp_path / "country_summary").exists()
