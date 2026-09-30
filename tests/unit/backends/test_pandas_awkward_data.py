"""Awkward data on the pandas backend reads and writes the way Spark does (E-08).

Each class is one finding from experiment E-08 (tasks/hardening/experiments/
E-08-awkward-data.md). The expected values come from Spark's own source; the
integration tier (tests/integration/test_awkward_data_parity.py) checks each case
against a live Spark session.

No Spark, no JVM: these run wherever pandas and pyarrow are installed.
"""

from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")

from ubunye.adapters import pandas_io  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402


def _read_bytes(tmp_path, fmt, data: bytes, name="in", **kw):
    src = tmp_path / f"{name}.{fmt}"
    src.write_bytes(data)
    return PandasBackend(**kw.pop("backend_kw", {})).read_frame(fmt, str(src), **kw)


class TestCsvHeaderNames:
    """F-060: Spark's makeSafeHeader renames blank and duplicate header names."""

    def test_duplicates_blanks_and_case_are_renamed_like_spark(self, tmp_path):
        frame = _read_bytes(
            tmp_path, "csv", b"a,a,,A,b,Col,col\n1,2,3,4,5,6,7\n", options={"header": "true"}
        )
        assert list(frame.native.columns) == ["a0", "a1", "_c2", "A3", "b", "Col5", "col6"]
        assert frame.native.iloc[0].tolist() == ["1", "2", "3", "4", "5", "6", "7"]

    def test_the_null_value_text_names_a_column_by_its_index(self, tmp_path):
        frame = _read_bytes(
            tmp_path, "csv", b"id,NA\n1,2\n", options={"header": "true", "nullValue": "NA"}
        )
        assert list(frame.native.columns) == ["id", "_c1"]

    def test_plain_headers_are_untouched(self):
        assert pandas_io.safe_header(["x", "y", "naïve", "with space"]) == [
            "x",
            "y",
            "naïve",
            "with space",
        ]
