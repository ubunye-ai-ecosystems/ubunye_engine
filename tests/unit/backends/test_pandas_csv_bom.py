"""A UTF-8 byte order mark at the start of a CSV file is dropped, as Spark does.

Found on real data: Kaggle's Olist translation table starts with a BOM, so the
pandas backend named its first column "\\ufeffproduct_category_name" and a join on
it failed, while Spark named it "product_category_name". The expected values come
from Spark 4.2 on the same bytes (tests/integration/test_pandas_backend_parity.py
checks the two engines against each other).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402

BOM = "﻿"


def _read(tmp_path, text, **options):
    src = tmp_path / "in.csv"
    src.write_bytes(text.encode("utf-8"))
    native = PandasBackend().read_frame("csv", str(src), options=options).native
    rows = native.astype(object).where(native.notna(), None).values.tolist()
    return list(native.columns), rows


@pytest.mark.parametrize("multiline", ["false", "true"])
class TestByteOrderMark:
    def test_dropped_from_the_header(self, tmp_path, multiline):
        columns, rows = _read(tmp_path, BOM + "a,b\n1,x\n", header="true", multiLine=multiline)
        assert columns == ["a", "b"] and rows == [["1", "x"]]

    def test_dropped_before_a_quoted_header(self, tmp_path, multiline):
        columns, _ = _read(tmp_path, BOM + '"a",b\n1,x\n', header="true", multiLine=multiline)
        assert columns == ["a", "b"]

    def test_dropped_before_quoted_data(self, tmp_path, multiline):
        _, rows = _read(tmp_path, BOM + '"a",b\n1,x\n', multiLine=multiline)
        assert rows[0] == ["a", "b"]

    def test_kept_when_it_is_not_at_the_start(self, tmp_path, multiline):
        _, rows = _read(tmp_path, "a,b\n" + BOM + "1,x\n", header="true", multiLine=multiline)
        assert rows == [[BOM + "1", "x"]]

    def test_dropped_at_the_start_of_every_file_in_a_folder(self, tmp_path, multiline):
        folder = tmp_path / "parts"
        folder.mkdir()
        for n in (1, 2):
            (folder / f"part-{n}.csv").write_bytes((BOM + f"a,b\n{n},x\n").encode("utf-8"))
        native = (
            PandasBackend()
            .read_frame("csv", str(folder), options={"header": "true", "multiLine": multiline})
            .native
        )
        assert list(native.columns) == ["a", "b"]
        assert sorted(native["a"].tolist()) == ["1", "2"]
