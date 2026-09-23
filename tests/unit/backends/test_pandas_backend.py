"""PandasBackend + pandas_io: the second Backend adapter (issue #38).

Runs wherever pandas is installed (the pandas extra); skips cleanly otherwise.
No Spark, no JVM.
"""

from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.errors import SinkWriteError, SourceReadError  # noqa: E402
from ubunye.core.write_modes import ResolvedWriteMode  # noqa: E402


def _overwrite() -> ResolvedWriteMode:
    return ResolvedWriteMode(mode="overwrite", save_mode="overwrite")


class TestLifecycleAndIdentity:
    def test_is_not_spark(self):
        assert PandasBackend().is_spark is False

    def test_has_no_spark_attribute(self):
        # The whole point: no session to reach for.
        assert not hasattr(PandasBackend(), "spark")

    def test_start_stop_and_context_manager_are_noops(self):
        with PandasBackend() as backend:
            backend.start()
            backend.stop()
        assert backend.app_name == "ubunye"


class TestReadWriteRoundtrip:
    def test_csv_roundtrip_through_the_dataframe_port(self, tmp_path):
        src = tmp_path / "in.csv"
        src.write_text("id,name\n1,ada\n2,alan\n", encoding="utf-8")
        backend = PandasBackend()

        frame = backend.read_frame("csv", str(src), options={"header": "true"})
        # Satisfies DataFramePort without being a Spark DataFrame.
        assert isinstance(frame, PandasDataFrameAdapter)
        assert frame.count() == 2
        assert set(frame.schema) == {"id", "name"}
        assert {r["name"] for r in frame.collect()} == {"ada", "alan"}

        out = tmp_path / "out.parquet"
        backend.execute_write(
            frame, _overwrite(), connector="s3", file_format="parquet", path=str(out)
        )
        assert out.exists()
        assert len(pd.read_parquet(out)) == 2

    def test_file_uri_scheme_is_stripped(self, tmp_path):
        src = tmp_path / "in.csv"
        src.write_text("a\n1\n", encoding="utf-8")
        uri = src.as_uri()  # file:///...
        frame = PandasBackend().read_frame("csv", uri, options={"header": "true"})
        assert frame.count() == 1


class TestWriteModes:
    def test_overwrite_replaces(self, tmp_path):
        out = tmp_path / "d.parquet"
        b = PandasBackend()
        b.execute_write(
            PandasDataFrameAdapter(pd.DataFrame({"x": [1, 2]})),
            _overwrite(),
            connector="s3",
            file_format="parquet",
            path=str(out),
        )
        b.execute_write(
            PandasDataFrameAdapter(pd.DataFrame({"x": [9]})),
            _overwrite(),
            connector="s3",
            file_format="parquet",
            path=str(out),
        )
        assert list(pd.read_parquet(out)["x"]) == [9]

    def test_append_concatenates(self, tmp_path):
        out = tmp_path / "d.parquet"
        b = PandasBackend()
        append = ResolvedWriteMode(mode="append", save_mode="append")
        for chunk in ([1, 2], [3]):
            b.execute_write(
                PandasDataFrameAdapter(pd.DataFrame({"x": chunk})),
                append,
                connector="s3",
                file_format="parquet",
                path=str(out),
            )
        assert sorted(pd.read_parquet(out)["x"]) == [1, 2, 3]

    def test_errorifexists_raises_on_existing(self, tmp_path):
        out = tmp_path / "d.parquet"
        b = PandasBackend()
        b.execute_write(
            PandasDataFrameAdapter(pd.DataFrame({"x": [1]})),
            _overwrite(),
            connector="s3",
            file_format="parquet",
            path=str(out),
        )
        with pytest.raises(SinkWriteError, match="already exists"):
            b.execute_write(
                PandasDataFrameAdapter(pd.DataFrame({"x": [2]})),
                ResolvedWriteMode(mode="errorifexists", save_mode="errorifexists"),
                connector="s3",
                file_format="parquet",
                path=str(out),
            )

    def test_ignore_leaves_existing_untouched(self, tmp_path):
        out = tmp_path / "d.parquet"
        b = PandasBackend()
        b.execute_write(
            PandasDataFrameAdapter(pd.DataFrame({"x": [1]})),
            _overwrite(),
            connector="s3",
            file_format="parquet",
            path=str(out),
        )
        b.execute_write(
            PandasDataFrameAdapter(pd.DataFrame({"x": [2]})),
            ResolvedWriteMode(mode="ignore", save_mode="ignore"),
            connector="s3",
            file_format="parquet",
            path=str(out),
        )
        assert list(pd.read_parquet(out)["x"]) == [1]


class TestUnsupported:
    def test_merge_is_refused_not_silently_overwritten(self, tmp_path):
        merge = ResolvedWriteMode(mode="merge", save_mode="overwrite", merge_keys=("id",))
        with pytest.raises(SinkWriteError, match="does not support write mode 'merge'"):
            PandasBackend().execute_write(
                PandasDataFrameAdapter(pd.DataFrame({"id": [1]})),
                merge,
                connector="s3",
                file_format="parquet",
                path=str(tmp_path / "d.parquet"),
            )

    def test_unsupported_read_format_raises(self, tmp_path):
        with pytest.raises(SourceReadError, match="cannot read file_format 'delta'"):
            PandasBackend().read_frame("delta", str(tmp_path / "x"))

    def test_managed_table_is_refused(self):
        with pytest.raises(SinkWriteError, match="not managed tables"):
            PandasBackend().execute_write(
                PandasDataFrameAdapter(pd.DataFrame({"x": [1]})),
                _overwrite(),
                connector="unity",
                file_format="parquet",
                table="main.f.c",
            )
