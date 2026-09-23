"""The pandas backend writes a path the way Spark writes it.

A path Spark writes is a folder of ``part-*`` files plus a ``_SUCCESS`` marker,
with Spark's text formats inside. The pandas backend writes the same layout, so
Spark can read what pandas wrote and the other way round, and ``append`` adds a
part file instead of rewriting everything. Expected text comes from Spark 4.2.
"""

from __future__ import annotations

import datetime as dt
import json
import os

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from ubunye.adapters import pandas_io  # noqa: E402
from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.errors import SinkWriteError  # noqa: E402
from ubunye.core.write_modes import ResolvedWriteMode  # noqa: E402

OVERWRITE = ResolvedWriteMode(mode="overwrite", save_mode="overwrite")
APPEND = ResolvedWriteMode(mode="append", save_mode="append")


def _write(target, frame, fmt="parquet", mode=OVERWRITE, backend=None, **kw):
    (backend or PandasBackend()).execute_write(
        frame, mode, connector="s3", file_format=fmt, path=str(target), **kw
    )


def _parts(folder):
    return sorted(n for n in os.listdir(folder) if n.startswith("part-"))


def _text(folder):
    return "".join(open(os.path.join(folder, p), encoding="utf-8").read() for p in _parts(folder))


class TestLayout:
    def test_a_folder_of_one_part_file_and_a_success_marker(self, tmp_path):
        _write(tmp_path / "out", pd.DataFrame({"x": [1, 2]}))
        names = os.listdir(tmp_path / "out")
        assert "_SUCCESS" in names
        (part,) = _parts(tmp_path / "out")
        assert part.startswith("part-00000-") and part.endswith("-c000.snappy.parquet")

    def test_file_endings_per_format(self, tmp_path):
        for fmt, ending in (("csv", "-c000.csv"), ("json", "-c000.json")):
            _write(tmp_path / fmt, pd.DataFrame({"x": [1]}), fmt=fmt)
            assert _parts(tmp_path / fmt)[0].endswith(ending)

    def test_nothing_is_left_beside_the_target(self, tmp_path):
        _write(tmp_path / "out", pd.DataFrame({"x": [1]}))
        _write(tmp_path / "out", pd.DataFrame({"x": [2]}))
        _write(tmp_path / "out", pd.DataFrame({"x": [3]}), mode=APPEND)
        assert sorted(os.listdir(tmp_path)) == ["out"]

    def test_accepts_the_adapter_or_a_plain_frame(self, tmp_path):
        _write(tmp_path / "a", PandasDataFrameAdapter(pd.DataFrame({"x": [1]})))
        _write(tmp_path / "b", pd.DataFrame({"x": [1]}))
        assert _parts(tmp_path / "a") and _parts(tmp_path / "b")


class TestModes:
    def test_overwrite_replaces_every_old_part(self, tmp_path):
        _write(tmp_path / "out", pd.DataFrame({"x": [1, 2]}))
        _write(tmp_path / "out", pd.DataFrame({"x": [9]}))
        assert len(_parts(tmp_path / "out")) == 1
        assert PandasBackend().read_frame("parquet", str(tmp_path / "out")).collect() == [{"x": 9}]

    def test_append_adds_a_part_and_leaves_the_old_ones(self, tmp_path):
        _write(tmp_path / "out", pd.DataFrame({"x": [1, 2]}), mode=APPEND)
        first = _parts(tmp_path / "out")
        _write(tmp_path / "out", pd.DataFrame({"x": [3]}), mode=APPEND)
        assert set(first) < set(_parts(tmp_path / "out"))
        frame = PandasBackend().read_frame("parquet", str(tmp_path / "out"))
        assert sorted(frame.native["x"].tolist()) == [1, 2, 3]

    def test_append_onto_a_single_file_is_refused(self, tmp_path):
        target = tmp_path / "old.parquet"
        pq.write_table(pa.table({"x": [1]}), target)
        with pytest.raises(SinkWriteError, match="single file"):
            _write(target, pd.DataFrame({"x": [2]}), mode=APPEND)
        assert target.is_file()

    def test_overwrite_replaces_a_single_file_with_a_folder(self, tmp_path):
        target = tmp_path / "old.parquet"
        pq.write_table(pa.table({"x": [1]}), target)
        _write(target, pd.DataFrame({"x": [2]}))
        assert target.is_dir()

    def test_errorifexists_and_ignore(self, tmp_path):
        _write(tmp_path / "out", pd.DataFrame({"x": [1]}))
        with pytest.raises(SinkWriteError, match="already exists"):
            _write(
                tmp_path / "out",
                pd.DataFrame({"x": [2]}),
                mode=ResolvedWriteMode(mode="errorifexists", save_mode="errorifexists"),
            )
        _write(
            tmp_path / "out",
            pd.DataFrame({"x": [3]}),
            mode=ResolvedWriteMode(mode="ignore", save_mode="ignore"),
        )
        frame = PandasBackend().read_frame("parquet", str(tmp_path / "out"))
        assert frame.collect() == [{"x": 1}]

    def test_a_failed_overwrite_keeps_the_old_data(self, tmp_path, monkeypatch):
        _write(tmp_path / "out", pd.DataFrame({"x": [1]}))
        before = _parts(tmp_path / "out")

        def boom(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(pq, "write_table", boom)
        with pytest.raises(SinkWriteError, match="disk full"):
            _write(tmp_path / "out", pd.DataFrame({"x": [2]}))
        assert _parts(tmp_path / "out") == before
        assert sorted(os.listdir(tmp_path)) == ["out"]


class TestCsvText:
    def test_no_header_by_default_like_spark(self, tmp_path):
        _write(tmp_path / "out", pd.DataFrame({"a": [1], "b": ["x"]}), fmt="csv")
        assert _text(tmp_path / "out") == "1,x\n"

    def test_header_when_asked(self, tmp_path):
        _write(
            tmp_path / "out",
            pd.DataFrame({"a": [1]}),
            fmt="csv",
            options={"header": "true"},
        )
        assert _text(tmp_path / "out") == "a\n1\n"

    def test_null_is_an_empty_field_and_booleans_are_lower_case(self, tmp_path):
        frame = pd.DataFrame({"a": pd.array([1, None], dtype="Int64"), "b": [True, False]})
        _write(tmp_path / "out", frame, fmt="csv")
        assert _text(tmp_path / "out") == "1,true\n,false\n"

    def test_timestamps_use_spark_text(self, tmp_path):
        stamp = pd.Timestamp("2024-01-02 01:04:05.123456", tz="UTC")
        _write(
            tmp_path / "out",
            pd.DataFrame({"ts": [stamp]}),
            fmt="csv",
            backend=PandasBackend(timezone="Africa/Johannesburg"),
        )
        # Spark's default: milliseconds, in the session zone, with its offset.
        assert _text(tmp_path / "out") == "2024-01-02T03:04:05.123+02:00\n"

    def test_doubles_are_written_the_java_way(self, tmp_path):
        values = [2.0, 1.5, 1e10, 1e-5, 123456789.0, -0.0, float("nan"), float("inf")]
        # Arrow backed, so NaN stays NaN (in a numpy float column NaN means missing).
        column = pd.Series(pd.arrays.ArrowExtensionArray(pa.array(values)))
        _write(tmp_path / "out", pd.DataFrame({"d": column}), fmt="csv")
        assert _text(tmp_path / "out").splitlines() == [
            "2.0",
            "1.5",
            "1.0E10",
            "1.0E-5",
            "1.23456789E8",
            "-0.0",
            "NaN",
            "Infinity",
        ]

    def test_text_is_quoted_only_when_it_must_be(self, tmp_path):
        values = ["plain", "a,b", 'say "hi"', "", None, "two\nlines", "back\\slash", " pad "]
        _write(tmp_path / "out", pd.DataFrame({"s": values}), fmt="csv")
        # Byte for byte what Spark 4.2 writes for the same column: a backslash
        # escapes a quote, an empty string is "", the null row is skipped, and
        # text is trimmed.
        assert _text(tmp_path / "out") == (
            'plain\n"a,b"\n"say \\"hi\\""\n""\n"two\nlines"\nback\\slash\npad\n'
        )

    def test_a_null_in_a_wider_row_is_an_empty_field(self, tmp_path):
        frame = pd.DataFrame({"s": ["", None], "n": pd.array([None, None], dtype="Int64")})
        _write(tmp_path / "out", frame, fmt="csv")
        assert _text(tmp_path / "out") == '"",\n,\n'

    def test_tricky_text_reads_back_unchanged(self, tmp_path):
        values = ["plain", "a,b", 'say "hi"', "", None]
        _write(tmp_path / "out", pd.DataFrame({"s": values}), fmt="csv")
        back = PandasBackend().read_frame("csv", str(tmp_path / "out")).native["_c0"]
        assert back.tolist()[:3] == values[:3]
        # As in Spark: "" reads back as null, and the null row was never written.
        assert back.isna().tolist()[3:] == [True]

    def test_utc_is_written_with_z(self, tmp_path):
        stamp = pd.Timestamp("2024-01-02 01:04:05", tz="UTC")
        _write(tmp_path / "out", pd.DataFrame({"ts": [stamp]}), fmt="csv")
        assert _text(tmp_path / "out") == "2024-01-02T01:04:05.000Z\n"


class TestJsonText:
    def test_json_lines_without_null_fields(self, tmp_path):
        frame = pd.DataFrame({"a": ["x", None], "b": pd.array([1, 2], dtype="Int64")})
        _write(tmp_path / "out", frame, fmt="json")
        lines = [json.loads(line) for line in _text(tmp_path / "out").splitlines()]
        assert lines == [{"a": "x", "b": 1}, {"b": 2}]

    def test_byte_for_byte_what_spark_writes(self, tmp_path):
        # The expected lines are Spark 4.2's own output for the same rows.
        line_separator = chr(0x2028)  # a line separator character inside a value
        table = pa.table(
            {
                "s": ['say "hi"', "", f"tab\there é ü {line_separator}", "x", "ctl" + chr(1)],
                "d": [float("nan"), -0.0, 1 / 3, float("-inf"), 1e-5],
                "arr": pa.array(
                    [[1.5, None], [], None, [float("inf")], [1e21]], pa.list_(pa.float64())
                ),
                "m": pa.array(
                    [{"k": None, "j": 1}, None, {"k": 2.0, "j": None}, None, None],
                    pa.struct([("k", pa.float64()), ("j", pa.int32())]),
                ),
            }
        )
        _write(tmp_path / "out", table.to_pandas(types_mapper=pd.ArrowDtype), fmt="json")
        # Split on newlines only: splitlines() also breaks at the line separator.
        assert _text(tmp_path / "out").split("\n")[:-1] == [
            '{"s":"say \\"hi\\"","d":"NaN","arr":[1.5,null],"m":{"j":1}}',
            '{"s":"","d":-0.0,"arr":[]}',
            '{"s":"tab\\there é ü ' + line_separator + '","d":0.3333333333333333,"m":{"k":2.0}}',
            '{"s":"x","d":"-Infinity","arr":["Infinity"]}',
            '{"s":"ctl\\u0001","d":1.0E-5,"arr":[1.0E21]}',
        ]

    def test_dates_and_timestamps_are_text(self, tmp_path):
        frame = pd.DataFrame(
            {
                "d": [dt.date(2024, 1, 2)],
                "ts": [pd.Timestamp("2024-01-02 01:04:05", tz="UTC")],
            }
        )
        _write(tmp_path / "out", frame, fmt="json")
        assert json.loads(_text(tmp_path / "out")) == {
            "d": "2024-01-02",
            "ts": "2024-01-02T01:04:05.000Z",
        }


class TestParquetTypes:
    def _schema(self, folder):
        return pq.read_schema(os.path.join(folder, _parts(folder)[0]))

    def test_timestamps_are_utc_microseconds_spark_can_read(self, tmp_path):
        frame = pd.DataFrame({"ts": pd.to_datetime(["2024-01-02 03:04:05.123456789"])})
        _write(tmp_path / "out", frame)
        # Spark cannot read nanosecond parquet timestamps.
        assert self._schema(tmp_path / "out").field("ts").type == pa.timestamp("us", tz="UTC")

    def test_naive_timestamps_are_read_in_the_backend_zone(self, tmp_path):
        frame = pd.DataFrame({"ts": pd.to_datetime(["2024-01-02 03:04:05"])})
        _write(tmp_path / "out", frame, backend=PandasBackend(timezone="Africa/Johannesburg"))
        value = pq.read_table(tmp_path / "out").column("ts")[0].as_py()
        assert value == dt.datetime(2024, 1, 2, 1, 4, 5, tzinfo=dt.timezone.utc)

    def test_an_all_null_column_is_text(self, tmp_path):
        _write(tmp_path / "out", pd.DataFrame({"x": [1], "n": [None]}))
        assert self._schema(tmp_path / "out").field("n").type == pa.string()

    def test_a_named_index_is_kept_as_columns(self, tmp_path):
        frame = pd.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3]}).groupby("k").sum()
        _write(tmp_path / "out", frame)
        assert self._schema(tmp_path / "out").names == ["k", "v"]

    def test_an_unnamed_index_is_dropped(self, tmp_path):
        frame = pd.DataFrame({"v": [1, 2, 3]})
        _write(tmp_path / "out", frame[frame.v > 1])
        assert self._schema(tmp_path / "out").names == ["v"]

    def test_compression_option(self, tmp_path):
        _write(tmp_path / "out", pd.DataFrame({"x": [1]}), options={"compression": "zstd"})
        assert _parts(tmp_path / "out")[0].endswith(".zstd.parquet")

    def test_what_pandas_wrote_reads_back_the_same(self, tmp_path):
        frame = pd.DataFrame(
            {
                "i": pd.array([1, None], dtype="Int32"),
                "s": ["a", None],
                "f": [1.5, None],
            }
        )
        _write(tmp_path / "out", frame)
        back = PandasBackend().read_frame("parquet", str(tmp_path / "out")).native
        types = {c: back[c].dtype.pyarrow_dtype for c in back.columns}
        assert types == {"i": pa.int32(), "s": pa.string(), "f": pa.float64()}
        assert back["i"].isna().tolist() == [False, True]


class TestRefusals:
    def test_partition_by_is_refused_not_ignored(self, tmp_path):
        with pytest.raises(SinkWriteError, match="partition"):
            _write(tmp_path / "out", pd.DataFrame({"x": [1]}), partition_by=["x"])
        assert not (tmp_path / "out").exists()

    def test_unknown_write_option_is_refused(self, tmp_path):
        with pytest.raises(SinkWriteError, match="maxRecordsPerFile"):
            _write(tmp_path / "out", pd.DataFrame({"x": [1]}), options={"maxRecordsPerFile": 5})

    def test_something_that_is_not_a_pandas_frame(self, tmp_path):
        with pytest.raises(SinkWriteError, match="list"):
            _write(tmp_path / "out", [1, 2])

    def test_duplicate_column_names(self, tmp_path):
        frame = pd.DataFrame([[1, 2]], columns=["a", "a"])
        with pytest.raises(SinkWriteError, match="duplicate"):
            _write(tmp_path / "out", frame)

    def test_nested_values_cannot_go_to_csv(self, tmp_path):
        with pytest.raises(SinkWriteError, match="csv"):
            _write(tmp_path / "out", pd.DataFrame({"x": [[1, 2]]}), fmt="csv")

    def test_remote_path(self):
        with pytest.raises(SinkWriteError, match="local paths"):
            _write("abfss://c@a.dfs.core.windows.net/x", pd.DataFrame({"x": [1]}))


def test_write_options_table_is_public():
    assert pandas_io.WRITE_OPTIONS["csv"] >= {"header", "sep"}
