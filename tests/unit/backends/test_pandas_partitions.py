"""The pandas backend writes and reads Spark's partition folders (F-012).

Every expected folder name, file name, type and value here comes from Spark
4.2 itself (the golden cases G01 to G44 probed on live Spark, and Spark's
source). ``tests/integration/test_pandas_partitions_parity.py`` checks the
same against a live SparkSession.
"""

from __future__ import annotations

import datetime as dt
import decimal
import logging
import os
import re

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.errors import SinkWriteError, SourceReadError  # noqa: E402
from ubunye.core.write_modes import ResolvedWriteMode  # noqa: E402

OVERWRITE = ResolvedWriteMode(mode="overwrite", save_mode="overwrite")
APPEND = ResolvedWriteMode(mode="append", save_mode="append")
DYNAMIC = ResolvedWriteMode(mode="overwrite_partitions", save_mode="overwrite")
ERROR = ResolvedWriteMode(mode="errorifexists", save_mode="errorifexists")
IGNORE = ResolvedWriteMode(mode="ignore", save_mode="ignore")
WINDOWS = os.name == "nt"
UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
UTC = dt.timezone.utc


def _write(target, frame, by, mode=OVERWRITE, fmt="parquet", zone="UTC", **kw):
    PandasBackend(timezone=zone).execute_write(
        frame,
        mode,
        connector="s3",
        file_format=fmt,
        path=str(target),
        partition_by=by,
        **kw,
    )


def _read(target, fmt="parquet", zone="UTC", **kw):
    return PandasBackend(timezone=zone).read_frame(fmt, str(target), **kw).native


def _types(frame):
    return [(c, frame[c].dtype.pyarrow_dtype) for c in frame.columns]


def _dirs(root):
    """Every folder under root, relative, with forward slashes."""
    out = []
    for here, dirs, _ in os.walk(root):
        for d in dirs:
            out.append(os.path.relpath(os.path.join(here, d), root).replace(os.sep, "/"))
    return sorted(out)


def _files(root):
    out = []
    for here, _, names in os.walk(root):
        for n in names:
            out.append(os.path.relpath(os.path.join(here, n), root).replace(os.sep, "/"))
    return sorted(out)


def _build(root, leaves, data=None, fmt="parquet"):
    """Folders written by hand, one data file each holding ``id`` = its position."""
    for n, leaf in enumerate(leaves):
        folder = os.path.join(root, *leaf.split("/")) if leaf else str(root)
        os.makedirs(folder, exist_ok=True)
        if fmt == "parquet":
            table = data if data is not None else pa.table({"id": pa.array([n], pa.int64())})
            pq.write_table(table, os.path.join(folder, f"part-{n}.parquet"))
        elif fmt == "csv":
            with open(os.path.join(folder, f"part-{n}.csv"), "w") as fh:
                fh.write(f"id\n{n}\n")
        else:
            with open(os.path.join(folder, f"part-{n}.json"), "w") as fh:
                fh.write(f'{{"id":{n}}}\n')


# --------------------------------------------------------------------------- #
# Folder names (W1 to W9)
# --------------------------------------------------------------------------- #

# G01: value -> folder, on every host, and the extra escapes Spark adds on Windows.
ALWAYS = {
    "plain": "p=plain",
    "a/b": "p=a%2Fb",
    "a=b": "p=a%3Db",
    "a%b": "p=a%25b",
    "a:b": "p=a%3Ab",
    "a#b": "p=a%23b",
    "a?b": "p=a%3Fb",
    "a*b": "p=a%2Ab",
    'a"b': "p=a%22b",
    "a'b": "p=a%27b",
    "a\\b": "p=a%5Cb",
    "a{b}": "p=a%7Bb}",
    "a[b]": "p=a%5Bb%5D",
    "a^b": "p=a%5Eb",
    "a\tb": "p=a%09b",
    "a\nb": "p=a%0Ab",
    "a\x7fb": "p=a%7Fb",
    "a~!@$&()+,;b": "p=a~!@$&()+,;b",
    "café": "p=café",
    "日本": "p=日本",
    "a%2Fb": "p=a%252Fb",
    "-": "p=-",
}
ON_WINDOWS = {"a b": "p=a%20b", "a<b": "p=a%3Cb", "a>b": "p=a%3Eb", "a|b": "p=a%7Cb"}
ELSEWHERE = {"a b": "p=a b", "a<b": "p=a<b", "a>b": "p=a>b", "a|b": "p=a|b"}


class TestFolderNames:
    def test_every_escaped_character_as_spark_writes_it(self, tmp_path):
        values = {**ALWAYS, **(ON_WINDOWS if WINDOWS else {})}
        values = {**values, "": "p=__HIVE_DEFAULT_PARTITION__"}
        frame = pd.DataFrame({"id": range(len(values) + 1), "p": list(values) + [None]})
        _write(tmp_path / "out", frame, ["p"])
        assert _dirs(tmp_path / "out") == sorted(set(values.values()))
        back = _read(tmp_path / "out")
        assert _types(back) == [("id", pa.int64()), ("p", pa.string())]
        # Every value round trips, except that "" comes back null (as on Spark).
        got = dict(zip(back["id"], back["p"].astype(object).where(back["p"].notna(), None)))
        for i, v in enumerate(list(values) + [None]):
            assert got[i] == (v or None), v

    def test_windows_escapes_follow_the_host(self):
        from ubunye.adapters.pandas_partitions import escape

        for value, folder in ON_WINDOWS.items():
            assert escape(value, windows=True) == folder[2:]
        for value, folder in ELSEWHERE.items():
            assert escape(value, windows=False) == folder[2:]

    def test_the_column_name_is_escaped_too(self, tmp_path):  # G02
        _write(tmp_path / "out", pd.DataFrame({"id": [1], "k:1 y": ["x"]}), ["k:1 y"])
        assert _dirs(tmp_path / "out") == ["k%3A1%20y=x" if WINDOWS else "k%3A1 y=x"]
        assert list(_read(tmp_path / "out").columns) == ["id", "k:1 y"]

    def test_levels_nest_in_partition_by_order(self, tmp_path):  # G03
        frame = pd.DataFrame(
            {"id": pd.array([1, 2], "int32[pyarrow]"), "a": ["x", "y"], "b": [10, 20]}
        )
        _write(tmp_path / "out", frame, ["b", "a"])
        assert _dirs(tmp_path / "out") == ["b=10", "b=10/a=x", "b=20", "b=20/a=y"]
        back = _read(tmp_path / "out").sort_values("id")
        assert _types(back) == [("id", pa.int32()), ("b", pa.int32()), ("a", pa.string())]
        assert back.values.tolist() == [[1, 10, "x"], [2, 20, "y"]]

    def test_partition_by_is_matched_ignoring_case_and_names_the_folder(self, tmp_path):  # W8
        _write(tmp_path / "out", pd.DataFrame({"id": [1], "a": ["x"]}), ["A"])
        assert _dirs(tmp_path / "out") == ["A=x"]
        (part,) = [f for f in _files(tmp_path / "out") if f.startswith("A=x/part-")]
        assert pq.read_schema(tmp_path / "out" / part).names == ["id"]


class TestValueText:
    def test_small_integers_and_null(self, tmp_path):  # G04
        frame = pd.DataFrame({"id": [1, 2, 3], "p": pd.array([1, -5, None], "int8[pyarrow]")})
        _write(tmp_path / "out", frame, ["p"])
        assert _dirs(tmp_path / "out") == ["p=-5", "p=1", "p=__HIVE_DEFAULT_PARTITION__"]
        back = _read(tmp_path / "out").sort_values("id")
        assert back["p"].dtype.pyarrow_dtype == pa.int32()  # Spark: tinyint reads as int
        assert back["p"].tolist()[:2] == [1, -5] and pd.isna(back["p"].tolist()[2])

    def test_big_integers(self, tmp_path):  # G05
        values = [1, 2147483648, -9223372036854775808]
        _write(tmp_path / "out", pd.DataFrame({"id": [1, 2, 3], "p": values}), ["p"])
        assert _dirs(tmp_path / "out") == sorted(f"p={v}" for v in values)
        back = _read(tmp_path / "out")
        assert back["p"].dtype.pyarrow_dtype == pa.int64()
        assert sorted(back["p"].tolist()) == sorted(values)

    def test_booleans_read_back_as_text(self, tmp_path):  # G11
        frame = pd.DataFrame({"id": [1, 2, 3], "p": pd.array([True, False, None], "boolean")})
        _write(tmp_path / "out", frame, ["p"])
        assert _dirs(tmp_path / "out") == ["p=__HIVE_DEFAULT_PARTITION__", "p=false", "p=true"]
        back = _read(tmp_path / "out").sort_values("id")
        assert back["p"].dtype.pyarrow_dtype == pa.string()
        assert back["p"].tolist()[:2] == ["true", "false"]

    def test_dates(self, tmp_path):  # G12
        days = [dt.date(2024, 1, 31), dt.date(1, 1, 1), dt.date(9999, 12, 31)]
        frame = pd.DataFrame({"id": [1, 2, 3], "p": pd.array(days, pd.ArrowDtype(pa.date32()))})
        _write(tmp_path / "out", frame, ["p"])
        assert _dirs(tmp_path / "out") == ["p=0001-01-01", "p=2024-01-31", "p=9999-12-31"]
        back = _read(tmp_path / "out").sort_values("id")
        assert back["p"].dtype.pyarrow_dtype == pa.date32() and back["p"].tolist() == days

    @staticmethod
    def _stamps(*values):
        return pd.array(list(values), pd.ArrowDtype(pa.timestamp("us", tz="UTC")))

    def test_timestamps_with_fractions_read_back_as_text(self, tmp_path):  # G13
        stamps = self._stamps(
            dt.datetime(2024, 1, 31, 10, 5, tzinfo=UTC),
            dt.datetime(2024, 1, 31, 10, 5, 6, 123456, tzinfo=UTC),
            dt.datetime(2024, 1, 31, 10, 5, 6, 500000, tzinfo=UTC),
            dt.datetime(2024, 1, 31, 0, 0, tzinfo=UTC),
        )
        _write(tmp_path / "out", pd.DataFrame({"id": [1, 2, 3, 4], "p": stamps}), ["p"])
        sp = "%20" if WINDOWS else " "
        assert _dirs(tmp_path / "out") == [
            f"p=2024-01-31{sp}00%3A00%3A00",
            f"p=2024-01-31{sp}10%3A05%3A00",
            f"p=2024-01-31{sp}10%3A05%3A06.123456",
            f"p=2024-01-31{sp}10%3A05%3A06.5",
        ]
        # `.123456` is not a timestamp to Spark's partition inference: text.
        assert _read(tmp_path / "out")["p"].dtype.pyarrow_dtype == pa.string()

    def test_whole_seconds_and_one_digit_read_back_as_timestamps(self, tmp_path):  # G14
        stamps = self._stamps(
            dt.datetime(2024, 1, 31, 10, 5, tzinfo=UTC),
            dt.datetime(2024, 1, 31, 10, 5, 6, 500000, tzinfo=UTC),
        )
        _write(tmp_path / "out", pd.DataFrame({"id": [1, 2], "p": stamps}), ["p"])
        back = _read(tmp_path / "out").sort_values("id")
        assert back["p"].dtype.pyarrow_dtype == pa.timestamp("us", tz="UTC")
        assert list(back["p"]) == list(stamps)

    def test_a_timestamp_is_written_in_the_session_zone(self, tmp_path):  # G15
        stamps = self._stamps(dt.datetime(2024, 1, 31, 10, 5, tzinfo=UTC))
        zone = "Africa/Johannesburg"
        _write(tmp_path / "out", pd.DataFrame({"id": [1], "p": stamps}), ["p"], zone=zone)
        sp = "%20" if WINDOWS else " "
        assert _dirs(tmp_path / "out") == [f"p=2024-01-31{sp}12%3A05%3A00"]
        assert list(_read(tmp_path / "out", zone=zone)["p"]) == list(stamps)  # same instant
        shifted = _read(tmp_path / "out", zone="UTC")["p"][0]  # read in UTC: 2 h later
        assert shifted == pd.Timestamp("2024-01-31 12:05", tz="UTC")


# --------------------------------------------------------------------------- #
# Files (W10 to W16)
# --------------------------------------------------------------------------- #


class TestFiles:
    def test_data_files_leave_out_the_partition_columns(self, tmp_path):  # G20
        frame = pd.DataFrame({"c1": [1], "p": ["x"], "c3": [3], "c4": ["s"]})
        _write(tmp_path / "out", frame, ["p"])
        (part,) = [f for f in _files(tmp_path / "out") if f.startswith("p=x/")]
        assert pq.read_schema(tmp_path / "out" / part).names == ["c1", "c3", "c4"]
        assert list(_read(tmp_path / "out").columns) == ["c1", "c3", "c4", "p"]

    def test_one_file_per_leaf_named_as_spark_names_it(self, tmp_path):  # W11, W14
        frame = pd.DataFrame({"id": [1, 2, 3, 4], "a": list("xyxy")})
        _write(tmp_path / "out", frame, ["a"])
        files = _files(tmp_path / "out")
        assert files[0] == "_SUCCESS" and len(files) == 3  # _SUCCESS at the root only
        pattern = rf"a=[xy]/part-00000-({UUID})\.c000\.snappy\.parquet"
        uuids = {re.fullmatch(pattern, f).group(1) for f in files[1:]}
        assert len(uuids) == 1  # one uuid per write

    @pytest.mark.parametrize(
        "fmt, ending",
        [("csv", ".c000.csv"), ("json", ".c000.json"), ("parquet", ".c000.snappy.parquet")],
    )
    def test_file_endings(self, tmp_path, fmt, ending):  # W13
        _write(tmp_path / "out", pd.DataFrame({"id": [1], "a": ["x"]}), ["a"], fmt=fmt)
        (part,) = [f for f in _files(tmp_path / "out") if f.startswith("a=x/")]
        assert part.endswith(ending)

    def test_each_csv_file_has_its_own_header_of_data_columns(self, tmp_path):  # W16
        frame = pd.DataFrame({"id": [1, 2], "b": [10, 20], "a": ["x", "y"]})
        _write(tmp_path / "out", frame, ["a"], fmt="csv", options={"header": "true"})
        for leaf, row in (("a=x", "1,10"), ("a=y", "2,20")):
            (part,) = os.listdir(tmp_path / "out" / leaf)
            text = (tmp_path / "out" / leaf / part).read_text(encoding="utf-8")
            assert text == f"id,b\n{row}\n"
        back = _read(tmp_path / "out", fmt="csv", options={"header": "true"})
        assert _types(back) == [("id", pa.string()), ("b", pa.string()), ("a", pa.string())]

    def test_json_files_hold_the_data_columns(self, tmp_path):
        _write(
            tmp_path / "out", pd.DataFrame({"id": [2], "b": [20], "a": ["x"]}), ["a"], fmt="json"
        )
        (part,) = os.listdir(tmp_path / "out" / "a=x")
        assert (tmp_path / "out" / "a=x" / part).read_text(encoding="utf-8") == '{"id":2,"b":20}\n'
        assert list(_read(tmp_path / "out", fmt="json").columns) == ["b", "id", "a"]

    def test_an_empty_frame_leaves_only_the_success_marker(self, tmp_path):  # W15
        frame = pd.DataFrame({"id": pd.array([], "int64[pyarrow]"), "a": pd.array([], "string")})
        _write(tmp_path / "out", frame, ["a"])
        assert _files(tmp_path / "out") == ["_SUCCESS"]
        with pytest.raises(SourceReadError):  # Spark: UNABLE_TO_INFER_SCHEMA
            _read(tmp_path / "out")


# --------------------------------------------------------------------------- #
# Modes (O1 to O7)
# --------------------------------------------------------------------------- #

BEFORE = pd.DataFrame({"id": [1, 2, 4, 3], "p": [1, 2, 2, 3], "q": ["a", "a", "b", "a"]})
NEW = pd.DataFrame({"id": [10, 11], "p": [2, 4], "q": ["a", "a"]})


def _golden_before(root):
    """Section 5 of the spec: four partitions, a stray file at the root and in p=1/q=a."""
    _write(root, BEFORE, ["p", "q"])
    (root / "stray.txt").write_text("x")
    (root / "p=1" / "q=a" / "stray.txt").write_text("x")


def _ids(root):
    for stray in (root / "stray.txt", root / "p=1" / "q=a" / "stray.txt"):
        if stray.exists():
            stray.unlink()
    return sorted(_read(root)["id"].tolist())


class TestModes:
    def test_static_overwrite_replaces_everything(self, tmp_path):  # O1
        out = tmp_path / "out"
        _golden_before(out)
        _write(out, NEW, ["p", "q"])
        assert _dirs(out) == ["p=2", "p=2/q=a", "p=4", "p=4/q=a"]
        assert "stray.txt" not in os.listdir(out)
        assert _ids(out) == [10, 11]

    def test_dynamic_overwrite_replaces_only_the_partitions_written(self, tmp_path):  # O3, G23
        out = tmp_path / "out"
        _golden_before(out)
        old_sibling = _files(out / "p=2" / "q=b")
        stamp = os.stat(out / "_SUCCESS").st_mtime_ns
        _write(out, NEW, ["p", "q"], mode=DYNAMIC)
        assert _dirs(out) == [
            "p=1", "p=1/q=a", "p=2", "p=2/q=a", "p=2/q=b", "p=3", "p=3/q=a", "p=4", "p=4/q=a"
        ]  # fmt: skip
        assert (out / "stray.txt").exists() and (out / "p=1" / "q=a" / "stray.txt").exists()
        assert _files(out / "p=2" / "q=b") == old_sibling  # sibling leaf untouched
        assert os.stat(out / "_SUCCESS").st_mtime_ns == stamp  # O4: left as it was
        assert _ids(out) == [1, 3, 4, 10, 11]
        assert sorted(os.listdir(tmp_path)) == ["out"]  # no staging left behind

    def test_dynamic_overwrite_of_a_new_target_writes_no_success_marker(self, tmp_path):  # O4
        _write(tmp_path / "out", NEW, ["p", "q"], mode=DYNAMIC)
        assert "_SUCCESS" not in os.listdir(tmp_path / "out")
        assert _ids(tmp_path / "out") == [10, 11]

    def test_dynamic_overwrite_with_an_empty_frame_changes_nothing(self, tmp_path):  # O5
        out = tmp_path / "out"
        _golden_before(out)
        before = _files(out)
        _write(out, NEW.head(0), ["p", "q"], mode=DYNAMIC)
        assert _files(out) == before

    def test_overwrite_partitions_without_partition_by_is_a_full_overwrite(self, tmp_path):  # O2
        out = tmp_path / "out"
        _golden_before(out)
        _write(out, NEW, None, mode=DYNAMIC)
        assert _dirs(out) == [] and _ids(out) == [10, 11]

    def test_a_failed_swap_puts_every_partition_back(self, tmp_path, monkeypatch):
        out = tmp_path / "out"
        _golden_before(out)
        before = {f: (out / f).read_bytes() for f in _files(out)}
        real = os.replace
        moved = []

        def flaky(src, dst):
            moved.append(dst)
            if str(dst).endswith(os.path.join("p=4", "q=a")):  # the second leaf moving in
                raise OSError("disk gone")
            return real(src, dst)

        monkeypatch.setattr(os, "replace", flaky)
        with pytest.raises(SinkWriteError, match="disk gone"):
            _write(out, NEW, ["p", "q"], mode=DYNAMIC)
        monkeypatch.setattr(os, "replace", real)
        assert any(str(d).endswith(os.path.join("p=2", "q=a")) for d in moved)  # it did swap one
        after = {f: (out / f).read_bytes() for f in _files(out)}
        assert after == before  # p=2/q=a is the old one again, p=4/q=a never appeared
        assert sorted(os.listdir(tmp_path)) == ["out"]

    def test_append_adds_a_file_to_each_leaf(self, tmp_path):  # O6
        out = tmp_path / "out"
        _write(out, BEFORE, ["p", "q"], mode=APPEND)
        _write(out, NEW, ["p", "q"], mode=APPEND)
        assert len(os.listdir(out / "p=2" / "q=a")) == 2  # one per write, different uuids
        assert _ids(out) == [1, 2, 3, 4, 10, 11]

    def test_errorifexists_and_ignore(self, tmp_path):  # O7
        out = tmp_path / "out"
        _write(out, BEFORE, ["p", "q"], mode=ERROR)
        with pytest.raises(SinkWriteError, match="already exists"):
            _write(out, NEW, ["p", "q"], mode=ERROR)
        _write(out, NEW, ["p", "q"], mode=IGNORE)
        assert _ids(out) == [1, 2, 3, 4]


# --------------------------------------------------------------------------- #
# Refusals (X1 to X4, W7, and what Spark does not read back the same)
# --------------------------------------------------------------------------- #


class TestRefusals:
    @pytest.mark.parametrize(
        "frame, by, message",
        [
            (pd.DataFrame({"a": [1], "b": [2]}), ["a", "b"], "Cannot use all columns"),  # X1
            (pd.DataFrame({"a": [1], "b": [2]}), ["zzz"], "Partition column `zzz` not found"),
            (pd.DataFrame({"a": [1], "b": [2], "c": [3]}), ["a", "A"], "already exists"),  # X3
            (pd.DataFrame({"a": [1], "p": [1.5]}), ["p"], "double"),
            (
                pd.DataFrame({"a": [1], "p": [decimal.Decimal("1.50")]}),
                ["p"],
                "decimal",
            ),
            (pd.DataFrame({"a": [1], "p": [b"ab"]}), ["p"], "binary"),
            (pd.DataFrame({"a": [1], "p": [[1, 2]]}), ["p"], "nested"),
            (pd.DataFrame({"a": [1], "p": [dt.time(10, 5)]}), ["p"], "time"),
            (pd.DataFrame({"a": [1], "p": [pd.Timedelta("1D")]}), ["p"], "duration"),
            (
                pd.DataFrame({"a": [1, 2], "p": ["__HIVE_DEFAULT_PARTITION__", None]}),
                ["p"],
                "map to the one folder",
            ),  # W7
            (pd.DataFrame({"a": [1], "p": ["a\x00b"]}), ["p"], "NUL"),
        ],
        ids=[
            "all-columns",
            "missing",
            "twice",
            "double",
            "decimal",
            "binary",
            "nested",
            "time",
            "interval",
            "default-name-and-null",
            "nul",
        ],
    )
    def test_refused_before_anything_is_written(self, tmp_path, frame, by, message):
        with pytest.raises(SinkWriteError, match=message):
            _write(tmp_path / "out", frame, by, mode=APPEND)
        assert not (tmp_path / "out").exists()
        assert os.listdir(tmp_path) == []

    def test_the_refusal_says_why_and_what_to_do(self, tmp_path):
        with pytest.raises(SinkWriteError) as info:
            _write(tmp_path / "out", pd.DataFrame({"a": [1], "p": [1.5]}), ["p"])
        assert "does not read them back as the same type" in str(info.value)
        assert "Cast the column to a string" in info.value.hint

    @pytest.mark.skipif(not WINDOWS, reason="Windows drops a trailing dot from a folder name")
    def test_a_value_windows_would_change_is_refused(self, tmp_path):
        with pytest.raises(SinkWriteError, match="ends in '.'"):
            _write(tmp_path / "out", pd.DataFrame({"a": [1], "p": ["5."]}), ["p"])

    def test_values_that_differ_only_in_case_are_refused_everywhere(self, tmp_path):
        # Windows and macOS disks ignore case: the two folders would be one.
        with pytest.raises(SinkWriteError, match="differ only in case"):
            _write(tmp_path / "out", pd.DataFrame({"a": [1, 2], "p": ["A", "a"]}), ["p"])

    def test_the_default_name_alone_is_written_and_reads_back_null(self, tmp_path):  # G18
        frame = pd.DataFrame({"a": [1], "p": ["__HIVE_DEFAULT_PARTITION__"]})
        _write(tmp_path / "out", frame, ["p"])
        back = _read(tmp_path / "out")
        # Spark reads an all null partition column as `void`; pandas has none, so text.
        assert back["p"].dtype.pyarrow_dtype == pa.string() and back["p"].isna().all()

    def test_the_backend_declares_it(self):
        caps = PandasBackend.CAPABILITIES
        assert "partitioned_writes" in caps.features
        assert "overwrite_partitions" in caps.write_modes


# --------------------------------------------------------------------------- #
# Reading: discovery and inference (R1 to R9, F1)
# --------------------------------------------------------------------------- #

INFERENCE = [
    (["p=1", "p=2147483648"], pa.int64(), [1, 2147483648]),  # G24
    (["p=1", "p=12345678901234567890"], pa.decimal128(20, 0), [1, 12345678901234567890]),  # G25
    (["p=1", "p=1.5"], pa.float64(), [1.0, 1.5]),  # G26
    (["p=2147483648", "p=1.5"], pa.string(), ["2147483648", "1.5"]),  # G27
    (["p=1", "p=2024-01-31"], pa.string(), ["1", "2024-01-31"]),  # G28
    (["p=007"], pa.int32(), [7]),  # G30
    (["p=1e3"], pa.decimal128(4, 0), [1000]),  # G31
    (["p=9223372036854775808"], pa.decimal128(19, 0), [9223372036854775808]),
    (["p=" + "9" * 39], pa.float64(), [1e39]),
    (["p=1.5d"], pa.float64(), [1.5]),  # G32
    (["p=Infinity"], pa.float64(), [float("inf")]),
    (["p=true"], pa.string(), ["true"]),  # G33
    (["p=0x10"], pa.string(), ["0x10"]),
    (["p=1_000"], pa.string(), ["1_000"]),
    (["p=2024-1-5"], pa.string(), ["2024-1-5"]),
    (["p=2024-01-31T10%3A05%3A00"], pa.string(), ["2024-01-31T10:05:00"]),
    (["p=10%3A05%3A06"], pa.string(), ["10:05:06"]),  # G34: Spark says time(6)
    (["p=a%2fb"], pa.string(), ["a/b"]),  # G35
    (["p=a%zzb"], pa.string(), ["a%zzb"]),
    (["p=a%2"], pa.string(), ["a%2"]),
    (["p=__HIVE_DEFAULT_PARTITION__", "p=3"], pa.int32(), [None, 3]),  # int + null
    (["p=__HIVE_DEFAULT_PARTITION__", "p=2024-01-31"], pa.date32(), [None, dt.date(2024, 1, 31)]),
    (["p=1.5", "p=1.0E20"], pa.string(), ["1.5", "1.0E20"]),  # decimal + double
    (
        ["p=2024-01-31", "p=2024-01-31%2010%3A05%3A00"],  # G29: date widens to timestamp
        pa.timestamp("us", tz="UTC"),
        [pd.Timestamp("2024-01-31", tz="UTC"), pd.Timestamp("2024-01-31 10:05", tz="UTC")],
    ),
    (
        ["p=2024-01-31%2010%3A05%3A00", "p=2024-01-31%2010%3A05%3A06.12"],  # 2 digits: text
        pa.string(),
        ["2024-01-31 10:05:00", "2024-01-31 10:05:06.12"],
    ),
]


class TestReadInference:
    @pytest.mark.parametrize("leaves, arrow_type, values", INFERENCE, ids=lambda x: str(x)[:40])
    def test_type_and_values_as_spark_infers_them(self, tmp_path, leaves, arrow_type, values):
        _build(tmp_path / "t", leaves)
        back = _read(tmp_path / "t").sort_values("id")
        assert _types(back) == [("id", pa.int64()), ("p", arrow_type)]
        got = [None if pd.isna(v) else v for v in back["p"].tolist()]
        assert got == values

    def test_nulls_everywhere_are_text(self, tmp_path):
        _build(tmp_path / "t", ["p=__HIVE_DEFAULT_PARTITION__"])
        assert _types(_read(tmp_path / "t")) == [("id", pa.int64()), ("p", pa.string())]

    def test_timestamps_are_read_in_the_session_zone(self, tmp_path):
        _build(tmp_path / "t", ["p=2024-01-31%2012%3A05%3A00"])
        back = _read(tmp_path / "t", zone="Africa/Johannesburg")
        assert back["p"][0] == pd.Timestamp("2024-01-31 10:05", tz="UTC")

    def test_widening_rules(self):
        from ubunye.adapters.pandas_partitions import wider

        assert wider(("int",), ("long",)) == ("long",)
        assert wider(("int",), ("decimal", 20)) == ("decimal", 20)
        assert wider(("int",), ("decimal", 4)) == ("decimal", 10)
        assert wider(("int",), ("double",)) == ("double",)
        assert wider(("long",), ("double",)) == ("string",)
        assert wider(("decimal", 21), ("double",)) == ("string",)
        assert wider(("int",), ("date",)) == ("string",)
        assert wider(("date",), ("timestamp",)) == ("timestamp",)
        assert wider(("int",), ("string",)) == ("string",)
        assert wider(("null",), ("date",)) == ("date",)


class TestReadLayouts:
    def test_two_levels_come_after_the_data_columns(self, tmp_path):
        _build(tmp_path / "t", ["z=1/a=x", "z=2/a=__HIVE_DEFAULT_PARTITION__"])
        back = _read(tmp_path / "t").sort_values("id")
        assert _types(back) == [("id", pa.int64()), ("z", pa.int32()), ("a", pa.string())]
        assert back["z"].tolist() == [1, 2] and back["a"].tolist()[0] == "x"

    def test_names_spark_skips_are_skipped(self, tmp_path):  # G36, R7
        root = tmp_path / "t"
        _build(root, ["p=1", "_tmpdir/p=9", ".dotdir/p=8", "p=2/_temporary/0"])
        for name in ("_hidden.parquet", ".hidden.parquet", "x._COPYING_"):
            pq.write_table(pa.table({"id": [99]}), root / "p=1" / name)
        (root / "p=1" / "_SUCCESS").write_text("")
        (root / "p=1" / "empty.parquet").write_bytes(b"")  # zero length: skipped
        back = _read(root)
        assert back["id"].tolist() == [0] and back["p"].tolist() == [1]

    def test_a_file_beside_partition_folders_is_dropped_with_a_warning(self, tmp_path, caplog):
        _build(tmp_path / "t", ["", "p=1"])  # G38
        with caplog.at_level(logging.WARNING):
            back = _read(tmp_path / "t")
        assert back["id"].tolist() == [1] and back["p"].tolist() == [1]
        assert "part-0.parquet" in caplog.text and "not read" in caplog.text

    @pytest.mark.parametrize(
        "leaves",
        [["p=1", "_p=7"], ["other", "p=1"], ["a=1", "b=1"], ["a=1/b=1", "a=2"]],
        ids=["G37", "G39", "G40-names", "G40-depth"],
    )
    def test_conflicting_layouts_are_refused_as_spark_refuses_them(self, tmp_path, leaves):
        _build(tmp_path / "t", leaves)
        with pytest.raises(SourceReadError, match="Conflicting"):
            _read(tmp_path / "t")

    def test_an_empty_value_is_refused(self, tmp_path):  # R2
        _build(tmp_path / "t", ["p="])
        with pytest.raises(SourceReadError, match="empty column name or value"):
            _read(tmp_path / "t")

    def test_names_differing_in_case_are_one_column(self, tmp_path):  # G41
        _build(tmp_path / "t", ["p=1", "P=2"])
        back = _read(tmp_path / "t")
        assert len(back.columns) == 2 and sorted(back.iloc[:, 1].tolist()) == [1, 2]

    def test_a_partition_column_in_the_data_takes_the_folder_value(self, tmp_path):  # G42
        data = pa.table({"p": ["from_file"], "id": pa.array([1], pa.int64())})
        _build(tmp_path / "t", ["p=5"], data=data)
        back = _read(tmp_path / "t")
        assert _types(back) == [("p", pa.int32()), ("id", pa.int64())]
        assert back.values.tolist() == [[5, 1]]

    def test_a_sub_folder_has_no_parent_column(self, tmp_path):  # G43, R9
        _build(tmp_path / "t", ["p=1/q=a", "p=2/q=b"])
        assert list(_read(tmp_path / "t" / "p=1").columns) == ["id", "q"]

    @pytest.mark.parametrize("fmt", ["csv", "json"])
    def test_text_formats_discover_the_same(self, tmp_path, fmt):  # G44, F1
        _build(tmp_path / "t", ["p=007/d=2024-01-31", "p=1/d=__HIVE_DEFAULT_PARTITION__"], fmt=fmt)
        opts = {"header": "true"} if fmt == "csv" else {}
        back = _read(tmp_path / "t", fmt=fmt, options=opts).sort_values("p")
        assert _types(back)[1:] == [("p", pa.int32()), ("d", pa.date32())]
        assert back["p"].tolist() == [1, 7]
        assert pd.isna(back["d"].tolist()[0]) and back["d"].tolist()[1] == dt.date(2024, 1, 31)

    def test_a_schema_names_the_partition_type(self, tmp_path):
        _build(tmp_path / "t", ["p=007"])
        back = _read(tmp_path / "t", schema="id BIGINT, p STRING")
        assert _types(back) == [("id", pa.int64()), ("p", pa.string())]
        assert back["p"].tolist() == ["007"]  # not inferred: Spark keeps the text too

    def test_a_plain_sub_folder_is_read_as_spark_reads_it(self, tmp_path):
        # Spark reads only the files directly in an unpartitioned folder.
        _build(tmp_path / "t", ["", "sub"])
        assert _read(tmp_path / "t")["id"].tolist() == [0]

    def test_files_in_a_plain_folder_under_a_partition_belong_to_it(self, tmp_path):
        _build(tmp_path / "t", ["p=1", "p=1/sub"])
        back = _read(tmp_path / "t")
        assert sorted(back["id"].tolist()) == [0, 1] and back["p"].tolist() == [1, 1]

    def test_what_spark_wrote_in_two_tasks_reads_whole(self, tmp_path):  # W12
        root = tmp_path / "t"
        for task in (0, 1):
            for leaf in ("a=x", "a=y"):
                os.makedirs(root / leaf, exist_ok=True)
                name = f"part-0000{task}-0a0a0a0a-0000-0000-0000-000000000000.c000.snappy.parquet"
                pq.write_table(pa.table({"id": [task]}), root / leaf / name)
        (root / "_SUCCESS").write_text("")
        back = _read(root)
        assert len(back) == 4 and sorted(back["a"].tolist()) == ["x", "x", "y", "y"]


# --------------------------------------------------------------------------- #
# Review of F-012 (skeptic round 3): each case lost or changed data before
# --------------------------------------------------------------------------- #


class TestReview:
    def test_an_old_partition_that_cannot_be_put_back_is_kept_and_named(
        self, tmp_path, monkeypatch
    ):
        out = tmp_path / "out"
        _golden_before(out)
        (old_part,) = os.listdir(out / "p=2" / "q=a")
        old_bytes = (out / "p=2" / "q=a" / old_part).read_bytes()
        real = os.replace
        target = os.path.join(str(out), "p=2", "q=a")

        def broken(src, dst):
            if os.path.normcase(os.path.abspath(dst)) == os.path.normcase(target):
                raise OSError("disk gone")  # neither the new leaf nor the old one goes in
            return real(src, dst)

        monkeypatch.setattr(os, "replace", broken)
        with pytest.raises(SinkWriteError, match="could not be put back") as info:
            _write(out, NEW, ["p", "q"], mode=DYNAMIC)
        monkeypatch.setattr(os, "replace", real)
        (kept,) = info.value.context["Kept"]
        assert kept.endswith(os.path.join(".old", "p=2", "q=a"))  # named by its partition
        assert (open(os.path.join(kept, old_part), "rb").read()) == old_bytes

    def test_two_instants_with_one_wall_clock_time_are_refused(self, tmp_path):
        # Europe/London, 2024-10-27: 00:30Z and 01:30Z are both 01:30 local. Spark's
        # write fails; one folder would silently merge two values.
        stamps = pd.array(
            [
                dt.datetime(2024, 10, 27, 0, 30, tzinfo=UTC),
                dt.datetime(2024, 10, 27, 1, 30, tzinfo=UTC),
            ],
            pd.ArrowDtype(pa.timestamp("us", tz="UTC")),
        )
        frame = pd.DataFrame({"id": [1, 2], "p": stamps})
        with pytest.raises(SinkWriteError, match="map to the one folder"):
            _write(tmp_path / "out", frame, ["p"], zone="Europe/London")
        assert not (tmp_path / "out").exists()
        _write(tmp_path / "utc", frame, ["p"], zone="UTC")  # two folders in UTC: fine
        assert len(_dirs(tmp_path / "utc")) == 2

    def test_an_outer_level_differing_in_case_is_refused_everywhere(self, tmp_path):
        frame = pd.DataFrame({"id": [1, 2], "p": ["A", "a"], "q": ["x", "y"]})
        with pytest.raises(SinkWriteError, match="differ only in case"):
            _write(tmp_path / "out", frame, ["p", "q"])
        assert not (tmp_path / "out").exists()

    @pytest.mark.parametrize("mode", [APPEND, DYNAMIC], ids=["append", "overwrite_partitions"])
    def test_a_new_folder_matching_an_existing_one_only_in_case_is_refused(self, tmp_path, mode):
        out = tmp_path / "out"
        _write(out, pd.DataFrame({"id": [1], "p": ["a"]}), ["p"])
        before = _files(out)
        with pytest.raises(SinkWriteError, match="differs from it only in case"):
            _write(out, pd.DataFrame({"id": [2], "p": ["A"]}), ["p"], mode=mode)
        assert _files(out) == before

    def test_left_staging_folders_are_named_and_never_deleted(self, tmp_path, caplog):
        out = tmp_path / "out"
        _golden_before(out)
        stale = tmp_path / ".out.ubunye-0123456789ab.old" / "p=2" / "q=a"
        stale.mkdir(parents=True)
        (stale / "part-old.parquet").write_bytes(b"only copy")
        with caplog.at_level(logging.WARNING):
            _write(out, NEW, ["p", "q"], mode=DYNAMIC)
        assert ".out.ubunye-0123456789ab.old" in caplog.text
        assert "the path under it is the partition" in caplog.text
        assert (stale / "part-old.parquet").read_bytes() == b"only copy"

    def test_an_empty_file_in_a_glob_is_skipped_as_spark_skips_it(self, tmp_path):
        folder = tmp_path / "in"
        folder.mkdir()
        pq.write_table(pa.table({"id": [1]}), folder / "a.parquet")
        (folder / "b.parquet").write_bytes(b"")
        assert _read(folder / "*.parquet")["id"].tolist() == [1]
