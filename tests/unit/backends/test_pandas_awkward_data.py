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


class TestCsvLongRows:
    """F-062: Spark has no limit on a value's length; pyarrow stopped past 1 MB."""

    @pytest.mark.parametrize("multiline", ["false", "true"])
    @pytest.mark.parametrize("quote", ["", '"'])
    def test_a_three_megabyte_value(self, tmp_path, multiline, quote):
        big = "x" * 3_000_000
        data = f"id,t\n1,{quote}{big}{quote}\n2,b\n".encode()
        frame = _read_bytes(
            tmp_path, "csv", data, options={"header": "true", "multiLine": multiline}
        )
        assert frame.native["t"].tolist() == [big, "b"]

    def test_a_long_value_in_a_ragged_file_and_a_long_header(self, tmp_path):
        # A row with too few fields sends the file through Python's csv module,
        # which stopped at 131,072 characters.
        big = "z" * 300_000
        data = f"id,{big}\n1,{big}\n2\n".encode()
        frame = _read_bytes(tmp_path, "csv", data, options={"header": "true"})
        assert list(frame.native.columns) == ["id", big]
        assert frame.native[big].tolist()[0] == big
        assert frame.native[big].isna().tolist() == [False, True]

    def test_a_long_value_with_line_breaks_in_another_encoding(self, tmp_path):
        big = ("é\n" * 800_000) + "end"
        data = f'id,t\n1,"{big}"\n'.encode("latin-1")
        frame = _read_bytes(
            tmp_path,
            "csv",
            data,
            options={"header": "true", "multiLine": "true", "encoding": "latin1"},
        )
        assert frame.native["t"].tolist() == [big]


def _types(frame) -> dict:
    native = frame.native
    return {c: native[c].dtype.pyarrow_dtype for c in native.columns}


class TestJsonInference:
    """F-064: JSON is typed by a port of Spark's JsonInferSchema."""

    def test_nested_fields_are_sorted_by_name(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"o":{"b":1,"z":{"y":1,"c":2}}}\n{"o":{"a":2}}\n')
        inner = pa.struct([("c", pa.int64()), ("y", pa.int64())])
        assert _types(frame)["o"] == pa.struct([("a", pa.int64()), ("b", pa.int64()), ("z", inner)])

    def test_a_number_and_text_in_one_field_is_text(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"a":1}\n{"a":"x"}\n{"a":1.50}\n{"a":true}\n')
        assert _types(frame)["a"] == pa.string()
        assert frame.native["a"].tolist() == ["1", "x", "1.5", "true"]

    def test_an_object_and_text_in_one_field_keeps_the_objects_json(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"a":{"k":1,"n":null,"s":"q\\u000b"}}\n{"a":"x"}\n')
        assert frame.native["a"].tolist() == ['{"k":1,"n":null,"s":"q\\u000B"}', "x"]

    def test_arrays_with_mixed_elements_are_arrays_of_text(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"a":[1,"x",{"q":1},[2]]}\n')
        assert _types(frame)["a"] == pa.list_(pa.string())
        assert list(frame.native["a"].tolist()[0]) == ["1", "x", '{"q":1}', "[2]"]

    def test_whole_numbers_past_64_bits_are_decimals(self, tmp_path):
        data = b'{"a":9223372036854775808,"b":123456789012345678901234567890}\n{"a":1,"b":2}\n'
        frame = _read_bytes(tmp_path, "json", data)
        assert _types(frame) == {"a": pa.decimal128(20, 0), "b": pa.decimal128(30, 0)}
        assert str(frame.native["a"][0]) == "9223372036854775808"

    def test_a_number_past_38_digits_is_a_double(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"a":' + b"9" * 40 + b"}\n")
        assert _types(frame)["a"] == pa.float64()

    def test_long_and_double_are_double(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"a":1}\n{"a":1.5}\n{"a":NaN}\n')
        assert _types(frame)["a"] == pa.float64()

    def test_empty_names_and_empty_objects_are_dropped(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"":1,"a":1,"e":{},"l":[{}],"o":{"":2,"k":3}}\n')
        assert _types(frame) == {"a": pa.int64(), "o": pa.struct([("k", pa.int64())])}

    def test_an_empty_string_merges_as_null(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"a":""}\n{"a":""}\n{"b":"","c":5}\n{"c":""}\n')
        assert _types(frame) == {"a": pa.string(), "b": pa.string(), "c": pa.int64()}
        assert frame.native["a"].tolist()[:2] == ["", ""]
        assert frame.native["a"].isna().tolist() == [False, False, True, True]
        assert frame.native["c"].isna().tolist() == [True, True, False, True]

    def test_arrays_of_objects_merge_their_keys(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{"a":[{"y":1},{"x":"s"}]}\n{"a":[]}\n')
        assert _types(frame)["a"] == pa.list_(pa.struct([("x", pa.string()), ("y", pa.int64())]))

    def test_names_sort_by_utf16_like_java(self):
        from ubunye.adapters import spark_json

        # U+FF21 sorts after U+1F600 by code point, before it by UTF-16 unit.
        names = sorted(["\U0001f600", "Ａ", "a"], key=spark_json.java_order)
        assert names == ["a", "\U0001f600", "Ａ"]


class TestJsonRecordsWithNoFields:
    """F-065: records with no fields are rows with no columns, as in Spark."""

    def test_the_rows_are_kept(self, tmp_path):
        frame = _read_bytes(tmp_path, "json", b'{}\n{}\n{"":1,"e":{}}\n')
        assert frame.count() == 3
        assert list(frame.native.columns) == []

    def test_a_transform_can_add_a_column_to_them(self, tmp_path):
        nw = pytest.importorskip("narwhals")
        frame = _read_bytes(tmp_path, "json", b"{}\n{}\n")
        out = nw.from_native(frame.native).with_columns(one=nw.lit(1)).to_native()
        assert out["one"].tolist() == [1, 1]


class TestParquetUnsignedIntegers:
    """F-066: Spark reads parquet UINT_8/16/32/64 as smallint, int, bigint, decimal(20,0)."""

    def test_unsigned_columns_get_sparks_types_and_keep_their_values(self, tmp_path):
        import decimal

        import pyarrow.parquet as pq

        table = pa.table(
            {
                "u8": pa.array([255, None], pa.uint8()),
                "u16": pa.array([65535, 0], pa.uint16()),
                "u32": pa.array([2**32 - 1, 0], pa.uint32()),
                "u64": pa.array([2**64 - 1, 0], pa.uint64()),
                "nested": pa.array([[1, 2], None], pa.list_(pa.uint8())),
                "st": pa.array([{"x": 1}, None], pa.struct([("x", pa.uint64())])),
                "i64": pa.array([-1, 1], pa.int64()),
            }
        )
        path = tmp_path / "u.parquet"
        pq.write_table(table, path)
        frame = PandasBackend().read_frame("parquet", str(path))
        assert _types(frame) == {
            "u8": pa.int16(),
            "u16": pa.int32(),
            "u32": pa.int64(),
            "u64": pa.decimal128(20, 0),
            "nested": pa.list_(pa.int16()),
            "st": pa.struct([("x", pa.decimal128(20, 0))]),
            "i64": pa.int64(),
        }
        assert frame.native["u64"][0] == decimal.Decimal(2**64 - 1)
        assert frame.native["u8"][0] == 255


NY = "America/New_York"
# 02:30 on 2024-03-10 does not exist in New York (clocks go 02:00 -> 03:00);
# 01:30 on 2024-11-03 happens twice (EDT, then EST).
DST_CSV = b"t\n2024-03-10 02:30:00\n2024-11-03 01:30:00\n2024-06-01 12:00:00\n"
# Java's ZonedDateTime.of: a gap moves later by its length, a fold takes the earlier.
DST_UTC = ["2024-03-10 07:30:00+00:00", "2024-11-03 05:30:00+00:00", "2024-06-01 16:00:00+00:00"]


def _utc_text(series) -> list:
    return [str(v.tz_convert("UTC")) for v in series]


class TestDaylightSavingGapsAndFolds:
    """F-063: a wall clock time in a gap or a fold is read the Java way, not refused."""

    def test_inferred_timestamps(self, tmp_path):
        frame = _read_bytes(
            tmp_path,
            "csv",
            DST_CSV,
            options={"header": "true", "inferSchema": "true"},
            backend_kw={"timezone": NY},
        )
        assert _utc_text(frame.native["t"]) == DST_UTC

    def test_an_explicit_schema(self, tmp_path):
        frame = _read_bytes(
            tmp_path,
            "csv",
            DST_CSV,
            options={"header": "true"},
            schema="t TIMESTAMP",
            backend_kw={"timezone": NY},
        )
        assert _utc_text(frame.native["t"]) == DST_UTC

    def test_a_naive_timestamp_a_transform_writes(self, tmp_path):
        import datetime as dt

        import pyarrow.parquet as pq

        from ubunye.core.write_modes import ResolvedWriteMode

        naive = [dt.datetime(2024, 3, 10, 2, 30), dt.datetime(2024, 11, 3, 1, 30)]
        out = str(tmp_path / "out")
        PandasBackend(timezone=NY).execute_write(
            pd.DataFrame({"t": pd.to_datetime(naive)}),
            ResolvedWriteMode(mode="overwrite", save_mode="overwrite"),
            connector="s3",
            file_format="parquet",
            path=out,
        )
        written = pq.read_table(out).column("t").to_pylist()
        assert [str(v) for v in written] == DST_UTC[:2]

    def test_times_outside_daylight_saving_are_unchanged(self):
        import datetime as dt

        col = pa.array([dt.datetime(2024, 6, 1, 12), None], pa.timestamp("us"))
        out = pandas_io.assume_zone(col, NY).cast(pa.timestamp("us", tz="UTC"))
        assert [str(v) for v in out.to_pylist()] == ["2024-06-01 16:00:00+00:00", "None"]


class TestCsvBytesNotInTheEncoding:
    """F-061: Spark reads a byte that is not valid in the encoding as U+FFFD."""

    def test_cp1252_bytes_read_as_utf8_become_replacement_characters(self, tmp_path):
        data = "name,price\nCafé,€5\n".encode("cp1252")
        frame = _read_bytes(tmp_path, "csv", data, options={"header": "true"})
        assert frame.native.iloc[0].tolist() == ["Caf�", "�5"]

    def test_bad_bytes_in_the_header_and_a_multiline_file(self, tmp_path):
        data = b'n\xe9me,v\n"a\nb\xff",1\n'
        frame = _read_bytes(tmp_path, "csv", data, options={"header": "true", "multiLine": "true"})
        assert list(frame.native.columns) == ["n�me", "v"]
        assert frame.native.iloc[0].tolist() == ["a\nb�", "1"]

    def test_the_declared_encoding_still_decides(self, tmp_path):
        data = "name,price\nCafé,€5\n".encode("cp1252")
        frame = _read_bytes(tmp_path, "csv", data, options={"header": "true", "encoding": "cp1252"})
        assert frame.native.iloc[0].tolist() == ["Café", "€5"]
