"""The pandas backend reads a path the way Spark reads it.

The promise is "the same folder runs on Spark or on pandas". A backend that reads
the same CSV into different columns or types breaks that promise quietly, so every
default Spark applies on read is pinned here. The expected values come from
Spark 4.2 itself (the integration tier checks the two engines against each other).

No Spark, no JVM: these run wherever pandas and pyarrow are installed.
"""

from __future__ import annotations

import json

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.errors import SourceReadError  # noqa: E402

CSV = (
    "small,big,dbl,flag,txt,empty,day,ts,mixed\n"
    "1,3000000000,1.5,true,a,,2024-01-02,2024-01-02 03:04:05,1\n"
    "2,4000000000,2.0,False,b,,2024-02-03,2024-02-03T01:02:03,x\n"
    ",5,,,,,,,\n"
)


def _arrow_types(frame) -> dict:
    """Column name -> the Arrow type behind it (the frame is Arrow-backed)."""
    native = frame.native
    return {c: native[c].dtype.pyarrow_dtype for c in native.columns}


def _read(tmp_path, fmt, text, name="in", **kw):
    src = tmp_path / f"{name}.{fmt}"
    src.write_text(text, encoding="utf-8")
    return PandasBackend(**kw.pop("backend_kw", {})).read_frame(fmt, str(src), **kw)


class TestCsvDefaults:
    def test_no_header_by_default_and_spark_column_names(self, tmp_path):
        frame = _read(tmp_path, "csv", "a,b\n1,2\n")
        assert list(frame.native.columns) == ["_c0", "_c1"]
        assert frame.count() == 2  # the header line is data, as in Spark

    def test_everything_is_text_without_infer_schema(self, tmp_path):
        frame = _read(tmp_path, "csv", CSV, options={"header": "true"})
        assert set(_arrow_types(frame).values()) == {pa.string()}

    def test_empty_field_is_null_not_empty_string(self, tmp_path):
        frame = _read(tmp_path, "csv", CSV, options={"header": "true"})
        assert frame.native["empty"].isna().all()
        assert frame.native["small"].isna().tolist() == [False, False, True]

    def test_infer_schema_matches_spark(self, tmp_path):
        frame = _read(tmp_path, "csv", CSV, options={"header": "true", "inferSchema": "true"})
        types = _arrow_types(frame)
        assert types["small"] == pa.int32()  # Spark: int when every value fits
        assert types["big"] == pa.int64()  # Spark: bigint
        assert types["dbl"] == pa.float64()
        assert types["flag"] == pa.bool_()  # "False" too: Spark is case blind
        assert types["txt"] == pa.string()
        assert types["empty"] == pa.string()  # an all-null column is text in Spark
        assert types["day"] == pa.date32()
        assert types["ts"] == pa.timestamp("us", tz="UTC")  # an instant, like Spark
        assert types["mixed"] == pa.string()

    def test_infer_schema_values(self, tmp_path):
        frame = _read(tmp_path, "csv", CSV, options={"header": "true", "inferSchema": "true"})
        rows = frame.collect()
        assert rows[0]["small"] == 1 and rows[0]["big"] == 3000000000
        assert rows[1]["flag"] is False or rows[1]["flag"] == False  # noqa: E712
        assert pd.isna(rows[2]["small"]) and rows[2]["big"] == 5

    def test_timestamp_text_is_read_in_the_backend_timezone(self, tmp_path):
        frame = _read(
            tmp_path,
            "csv",
            "ts\n2024-01-02 03:04:05\n",
            options={"header": "true", "inferSchema": "true"},
            backend_kw={"timezone": "Africa/Johannesburg"},
        )
        value = frame.native["ts"].iloc[0]
        assert value == pd.Timestamp("2024-01-02 01:04:05", tz="UTC")

    def test_timestamps_default_to_utc(self, tmp_path):
        frame = _read(
            tmp_path,
            "csv",
            "ts\n2024-01-02 03:04:05\n",
            options={"header": "true", "inferSchema": "true"},
        )
        assert frame.native["ts"].iloc[0] == pd.Timestamp("2024-01-02 03:04:05", tz="UTC")

    def test_separator_and_null_value(self, tmp_path):
        frame = _read(
            tmp_path,
            "csv",
            "a;b\n1;NA\n",
            options={"header": "true", "sep": ";", "nullValue": "NA"},
        )
        assert frame.native["a"].iloc[0] == "1"
        assert frame.native["b"].isna().all()

    def test_option_names_are_case_blind_like_spark(self, tmp_path):
        frame = _read(tmp_path, "csv", "a\n1\n", options={"HEADER": "TRUE", "inferschema": "True"})
        assert _arrow_types(frame)["a"] == pa.int32()

    def test_explicit_schema_names_columns_and_skips_the_header(self, tmp_path):
        frame = _read(
            tmp_path,
            "csv",
            "x,y\n1,a\n",
            options={"header": "true"},
            schema="id INT, name STRING",
        )
        assert list(frame.native.columns) == ["id", "name"]
        assert _arrow_types(frame) == {"id": pa.int32(), "name": pa.string()}
        assert frame.collect() == [{"id": 1, "name": "a"}]

    def test_schema_timestamps_without_an_offset_are_session_time(self, tmp_path):
        frame = _read(
            tmp_path,
            "csv",
            "2024-01-02 03:04:05\n2024-01-02T03:04:05+00:00\n",
            schema="ts TIMESTAMP",
            backend_kw={"timezone": "Africa/Johannesburg"},
        )
        assert frame.native["ts"].tolist() == [
            pd.Timestamp("2024-01-02 01:04:05", tz="UTC"),
            pd.Timestamp("2024-01-02 03:04:05", tz="UTC"),
        ]

    def test_schema_booleans_are_case_blind(self, tmp_path):
        frame = _read(tmp_path, "csv", "True\nFALSE\n", schema="b BOOLEAN")
        assert frame.native["b"].tolist() == [True, False]


class TestJsonDefaults:
    LINES = (
        '{"z":1,"a":"x","f":1.5,"b":true,"n":null,"arr":[1,2],'
        '"obj":{"k":1,"c":"q"},"ts":"2024-01-02T03:04:05"}\n'
        '{"z":3000000000,"a":"y"}\n'
    )

    def test_json_lines_is_the_default(self, tmp_path):
        assert _read(tmp_path, "json", self.LINES).count() == 2

    def test_columns_are_sorted_by_name_like_spark(self, tmp_path):
        frame = _read(tmp_path, "json", self.LINES)
        assert list(frame.native.columns) == [
            "a",
            "arr",
            "b",
            "f",
            "n",
            "obj",
            "ts",
            "z",
        ]

    def test_types_match_spark(self, tmp_path):
        types = _arrow_types(_read(tmp_path, "json", self.LINES))
        assert types["z"] == pa.int64()  # Spark: bigint for every JSON integer
        assert types["f"] == pa.float64()
        assert types["b"] == pa.bool_()
        assert types["n"] == pa.string()  # all null: text
        assert types["ts"] == pa.string()  # Spark does not infer timestamps in JSON
        assert types["arr"] == pa.list_(pa.int64())
        # Nested fields are sorted too.
        assert [f.name for f in types["obj"]] == ["c", "k"]

    def test_a_missing_key_is_null(self, tmp_path):
        rows = _read(tmp_path, "json", self.LINES).collect()
        assert rows[1]["a"] == "y" and pd.isna(rows[1]["f"])

    def test_timestamp_text_is_kept_verbatim(self, tmp_path):
        rows = _read(tmp_path, "json", self.LINES).collect()
        assert rows[0]["ts"] == "2024-01-02T03:04:05"

    def test_multiline_reads_a_json_array(self, tmp_path):
        text = json.dumps([{"b": 1}, {"a": 2}], indent=2)
        frame = _read(tmp_path, "json", text, options={"multiLine": "true"})
        assert list(frame.native.columns) == ["a", "b"]
        assert frame.count() == 2

    def test_explicit_schema_selects_and_casts(self, tmp_path):
        frame = _read(tmp_path, "json", '{"a":1,"b":"x"}\n', schema="a DOUBLE")
        assert list(frame.native.columns) == ["a"]
        assert _arrow_types(frame)["a"] == pa.float64()


class TestPaths:
    def test_reads_a_spark_style_folder_and_skips_markers(self, tmp_path):
        folder = tmp_path / "out"
        folder.mkdir()
        (folder / "part-00000-a-c000.csv").write_text("1\n", encoding="utf-8")
        (folder / "part-00001-b-c000.csv").write_text("2\n", encoding="utf-8")
        (folder / "_SUCCESS").write_text("", encoding="utf-8")
        (folder / ".part-00000-a-c000.csv.crc").write_text("junk", encoding="utf-8")
        frame = PandasBackend().read_frame("csv", str(folder))
        assert sorted(frame.native["_c0"]) == ["1", "2"]

    def test_reads_a_glob(self, tmp_path):
        for i in range(3):
            (tmp_path / f"f{i}.csv").write_text(f"v\n{i}\n", encoding="utf-8")
        frame = PandasBackend().read_frame(
            "csv", str(tmp_path / "f*.csv"), options={"header": "true"}
        )
        assert frame.count() == 3

    def test_file_uri(self, tmp_path):
        src = tmp_path / "in.csv"
        src.write_text("a\n1\n", encoding="utf-8")
        assert PandasBackend().read_frame("csv", src.as_uri()).count() == 2

    def test_missing_path_says_so(self, tmp_path):
        with pytest.raises(SourceReadError, match="does not exist"):
            PandasBackend().read_frame("csv", str(tmp_path / "nope.csv"))

    def test_remote_paths_are_refused_clearly(self):
        with pytest.raises(SourceReadError, match="local paths"):
            PandasBackend().read_frame("parquet", "s3a://bucket/key")

    def test_parquet_roundtrip_keeps_types(self, tmp_path):
        table = pa.table({"i": pa.array([1, None], pa.int32()), "s": ["a", None]})
        import pyarrow.parquet as pq

        pq.write_table(table, tmp_path / "t.parquet")
        frame = PandasBackend().read_frame("parquet", str(tmp_path / "t.parquet"))
        assert _arrow_types(frame) == {"i": pa.int32(), "s": pa.string()}


class TestRefusals:
    def test_unknown_option_is_refused_not_ignored(self, tmp_path):
        with pytest.raises(SourceReadError, match="dateFormat"):
            _read(tmp_path, "csv", "a\n1\n", options={"dateFormat": "dd/MM/yyyy"})

    def test_unsupported_schema_type_is_refused(self, tmp_path):
        with pytest.raises(SourceReadError, match="MAP<STRING,INT>"):
            _read(tmp_path, "json", '{"a":1}\n', schema="a MAP<STRING,INT>")

    def test_unsupported_format(self, tmp_path):
        with pytest.raises(SourceReadError, match="cannot read file_format 'delta'"):
            PandasBackend().read_frame("delta", str(tmp_path))


class TestParseMode:
    """Spark's `mode` option for csv and json: what to do with a malformed record."""

    BAD_CSV = "a,b\n1,2\n3,4,5\n6,7\n"  # the second data row has one field too many

    def test_failfast_stops_at_a_malformed_row(self, tmp_path):
        with pytest.raises(SourceReadError):
            _read(tmp_path, "csv", self.BAD_CSV, options={"header": "true", "mode": "FAILFAST"})

    def test_dropmalformed_skips_it_like_spark(self, tmp_path):
        frame = _read(
            tmp_path, "csv", self.BAD_CSV, options={"header": "true", "mode": "DROPMALFORMED"}
        )
        assert frame.native["a"].tolist() == ["1", "6"]

    def test_permissive_evens_rows_like_spark(self, tmp_path):
        # Spark 4.2 on this file: the long row is cut, the short one padded with null.
        frame = _read(tmp_path, "csv", self.BAD_CSV + "7\n", options={"header": "true"})
        rows = [
            tuple(None if pd.isna(v) else v for v in r)
            for r in frame.native.itertuples(index=False)
        ]
        assert rows == [("1", "2"), ("3", "4"), ("6", "7"), ("7", None)]

    def test_permissive_with_infer_schema(self, tmp_path):
        frame = _read(
            tmp_path, "csv", "a,b\n1,2\n3\n", options={"header": "true", "inferSchema": "true"}
        )
        assert _arrow_types(frame) == {"a": pa.int32(), "b": pa.int32()}
        assert frame.native["b"].isna().tolist() == [False, True]

    def test_permissive_pads_with_the_null_marker(self, tmp_path):
        frame = _read(
            tmp_path, "csv", "a;b\n1\n", options={"header": "true", "sep": ";", "nullValue": "NA"}
        )
        assert frame.native["b"].isna().tolist() == [True]

    def test_permissive_is_accepted_and_reads_clean_data(self, tmp_path):
        frame = _read(tmp_path, "csv", "a\n1\n", options={"header": "true", "mode": "permissive"})
        assert frame.count() == 1

    def test_json_dropmalformed(self, tmp_path):
        frame = _read(
            tmp_path, "json", '{"a":1}\nnot json\n{"a":2}\n', options={"mode": "DROPMALFORMED"}
        )
        assert frame.native["a"].tolist() == [1, 2]

    def test_json_failfast(self, tmp_path):
        with pytest.raises(SourceReadError):
            _read(tmp_path, "json", '{"a":1}\nnot json\n', options={"mode": "FAILFAST"})

    def test_an_unknown_mode_is_refused(self, tmp_path):
        with pytest.raises(SourceReadError, match="SOMETIMES"):
            _read(tmp_path, "csv", "a\n1\n", options={"mode": "SOMETIMES"})


class TestQuotesLikeSpark:
    """Quotes Spark does not unescape.

    Spark's escape character is a backslash, so a doubled quote inside a quoted
    field is not an escape: Spark keeps the field much as written. The Titanic
    data has 53 such names, and the pandas backend read every one differently.
    Each expected value below is what Spark 4.2 read from the same line.
    """

    SPARK = [
        ('"McGowan, Miss. Anna ""Annie"""', '"McGowan, Miss. Anna ""Annie"""'),
        ('"Petroff, Mr. Pastcho (""Pentcho"")"', '"Petroff, Mr. Pastcho (""Pentcho"")"'),
        ('"a""b"', '"a""b"'),
        ('""""', '""'),
        ('"x""', 'x"'),
        ('"abc"def', '"abc"def'),
        ('"abc" def', '"abc" def'),
        ('"x" ', "x"),
        (' "x"', ' "x"'),
        ('abc"def', 'abc"def'),
        ('"a\\"b"', 'a"b'),
        ("a\\b", "a\\b"),
        ('"a\\\\"', "a\\"),
        ('""', None),
    ]

    @pytest.mark.parametrize("field,spark", SPARK, ids=[c for c, _ in SPARK])
    def test_one_field_reads_as_spark_reads_it(self, tmp_path, field, spark):
        frame = _read(tmp_path, "csv", f"name,end\n{field},END\n", options={"header": "true"})
        got = frame.native["name"].iloc[0]
        assert (None if pd.isna(got) else got) == spark
        assert frame.native["end"].tolist() == ["END"]

    def test_an_unclosed_quote_ends_with_its_line(self, tmp_path):
        # Spark: a line holding one quote is a value holding one quote, and the
        # next line is a record of its own (multiLine is off).
        frame = _read(tmp_path, "csv", 'name\n"\nnext\n', options={"header": "true"})
        assert frame.native["name"].tolist() == ['"', "next"]

    def test_an_unexpected_quote_keeps_the_text_to_the_next_delimiter(self, tmp_path):
        # Spark: '"a,b""c,d"' is the fields '"a,b""c' and 'd"'.
        frame = _read(tmp_path, "csv", 'x,y\n"a,b""c,d"\n', options={"header": "true"})
        assert frame.native.iloc[0].tolist() == ['"a,b""c', 'd"']

    def test_types_are_still_inferred(self, tmp_path):
        text = 'id,name,age\n1,"Anna ""Annie""",22\n2,"Bo",\n'
        frame = _read(tmp_path, "csv", text, options={"header": "true", "inferSchema": "true"})
        assert _arrow_types(frame) == {"id": pa.int32(), "name": pa.string(), "age": pa.int32()}
        assert frame.native["name"].tolist() == ['"Anna ""Annie"""', "Bo"]
        assert frame.native["age"].isna().tolist() == [False, True]

    def test_the_null_marker_still_applies(self, tmp_path):
        text = 'a;b\n"x""y";NA\n'
        frame = _read(
            tmp_path, "csv", text, options={"header": "true", "sep": ";", "nullValue": "NA"}
        )
        assert frame.native["a"].tolist() == ['"x""y"']
        assert frame.native["b"].isna().tolist() == [True]

    def test_a_doubled_quote_is_an_escape_when_the_escape_is_the_quote(self, tmp_path):
        text = 'name\n"Anna ""Annie"""\n'
        frame = _read(tmp_path, "csv", text, options={"header": "true", "escape": '"'})
        assert frame.native["name"].tolist() == ['Anna "Annie"']

    def test_a_line_of_blanks_is_dropped_like_spark(self, tmp_path):
        # Spark drops a line that trims to nothing; pyarrow read it as a row.
        frame = _read(tmp_path, "csv", "a,b\n1,2\n  \t \n3,4\n", options={"header": "true"})
        assert frame.native["a"].tolist() == ["1", "3"]

    def test_a_gzipped_file_with_such_quotes(self, tmp_path):
        import gzip

        src = tmp_path / "in.csv.gz"
        src.write_bytes(gzip.compress(b'name\n"a""b"\n'))
        frame = PandasBackend().read_frame("csv", str(src), options={"header": "true"})
        assert frame.native["name"].tolist() == ['"a""b"']
