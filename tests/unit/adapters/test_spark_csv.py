"""ubunye.adapters.spark_csv: Spark's CSV splitting, without Spark.

Every expected value here is what Spark 4.2 read from the same text (the
integration tier fuzzes the port against a live session).
"""

from __future__ import annotations

import pytest

from ubunye.adapters.spark_csv import (
    line_separator,
    needs_spark_split,
    respell,
    split_line,
    split_text,
)


class TestWhichFilesNeedIt:
    @pytest.mark.parametrize(
        "text",
        [
            "a,b\n1,2\n",
            'a,"b, c"\n"x",y\n',
            'a,""\n',
            '"x"\r\n"y"\r\n',
            "a,C:\\data\\x\n",  # a backslash that escapes nothing: both keep it
        ],
    )
    def test_plain_files_are_left_to_pyarrow(self, text):
        assert not needs_spark_split(text, ",", '"', "\\", False)

    @pytest.mark.parametrize(
        "text",
        [
            'a,"b ""c"""\n',  # a doubled quote
            'a,"b\\"c"\n',  # an escaped quote
            "a,b\\\\c\n",  # an escaped escape
            'a,"b\\c"\n',  # a backslash inside quotes
            'a,"b"c\n',  # text after the closing quote
            'a,b"c\n',  # a quote in mid value
            "a\n   \nb\n",  # a line of blanks: Spark drops it
            "a\rb\n",  # a lone carriage return
        ],
    )
    def test_files_pyarrow_would_read_differently(self, text):
        assert needs_spark_split(text, ",", '"', "\\", False)

    def test_a_blank_line_is_kept_in_multiline_mode(self):
        assert not needs_spark_split("a\n   \nb\n", ",", '"', "\\", True)

    def test_an_empty_quoted_value_at_a_line_end_with_no_escape(self):
        # Spark reads it as one quote here, and as empty before a delimiter.
        assert needs_spark_split('a,""\n', ",", '"', "", False)
        assert not needs_spark_split('"",a\n', ",", '"', "", False)
        assert split_line('a,""', escape="") == ["a", '"']
        assert split_line('"",a', escape="") == ["", "a"]


class TestSplitting:
    @pytest.mark.parametrize(
        "line,fields",
        [
            ('"McGowan, Miss. Anna ""Annie"""', ['"McGowan, Miss. Anna ""Annie"""']),
            ('"a,b""c,d"', ['"a,b""c', 'd"']),
            ('"x" ,y', ["x", "y"]),
            ('"abc" def,y', ['"abc" def', "y"]),
            ('"a\\"b",c', ['a"b', "c"]),
            ("a,,b,", ["a", None, "b", None]),
            ('"', ['"']),
        ],
    )
    def test_lines_split_as_spark_splits_them(self, line, fields):
        assert split_line(line) == fields

    def test_blank_lines_are_dropped_and_line_endings_are_any_of_three(self):
        assert split_text("a\r\n \t\rb\nc") == [["a"], ["b"], ["c"]]

    def test_multiline_keeps_a_newline_inside_quotes(self):
        assert split_text('a,"b\nc"\nd\n', multiline=True) == [["a", "b\nc"], ["d"]]

    @pytest.mark.parametrize(
        "text,sep", [("a\nb", "\n"), ("a\r\nb", "\r\n"), ("a\rb", "\r"), ("ab", "\n")]
    )
    def test_the_line_ending_is_the_first_one_found(self, text, sep):
        assert line_separator(text) == sep


class TestRespell:
    def test_plain_lines_pass_through_and_odd_ones_are_rewritten(self):
        text = 'id,name\n1,"Braund, Mr. Owen"\n2,"Anna ""Annie"""\n'
        assert respell(text, ",", '"', "\\", False) == (
            'id,name\n1,"Braund, Mr. Owen"\n2,"""Anna """"Annie"""""""\n'
        )

    def test_a_null_is_written_as_the_null_marker(self):
        assert (
            respell('a,,"b""c"\n', ",", '"', "\\", False, null_text="NA") == 'a,NA,"""b""""c"""\n'
        )


class TestTheBackendAgreesWithThePort:
    """Whatever path a file takes, the pandas backend reads what the port splits.

    The port (split_text) is fuzzed against live Spark in the integration tier;
    this holds the pandas backend to it for random files of quotes, escapes,
    delimiters, blanks and line breaks, so the quick check that sends a plain
    file straight to pyarrow is tested here, with no Spark.
    """

    WIDTH = 6
    #: Characters that make trouble, and whole fields as real files have them.
    CHARS = ["a", "b", ",", '"', '"', "\\", " ", "\t", '"x"', '""', "ok"]
    FIELDS = ["1", "a b", '"a, b"', '"x"', "", '""', "C:\\d", '"a""b"', '"q" ', '"q\\"', "  "]
    WEIGHTS = [30, 30, 20, 20, 10, 5, 5, 2, 2, 2, 1]

    def _line(self, rnd, kind, multiline):
        if kind == "fields":
            return ",".join(rnd.choices(self.FIELDS, self.WEIGHTS, k=rnd.randint(1, self.WIDTH)))
        chars = self.CHARS + (["\n"] if multiline else [])
        return "".join(rnd.choices(chars, k=rnd.randint(1, 8)))

    @pytest.mark.parametrize("kind", ["chars", "fields"])
    @pytest.mark.parametrize("escape", ["\\", '"', ""])
    @pytest.mark.parametrize("ending", ["\n", "\r\n"])
    @pytest.mark.parametrize("multiline", [False, True])
    def test_random_files(self, tmp_path, kind, escape, ending, multiline):
        import random

        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        from ubunye.backends.pandas_backend import PandasBackend

        rnd = random.Random(f"{kind}{escape!r}{ending!r}{multiline}")
        schema = ", ".join(f"c{i} STRING" for i in range(self.WIDTH))
        options = {"escape": escape, "multiLine": str(multiline).lower()}
        plain = 0
        for n in range(60):
            lines = [self._line(rnd, kind, multiline) for _ in range(5)]
            text = ("\n".join(lines) + "\n").replace("\n", ending)
            plain += not needs_spark_split(text, ",", '"', escape, multiline)
            path = tmp_path / f"f{n}.csv"
            path.write_bytes(text.encode("utf-8"))
            frame = PandasBackend().read_frame("csv", str(path), options=options, schema=schema)
            got = [
                tuple(None if pd.isna(v) else v for v in row)
                for row in frame.native.itertuples(index=False)
            ]
            want = [
                tuple(v if v not in ("", None) else None for v in (r + [None] * self.WIDTH))[
                    : self.WIDTH
                ]
                for r in split_text(text, ",", '"', escape, multiline)
            ]
            assert got == want, repr(text)
        if kind == "fields":
            assert plain >= 5, "too few files took the fast path to test it"
