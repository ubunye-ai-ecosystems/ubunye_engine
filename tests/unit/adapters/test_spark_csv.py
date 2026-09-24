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
        ["a,b\n1,2\n", 'a,"b, c"\n"x",y\n', 'a,""\n', '"x"\r\n"y"\r\n'],
    )
    def test_plain_files_are_left_to_pyarrow(self, text):
        assert not needs_spark_split(text, ",", '"', "\\", False)

    @pytest.mark.parametrize(
        "text",
        [
            'a,"b ""c"""\n',  # a doubled quote
            'a,"b\\"c"\n',  # an escaped quote
            "a,b\\c\n",  # a backslash outside quotes
            'a,"b"c\n',  # text after the closing quote
            'a,b"c\n',  # a quote in mid value
            "a\n   \nb\n",  # a line of blanks: Spark drops it
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
