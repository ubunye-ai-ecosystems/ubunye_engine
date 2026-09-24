"""Split CSV text into fields exactly as Spark does, for the files pyarrow reads differently.

Spark reads CSV with the univocity parser (2.9.1). Its escape character is a
backslash, not a doubled quote, and when it meets a quote it did not expect
(the doubled quotes around a nickname in the Titanic names, or ``"abc"def``) it
keeps the text much as written, up to the next delimiter
(``unescapedQuoteHandling=STOP_AT_DELIMITER``). pyarrow unescapes those instead,
so the same file gave different rows.

Most files never meet those cases, and pyarrow reads them exactly as Spark
does, fast. :func:`needs_spark_split` finds the files that do, with regular
expressions over the text. Only those go through :func:`respell`, which splits
them here, in Python, and writes them back as plain CSV for pyarrow, so types
and nulls are read the normal way. With multiLine off each line stands alone,
so only the lines that need it are split; the rest are kept as they are.

The splitter is a port of univocity's ``CsvParser`` with the settings Spark
uses (no whitespace trimming, quotes and escapes not kept, unquoted values not
unescaped). The method names follow the Java source so each rule can be
checked against it; tests/integration/test_csv_quote_parity.py fuzzes it
against a live Spark session, on Spark 3.5 and Spark 4. It follows Spark 4
where the two differ, which is known only for multi-line files that mix line
endings (a CR CR LF line end).
"""

from __future__ import annotations

import csv
import io
import re
from typing import Iterator, List, Optional

_NUL = "\0"
_NL = "\n"

#: A parsed value: text, or None where univocity gives its null value (an
#: empty unquoted field), which Spark then reads as null.
Value = Optional[str]

# A line of nothing but blanks (Scala's trim removes chars up to the space).
_BLANK_LINE = re.compile("(?:^|[\r\n])[\x00-\x09\x0b\x0c\x0e-\x20]+(?=[\r\n]|$)")
_LINE_END = re.compile("\r\n|\r|\n")


def _plain_fields(delimiter: str, quote: str, escape: str, multiline: bool) -> "re.Pattern[str]":
    """A quoted field pyarrow reads as Spark does: nothing to unescape inside."""
    q, d = re.escape(quote), re.escape(delimiter)
    banned = q + (re.escape(escape) if escape else "") + "\r" + ("" if multiline else "\n")
    start, end = f"(?:^|(?<=\n)|(?<={d})){q}", f"{q}(?=$|\r?\n|{d})"
    if escape:
        return re.compile(f"{start}[^{banned}]*{end}")
    # With no escape character, Spark reads an empty quoted value at the end of
    # a line as one quote (univocity compares the empty escape with its empty
    # "previous character"); only before a delimiter is it read as empty.
    return re.compile(f"{start}(?:[^{banned}]+{end}|{q}(?={d}))")


def needs_spark_split(text: str, delimiter: str, quote: str, escape: str, multiline: bool) -> bool:
    """Whether pyarrow could read ``text`` differently from Spark.

    True when a quote or escape character appears anywhere other than as the
    two ends of a plain quoted field (one that holds no quote, escape or
    carriage return, and no newline unless ``multiline``), or, with multiLine
    off, when a line holds only blanks (Spark drops it, pyarrow keeps it).
    """
    if not multiline and _BLANK_LINE.search(text):
        return True
    specials = [c for c in (quote, escape) if c]
    if not any(c in text for c in specials):
        return False
    if not quote:
        return True
    rest = _plain_fields(delimiter, quote, escape, multiline).sub("", text)
    return any(c in rest for c in specials)


def respell(
    text: str, delimiter: str, quote: str, escape: str, multiline: bool, null_text: str = ""
) -> str:
    """``text`` as plain CSV that pyarrow reads exactly as Spark reads ``text``.

    The result quotes with ``quote`` (a double quote when quoting is off),
    escapes a quote by doubling it, and has no escape character. A value Spark
    reads as null is written as ``null_text``, the reader's null marker.
    """
    out = io.StringIO()
    writer = csv.writer(out, delimiter=delimiter, quotechar=quote or '"', lineterminator=_NL)

    def write(row: List[Value]) -> None:
        writer.writerow([null_text if v is None else v for v in row])

    if multiline or not quote:
        # Records may span lines, or unquoted lines may hold quotes: split it all.
        for row in split_text(text, delimiter, quote, escape, multiline):
            write(row)
        return out.getvalue()
    specials = [c for c in (quote, escape) if c]
    plain = _plain_fields(delimiter, quote, escape, False)
    for line in _LINE_END.split(text):
        if not line.strip(_BLANKS):
            continue  # Spark drops blank lines
        rest = plain.sub("", line) if any(c in line for c in specials) else ""
        if any(c in rest for c in specials):
            write(split_line(line, delimiter, quote, escape))
        else:
            out.write(line + _NL)
    return out.getvalue()


class _EOF(Exception):
    """The input ran out (univocity's EOFException)."""


class _Splitter:
    """univocity's CsvParser, single-character delimiter, with Spark's settings."""

    def __init__(
        self, text: str, delimiter: str, quote: str, escape: str, line_separator: str = ""
    ) -> None:
        self.text = text
        self.i = 0
        # The file's line ending, read as a newline (none for a single line).
        self.sep1 = line_separator[:1] or _NUL
        self.sep2 = line_separator[1:2] or _NUL
        self.record_start = 0
        self.d = delimiter
        self.q = quote or _NUL
        self.esc = escape or _NUL
        # Spark: the escape escapes itself unless it is the quote character.
        self.esc_esc = escape if escape and escape != quote else _NUL
        self.ch = _NUL
        self.prev = _NUL
        self.unescaped = False
        self.app: List[str] = []
        self.values: List[Value] = []

    # -- input and output ----------------------------------------------------

    def next_char(self) -> str:
        text, i = self.text, self.i
        if i >= len(text):
            raise _EOF
        ch = text[i]
        self.i = i + 1
        if ch == self.sep1 and (self.sep2 == _NUL or text[i + 1 : i + 2] == self.sep2):
            # The line ending, normalised to a newline (inside quotes too).
            self.i += self.sep2 != _NUL
            return _NL
        return ch

    def is_line_ending(self, i: int) -> bool:
        """Whether the raw text at ``i`` starts a line ending."""
        return self.text[i] == self.sep1 and (
            self.sep2 == _NUL or self.text[i + 1 : i + 2] == self.sep2
        )

    def append_until(self, ch: str, *stops: str) -> str:
        # The field `ch` is not updated if the input runs out here (as in Java).
        while ch not in stops:
            self.app.append(ch)
            ch = self.next_char()
        return ch

    def value_parsed(self) -> None:
        self.values.append("".join(self.app))
        self.app = []

    def empty_parsed(self) -> None:
        self.values.append(None)

    def row_parsed(self) -> Optional[List[Value]]:
        row, self.values = self.values, []
        return row or None

    # -- CsvParser -----------------------------------------------------------

    def parse_record(self) -> None:
        d, q = self.d, self.q
        while self.ch != _NL:
            if self.ch == d or self.ch == _NL:
                self.empty_parsed()
            else:
                self.unescaped = False
                self.prev = _NUL
                if self.ch == q:
                    value = self.get_quoted_string()
                    if value is not None:
                        self.values.append(value)
                        try:
                            self.ch = self.next_char()
                            if self.ch == d:
                                try:
                                    self.ch = self.next_char()
                                    if self.ch == _NL:
                                        self.empty_parsed()
                                except _EOF:
                                    self.empty_parsed()
                                    return
                        except _EOF:
                            return
                        continue
                    self.parse_quoted_value()
                    self.value_parsed()
                else:
                    self.ch = self.append_until(self.ch, d, _NL)
                    self.value_parsed()
            if self.ch != _NL:
                self.ch = self.next_char()
                if self.ch == _NL:
                    self.empty_parsed()

    def get_quoted_string(self) -> Optional[str]:
        """The fast path for a plain quoted value closed before a delimiter.

        None when the value needs the full parse. It matters: with no escape
        character set, ``"",x`` is read here as an empty value, where the full
        parse would not. It looks at the raw text, so a quote before a CRLF line
        ending is not seen as closing (the CR is not the newline).
        """
        text, start, n = self.text, self.i, len(self.text)
        q, esc = self.q, self.esc
        i = start
        while True:
            if i >= n:
                return None
            ch = text[i]
            if ch == q:
                if text[i - 1] == esc:
                    return None
                if i + 1 < n and text[i + 1] in (self.d, _NL):
                    break
                return None
            if ch == esc:
                if i + 1 < n and text[i + 1] in (q, self.esc_esc):
                    return None
            elif self.is_line_ending(i):
                return None
            i += 1
        self.i = i + 1
        return text[start:i]

    def handle_unescaped_quote_in_value(self) -> None:
        self.app.append(self.q)
        self.prev = self.ch
        self.parse_value_processing_escape()

    def handle_unescaped_quote(self) -> None:
        self.unescaped = True
        self.app.append(self.q)
        self.app.append(self.ch)
        self.prev = self.ch
        self.parse_quoted_value()

    def process_quote_escape(self) -> None:
        ch, prev, q, esc, esc_esc = self.ch, self.prev, self.q, self.esc, self.esc_esc
        if ch == esc and prev == esc_esc and esc_esc != _NUL:
            self.app.append(esc)
            self.ch = _NUL
        elif prev == esc:
            if ch == q:
                self.app.append(q)
                self.ch = _NUL
            else:
                self.app.append(prev)
        elif ch == q and prev == q:
            self.app.append(q)
        elif prev == q:
            self.handle_unescaped_quote_in_value()

    def parse_value_processing_escape(self) -> None:
        while self.ch != self.d and self.ch != _NL:
            if self.ch != self.q and self.ch != self.esc:
                if self.prev == self.q:
                    self.handle_unescaped_quote_in_value()
                    return
                self.app.append(self.ch)
            else:
                self.process_quote_escape()
            self.prev = self.ch
            self.ch = self.next_char()

    def parse_quoted_value(self) -> None:
        d, q, esc = self.d, self.q, self.esc
        if self.prev != _NUL:
            # STOP_AT_DELIMITER: the rest of the field as written.
            self.app.insert(0, q)
            self.ch = self.next_char()
            self.ch = self.append_until(self.ch, d, _NL)
            return
        self.ch = self.next_char()
        while True:
            if self.prev == q and (_blank(self.ch) or self.ch == d or self.ch == _NL):
                break
            if self.ch != q and self.ch != esc:
                if self.prev == q:
                    self.handle_unescaped_quote()
                    return
                if self.prev == esc and esc != _NUL:
                    self.app.append(esc)
                self.ch = self.append_until(self.ch, q, esc, self.esc_esc)
                self.prev = self.ch
                self.ch = self.next_char()
            else:
                self.process_quote_escape()
                self.prev = self.ch
                self.ch = self.next_char()
                if self.unescaped and (self.ch == d or self.ch == _NL):
                    return
        # Blanks after the closing quote are dropped; anything else after them
        # means the quote did not close the value, and parsing carries on.
        if self.ch != d and self.ch != _NL and _blank(self.ch):
            blanks = []
            while True:
                blanks.append(self.ch)
                self.ch = self.next_char()
                if self.ch == _NL:
                    return
                if not (_blank(self.ch) and self.ch != d):
                    break
            if self.ch != d:
                self.app.append(q)
                self.app.extend(blanks)
                self.app.append(self.ch)
                self.prev = self.ch
                self.parse_quoted_value()
        if self.ch != d and self.ch != _NL:
            raise ValueError(
                f"Unexpected character {self.ch!r} following quoted value of CSV field"
            )

    def consume_value_on_eof(self) -> bool:
        if self.ch == self.q:
            if self.prev == self.q:
                return True
            if not self.unescaped:
                self.app.append(self.q)
        # Spark's comment character is NUL when no comment option is set.
        out = self.prev != _NUL and self.ch not in (self.d, _NL, _NUL)
        self.ch = self.prev = _NUL
        return out

    def handle_eof(self) -> Optional[List[Value]]:
        consume = self.consume_value_on_eof()
        parsed_something = self.i > self.record_start
        if self.values or consume:
            if self.app or consume:
                self.value_parsed()
            elif parsed_something:
                self.empty_parsed()
            return self.row_parsed()
        if self.app or parsed_something:
            if not self.app:
                self.empty_parsed()
            else:
                self.value_parsed()
            return self.row_parsed()
        return None

    # -- driving -------------------------------------------------------------

    def parse_line(self) -> Optional[List[Value]]:
        """One line on its own, as Spark parses each line when multiLine is off."""
        try:
            self.ch = self.next_char()
            self.parse_record()
            return self.row_parsed()
        except _EOF:
            return self.handle_eof()

    def parse_all(self) -> Iterator[List[Value]]:
        """Every record of a whole file, as Spark parses it when multiLine is on."""
        while True:
            self.record_start = self.i
            try:
                self.ch = self.next_char()
                self.parse_record()
                row = self.row_parsed()
            except _EOF:
                row = self.handle_eof()
                if row:
                    yield row
                return
            if row:
                yield row


def _blank(ch: str) -> bool:
    return _NUL < ch <= " "


def split_line(
    line: str, delimiter: str = ",", quote: str = '"', escape: str = "\\"
) -> List[Value]:
    """One line split into Spark's values (multiLine off)."""
    return _Splitter(line, delimiter, quote, escape).parse_line() or []


def line_separator(text: str) -> str:
    """The line ending univocity detects: the first one in the text (LF, CRLF or CR)."""
    match = re.search("[\r\n]", text)
    if match is None:
        return _NL
    if match.group() == _NL:
        return _NL
    return "\r\n" if text[match.end() : match.end() + 1] == _NL else "\r"


def split_text(
    text: str, delimiter: str = ",", quote: str = '"', escape: str = "\\", multiline: bool = False
) -> List[List[Value]]:
    """Every record of a file's text, split as Spark splits it."""
    if multiline:
        splitter = _Splitter(text, delimiter, quote, escape, line_separator(text))
        return list(splitter.parse_all())
    rows = []
    # Hadoop's line reader ends a line at LF, CR or CRLF.
    for line in _LINE_END.split(text):
        if line.strip(_BLANKS):  # Spark drops blank lines (Scala's trim)
            rows.append(split_line(line, delimiter, quote, escape))
    return rows


_BLANKS = "".join(chr(c) for c in range(33))
