"""Split CSV text into fields exactly as Spark does, for the files pyarrow reads differently.

Spark reads CSV with the univocity parser (2.9.1). Its escape character is a
backslash, not a doubled quote, and when it meets a quote it did not expect
(the doubled quotes around a nickname in the Titanic names, or ``"abc"def``) it
keeps the text much as written, up to the next delimiter
(``unescapedQuoteHandling=STOP_AT_DELIMITER``). pyarrow unescapes those instead,
so the same file gave different rows.

Most files never meet those cases, and pyarrow reads them exactly as Spark
does, fast. :func:`needs_spark_split` finds the files that do, from the
positions of their quotes, escapes and line breaks (numpy, no Python loop over
the text). Only those go through :func:`respell`, which splits them here, in
Python, and writes them back as plain CSV for pyarrow, so types and nulls are
read the normal way. With multiLine off each line stands alone, so only the
lines that need it are split; the rest are kept as they are.

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
from typing import Any, Iterator, List, Optional

_NUL = "\0"
_NL = "\n"

#: A parsed value: text, or None where univocity gives its null value (an
#: empty unquoted field), which Spark then reads as null.
Value = Optional[str]

_LINE_END = re.compile("\r\n|\r|\n")


def needs_spark_split(text: str, delimiter: str, quote: str, escape: str, multiline: bool) -> bool:
    """Whether pyarrow could read ``text`` differently from Spark.

    True when a quote is anywhere but the two ends of a plain quoted field (one
    that holds no quote or carriage return, and no newline unless
    ``multiline``), when an escape character comes before a quote or another
    escape, or, with multiLine off, when a line holds only blanks (Spark drops
    it, pyarrow keeps it) or a lone carriage return ends a line.
    """
    flags = _messy(text, delimiter, quote, escape, multiline)
    return bool(flags.any())


def _messy(text: str, delimiter: str, quote: str, escape: str, multiline: bool) -> Any:
    """Which lines pyarrow could read differently from Spark (one flag per line).

    With multiLine on, one flag for the whole text. The checks run over the
    UTF-8 bytes with numpy, on the positions of the few characters that matter
    (line breaks, quotes, escapes), never byte by byte in Python: the quote,
    delimiter, escape and line breaks are ASCII, so they never occur inside
    another character's bytes. An 18 MB file of plain quoted fields is checked
    in tens of milliseconds (the regular expression this replaced took most of a
    second).
    """
    import numpy as np

    raw = text.encode("utf-8", "surrogatepass")
    b = np.frombuffer(raw, dtype=np.uint8)
    n = len(b)
    NL, CR = 10, 13
    D = ord(delimiter)
    Q = ord(quote) if quote else None
    E = ord(escape) if escape and escape != quote else None

    newlines = np.flatnonzero(b == NL)
    lines = len(newlines) + 1
    messy = np.zeros(1 if multiline else lines, dtype=bool)

    def line_of(positions: Any) -> Any:
        """The line each position is on (a newline is on the line it ends)."""
        return np.searchsorted(newlines, positions)

    def mark(positions: Any) -> None:
        if len(positions):
            if multiline:
                messy[0] = True
            else:
                messy[line_of(positions)] = True

    def after(positions: Any) -> Any:
        """The byte after each position (a newline past the end)."""
        nxt = positions + 1
        return np.where(nxt < n, b[np.minimum(nxt, n - 1)], NL)

    def before(positions: Any) -> Any:
        prv = positions - 1
        return np.where(prv >= 0, b[np.maximum(prv, 0)], NL)

    cr = np.flatnonzero(b == CR)
    if len(cr) and not multiline and bool((after(cr) != NL).any()):
        messy[:] = True  # a lone CR ends a line for both, but splits lines differently
        return messy

    if Q is None:
        # No quoting: every field is read as written, by both; only an escape
        # character could differ, and only Spark's parser knows how.
        if escape and bool((b == ord(escape)).any()):
            messy[:] = True
    else:
        qpos = np.flatnonzero(b == Q)
        esc = np.flatnonzero(b == E) if E is not None else np.empty(0, dtype=np.intp)
        if len(esc):
            # An escape before a quote or an escape: Spark unescapes, pyarrow does not.
            mark(esc[(after(esc) == Q) | (after(esc) == E)])
        if len(qpos):
            if multiline:
                rank = np.arange(len(qpos))
                if len(qpos) % 2:
                    messy[0] = True
                    return messy
            else:
                line_q = line_of(qpos)
                rank = np.arange(len(qpos)) - np.searchsorted(line_q, line_q, side="left")
                counts = np.bincount(line_q, minlength=lines)
                messy |= counts % 2 == 1
                even = counts[line_q] % 2 == 0
                qpos, rank = qpos[even], rank[even]
            opens, closes = qpos[rank % 2 == 0], qpos[rank % 2 == 1]
            bad = (before(opens) != D) & (before(opens) != NL)
            nxt = after(closes)
            bad |= (nxt != D) & (nxt != NL) & (nxt != CR)
            # Nothing inside a quoted value that either parser treats specially:
            # count those characters between each pair by their sorted positions.
            special = [cr, esc] if multiline else [cr, esc, newlines]
            specials = np.sort(np.concatenate(special))
            between = np.searchsorted(specials, closes) - np.searchsorted(specials, opens + 1)
            bad |= between > 0
            if not escape:
                # Spark reads an empty quoted value at a line end as one quote.
                bad |= (closes == opens + 1) & (nxt != D)
            mark(opens[bad])

    if not multiline:
        # A line of blanks: Spark drops it (Scala's trim), pyarrow keeps it.
        # Only a line that starts with a blank can be one, so only those are read.
        starts = np.concatenate(([0], newlines + 1))
        starts = starts[starts < n]
        first = b[starts]
        for line in np.flatnonzero((first <= 0x20) & (first != NL) & (first != CR)).tolist():
            end = int(newlines[line]) if line < len(newlines) else n
            content = raw[int(starts[line]) : end].rstrip(b"\r")
            if content and not content.translate(None, _BLANK_BYTES):
                messy[line] = True
    return messy


_BLANK_BYTES = bytes(range(0x21))


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

    flags = None if multiline or not quote else _messy(text, delimiter, quote, escape, False)
    if flags is None or bool(flags.all()):
        # Records may span lines, unquoted lines may hold quotes, or a lone CR
        # ends lines: split it all.
        for row in split_text(text, delimiter, quote, escape, multiline):
            write(row)
        return out.getvalue()
    # Line by line: each plain line is kept as written, and only the lines
    # that need it are split (the flags count lines the same way, at "\n").
    for line, needs in zip(text.split(_NL), flags.tolist()):
        line = line[:-1] if line.endswith("\r") else line
        if not needs:
            if line:
                out.write(line + _NL)
        elif line.strip(_BLANKS):  # Spark drops blank lines
            write(split_line(line, delimiter, quote, escape))
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
