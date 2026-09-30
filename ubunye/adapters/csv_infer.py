"""CSV column types chosen as Spark's ``inferSchema`` chooses them.

pyarrow's CSV inference and Spark's (``CSVInferSchema``, Spark 3.5 and 4) agree on
plain files and differ on numbers written a little differently. Spark tries each
value as ``int`` (``Integer.parseInt``), ``bigint`` (``Long.parseLong``), a
whole-number ``decimal`` (``new BigDecimal``, scale 0 only), ``double``
(``Double.parseDouble``, or the texts ``NaN``, ``Inf``, ``-Inf``), a date, a
timestamp, ``boolean`` (``true``/``false`` in any case), and else text; the types of
a column's values are then merged (``compatibleType``). So, unlike pyarrow:

* `` 12`` (a space after the comma) is a ``double``: ``parseInt`` refuses the
  space, ``parseDouble`` trims it. ``+5`` is an ``int``.
* ``9223372036854775808`` and longer whole numbers are ``decimal(digits, 0)``,
  read exactly, not a ``double`` that loses digits (``double`` past 38 digits).
* ``1.5d`` and ``2f`` are doubles (Java's type suffix); ``inf`` and ``Infinity ``
  with a space are not all the same: ``Infinity`` and ``Inf`` are doubles, ``inf``
  is text.
* ``TRUE``, ``True`` and ``tRuE`` are booleans.
* A column holding a date and a timestamp is a timestamp; one holding
  timestamps with and without an offset is a timestamp.

Dates and timestamps are recognised in the strict ISO forms pyarrow reads
(``2024-01-02``, ``2024-01-02 03:04:05.123``, ``2024-01-02T03:04:05+02:00``);
Spark also takes looser forms (``2024-1-2``, ``2024-01``), which stay text here.

Everything runs on Arrow arrays (RE2 regular expressions and casts), a column at
a time, with no Python loop over values.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

# Java's Double.parseDouble: blanks (<= U+0020) trimmed, a sign, NaN, Infinity,
# a decimal number with an optional exponent, or a hex number with a binary
# exponent, and an optional f/F/d/D suffix after a number.
_BLANKS = "".join(chr(c) for c in range(0x21))
_JAVA_DOUBLE = (
    r"^[\x00-\x20]*[+-]?(NaN|Infinity|"
    r"(([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?[fFdD]?)|"
    r"(0[xX]([0-9a-fA-F]+\.?|[0-9a-fA-F]*\.[0-9a-fA-F]+)[pP][+-]?[0-9]+[fFdD]?))[\x00-\x20]*$"
)
_WHOLE = r"^[+-]?[0-9]+$"
# What Arrow's float parser reads (and more): a first value like this is worth a cast.
_ARROW_NUMBER = r"[+-]?(([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?|(?i:inf|infinity|nan))"
_DATE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
# Microseconds at most: Spark holds microseconds, and a longer fraction stays text.
_TIMESTAMP = (
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}([T ][0-9]{2}(:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,6})?)?)?)?"
    r"(Z|[+-][0-9]{2}:?[0-9]{2})?$"
)
_OFFSET = r"(Z|[+-][0-9]{2}:?[0-9]{2})$"
# Spark's defaults for nanValue, positiveInf and negativeInf.
_SPECIAL = {"NaN": "NaN", "Inf": "Infinity", "-Inf": "-Infinity"}
_INT_MAX, _LONG_MAX = 2**31 - 1, 2**63 - 1

# Kinds: ("int",) ("long",) ("decimal", precision) ("double",) ("boolean",)
#        ("date",) ("timestamp",) ("string",) and None for a column of nulls.


def _all(mask: Any) -> bool:
    import pyarrow.compute as pc

    return bool(pc.all(mask).as_py()) if len(mask) else True


def _whole_kind(values: Any) -> Tuple[Any, ...]:
    """The merged kind of whole numbers (every value matches ``_WHOLE``)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    digits = pc.replace_substring_regex(values, pattern=r"^[+-]?0*", replacement="")
    widths = pc.utf8_length(digits)
    widest = pc.max(widths).as_py() or 0
    if widest <= 18:
        numbers = pc.replace_substring_regex(values, pattern=r"^\+", replacement="").cast(
            pa.int64()
        )
        low, high = pc.min_max(numbers).values()
        fits = -(2**31) <= low.as_py() and high.as_py() <= _INT_MAX
        return ("int",) if fits else ("long",)
    # 19 digits or more: the ones past a long are decimals of their digit count.
    negative = pc.starts_with(values, pattern="-")
    limit = pc.if_else(negative, "9223372036854775808", "9223372036854775807")
    past_long = pc.or_(
        pc.greater(widths, 19),
        pc.and_(pc.equal(widths, 19), pc.greater(digits, limit)),
    )
    if not pc.any(past_long).as_py():
        return ("long",)
    precision = pc.max(pc.filter(widths, past_long)).as_py()
    if precision > 38:
        return ("double",)
    # A long merged with a decimal(p, 0) is a decimal(max(p, 20), 0).
    if not _all(past_long):
        precision = max(precision, 20)
    return ("decimal", precision)


def _has_byte(values: Any, chars: bytes) -> bool:
    """Whether any value holds one of ``chars``, from the text buffer (memchr speed)."""
    import numpy as np

    data = values.buffers()[2]
    if data is None:
        return False
    raw = np.frombuffer(data, dtype=np.uint8)
    return any(bool((raw == c).any()) for c in chars)


def _fast_kind(col: Any) -> Optional[Tuple[Tuple[Any, ...], Any]]:
    """The kind and the typed column, from Arrow's own parsers where they read as Java does.

    Arrow reads a few forms Java does not (``0x1F`` as a whole number, ``inf``
    and ``nan`` as doubles); those columns go to :func:`_slow_kind`, as do the
    forms only Java reads (`` 12``, ``1.5d``). None when this cannot tell.
    """
    import re

    import pyarrow as pa
    import pyarrow.compute as pc

    # The first value says which parsers can succeed; a failing cast is costly.
    first = pc.drop_null(col.slice(0, 64))
    first = str(first[0]) if len(first) else ""
    if re.fullmatch("-?[0-9]+", first):
        whole = _cast_or_none(col, pa.int64())
    elif re.fullmatch(_ARROW_NUMBER, first):
        whole = None
    else:
        return _fast_other(col, first)
    if whole is not None:
        if _has_byte(col, b"xX"):
            return None
        low, high = (v.as_py() for v in pc.min_max(whole).values())
        if -(2**31) <= low and high <= _INT_MAX:
            return ("int",), whole.cast(pa.int32())
        return ("long",), whole
    doubles = _cast_or_none(col, pa.float64())
    if doubles is not None:
        odd = pc.invert(pc.is_finite(doubles))
        if pc.any(odd).as_py() and not _all(_java_number(pc.filter(col, odd))):
            return ("string",), col  # inf, nan, INF: text to Spark
        if not _has_byte(col, b".eE"):
            return None  # whole numbers only (a sign, or past 64 bits): Spark's integer rules
        return ("double",), doubles
    return None


def _fast_other(col: Any, first: str) -> Optional[Tuple[Tuple[Any, ...], Any]]:
    """Booleans, dates and timestamps, from Arrow's parsers (see :func:`_fast_kind`)."""
    import re

    import pyarrow as pa
    import pyarrow.compute as pc

    values = pc.drop_null(col)
    if first.lower() in ("true", "false") and _all(
        pc.is_in(pc.utf8_lower(values), value_set=pa.array(["true", "false"]))
    ):
        return ("boolean",), pc.utf8_lower(col).cast(pa.bool_())
    if re.fullmatch(_DATE[1:-1], first):
        days = _cast_or_none(col, pa.date32())
        if days is not None:
            return ("date",), days
    if re.fullmatch(_TIMESTAMP[1:-1], first):
        if _cast_or_none(col, pa.timestamp("us")) is not None:
            return ("timestamp",), None
        if _cast_or_none(col, pa.timestamp("us", tz="UTC")) is not None:
            return ("timestamp",), None
    return None


def _cast_or_none(values: Any, target: Any) -> Any:
    """``values`` cast to ``target``, or None if any value cannot be.

    A failing cast reads the whole column before it fails, so the first values
    are tried alone first: a text column is ruled out at the cost of 256 values.
    """
    import pyarrow as pa

    try:
        if len(values) > 256:
            values.slice(0, 256).cast(target)
        return values.cast(target)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return None


def _java_number(values: Any) -> Any:
    import pyarrow as pa
    import pyarrow.compute as pc

    return pc.or_(
        pc.match_substring_regex(values, pattern=_JAVA_DOUBLE),
        pc.is_in(values, value_set=pa.array(list(_SPECIAL))),
    )


def _sample_all(values: Any, pattern: str) -> bool:
    """Whether the first values all match: a cheap way to rule a pattern out."""
    import pyarrow.compute as pc

    return _all(pc.match_substring_regex(values.slice(0, 256), pattern=pattern))


def _slow_kind(values: Any) -> Tuple[Any, ...]:
    """The kind by Java's rules, value by value (as Arrow arrays)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if _sample_all(values, _WHOLE) and _all(pc.match_substring_regex(values, pattern=_WHOLE)):
        return _whole_kind(values)
    head = values.slice(0, 256)
    if _all(_java_number(head)) and _all(_java_number(values)):
        # A double merged with an int, a bigint or a decimal is a double.
        return ("double",)
    if _sample_all(values, _DATE) and _all(pc.match_substring_regex(values, pattern=_DATE)):
        return ("date",) if _casts(values, pa.date32()) else ("string",)
    if _sample_all(values, _TIMESTAMP) and _all(
        pc.match_substring_regex(values, pattern=_TIMESTAMP)
    ):
        return ("timestamp",) if _timestamps_cast(values) else ("string",)
    return ("string",)


def _casts(values: Any, target: Any) -> bool:
    import pyarrow as pa

    try:
        values.cast(target)
        return True
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return False


def _timestamps_cast(values: Any) -> bool:
    """Every value is a real date and time (with or without an offset)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    aware = pc.match_substring_regex(values, pattern=_OFFSET)
    naive = pc.invert(aware)
    ok = True
    if pc.any(aware).as_py():
        ok = _casts(pc.filter(values, aware), pa.timestamp("us", tz="UTC"))
    if ok and pc.any(naive).as_py():
        ok = _casts(pc.filter(values, naive), pa.timestamp("us"))
    return ok


def _java_doubles(col: Any) -> Any:
    """Text that ``_kind`` called double, as Java's parseDouble reads it."""
    import pyarrow as pa
    import pyarrow.compute as pc

    text = pc.utf8_trim(col, characters=_BLANKS)
    for special, java in _SPECIAL.items():
        text = pc.if_else(pc.equal(col, special), java, text)
    text = pc.replace_substring_regex(text, pattern=r"([0-9.])[fFdD]$", replacement=r"\1")
    hexed = pc.fill_null(pc.match_substring_regex(text, pattern="^[+-]?0[xX]"), False)
    if not pc.any(hexed).as_py():
        return text.cast(pa.float64())
    values = [
        None if t is None else (float.fromhex(t) if h else float(t))
        for t, h in zip(text.to_pylist(), hexed.to_pylist())
    ]
    return pa.array(values, pa.float64())


def convert(col: Any, kind: Optional[Tuple[Any, ...]], cast_timestamp: Any) -> Any:
    """A text column as ``kind``, each value read as Spark's UnivocityParser reads it."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if kind is None or kind == ("string",):
        return col
    tag = kind[0]
    if tag in ("int", "long", "decimal"):
        target = {"int": pa.int32(), "long": pa.int64()}.get(tag) or pa.decimal128(kind[1], 0)
        return pc.replace_substring_regex(col, pattern=r"^\+", replacement="").cast(target)
    if tag == "double":
        return _java_doubles(col)
    if tag == "boolean":
        return pc.utf8_lower(col).cast(pa.bool_())
    if tag == "date":
        return col.cast(pa.date32())
    if tag == "timestamp":
        return cast_timestamp(col)
    raise ValueError(f"unknown kind {kind}")


def infer_table(table: Any, cast_timestamp: Any) -> Any:
    """Every column of an all-text table typed as Spark's inferSchema types it.

    ``cast_timestamp`` turns a text column into instants in the session zone.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    columns: List[Any] = []
    fields: List[Any] = []
    for field, col in zip(table.schema, table.columns):
        col = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
        typed = None
        if col.null_count == len(col):
            kind = None  # no values: Spark's NullType, read as text
        else:
            fast = _fast_kind(col)
            kind, typed = fast if fast is not None else (_slow_kind(pc.drop_null(col)), None)
        if typed is None:
            typed = convert(col, kind, cast_timestamp)
        columns.append(typed)
        fields.append(pa.field(field.name, typed.type))
    return pa.table(columns, schema=pa.schema(fields))
