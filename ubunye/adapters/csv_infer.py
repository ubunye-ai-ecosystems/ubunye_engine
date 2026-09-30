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


# A number as java.math.BigDecimal reads it: sign, digits, a point, an exponent.
_BIGDEC = (
    r"^(?P<sign>[+-]?)(?P<whole>[0-9]*)(?:\.(?P<frac>[0-9]*))?(?:[eE](?P<exp>[+-]?[0-9]{1,9}))?$"
)


class _Digits(dict):
    """str.translate table: every Unicode decimal digit to its ASCII digit.

    Java's Integer.parseInt and BigDecimal read any Unicode digit (Character.digit);
    Double.parseDouble reads ASCII digits only.
    """

    def __missing__(self, code: int) -> Any:
        import unicodedata

        value = unicodedata.decimal(chr(code), None)
        self[code] = chr(code) if value is None else str(value)
        return self[code]


_DIGITS = _Digits()


def _has_high_bytes(values: Any) -> bool:
    import numpy as np

    data = values.buffers()[2]
    return data is not None and bool((np.frombuffer(data, dtype=np.uint8) >= 0x80).any())


def ascii_digits(col: Any) -> Any:
    """``col`` with Unicode digits as ASCII digits (the rest as it is).

    Only when the first values then read as numbers: a text column with accents
    is left alone rather than rewritten value by value.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    if not _has_high_bytes(col):
        return col
    head = pc.drop_null(col.slice(0, 256)).to_pylist()
    changed = [t for v in head for t in [v.translate(_DIGITS)] if t != v]
    if (
        not changed
        or not pc.any(
            pc.match_substring_regex(pa.array(changed, pa.string()), pattern=_BIGDEC)
        ).as_py()
    ):
        return col
    return pa.array(
        [None if v is None else v.translate(_DIGITS) for v in col.to_pylist()], pa.string()
    )


def _fits(values: Any, digits: Any, widths: Any, limit: int) -> Any:
    """Which whole numbers fit in a signed integer of ``limit`` (2**31-1 or 2**63-1)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    top = str(limit)
    negative = pc.starts_with(values, pattern="-")
    bound = pc.if_else(negative, str(limit + 1), top)
    return pc.or_(
        pc.less(widths, len(top)),
        pc.and_(pc.equal(widths, len(top)), pc.less_equal(digits, bound)),
    ).cast(pa.bool_())


def _number_kind(text: Any, values: Any) -> Optional[Tuple[Any, ...]]:
    """Spark's merged numeric type for a column, or None if it is not all numbers.

    ``text`` holds the values with Unicode digits made ASCII; ``values`` as read.
    The port of ``CSVInferSchema.inferField`` for numbers, in row order: each value
    is tried as ``Integer.parseInt``, ``Long.parseLong``, ``new BigDecimal`` (kept
    only when its scale is 0: ``5.``, ``1.5E1`` and ``0E0`` are whole-number
    decimals; ``1E5`` and ``1.5`` are not) and ``Double.parseDouble``. The fold is
    Spark's: once the type is a decimal, later values are read as decimals too, so
    the precision depends on the order (a bigint before the first decimal widens it
    to 20 digits, an int to 10; after it, a bigint adds only its own digits).
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc

    head = text.slice(0, 256)
    if not _all(
        pc.or_(
            pc.match_substring_regex(head, pattern=_BIGDEC),
            _java_number(values.slice(0, 256)),
        )
    ):
        return None
    parts = pc.extract_regex(text, pattern=_BIGDEC)
    whole_part, frac = pc.struct_field(parts, "whole"), pc.struct_field(parts, "frac")
    exp = pc.struct_field(parts, "exp")
    matched = pc.fill_null(
        pc.greater(pc.add(pc.utf8_length(whole_part), pc.utf8_length(frac)), 0), False
    )
    java = _java_number(values)
    if not _all(pc.or_(matched, java)):
        return None
    exponent = pc.if_else(pc.equal(exp, ""), "0", pc.replace_substring(exp, "+", ""))
    exponent = exponent.cast(pa.int64())
    scale0 = pc.fill_null(pc.equal(pc.utf8_length(frac).cast(pa.int64()), exponent), False)
    scale0 = pc.and_(matched, scale0)
    if not _all(scale0):
        # A value that is not a whole number: a double, if Java reads it as one.
        # Once the type is double, each later value must be a Java double too: a
        # whole number in Unicode digits (Integer.parseInt reads it, parseDouble
        # does not) before the first double is read as null, after it makes text.
        if not _all(pc.or_(scale0, java)):
            return ("string",)
        flags = np.asarray(pc.invert(scale0).to_numpy(zero_copy_only=False), dtype=bool)
        first = int(flags.argmax())
        return ("double",) if _all(java.slice(first)) else ("string",)
    digits = pc.replace_substring_regex(
        pc.binary_join_element_wise(whole_part, frac, ""), pattern="^0*", replacement=""
    )
    widths = pc.max_element_wise(pc.utf8_length(digits), 1)
    if pc.max(widths).as_py() > 38:
        # BigDecimal past 38 digits is not a DecimalType: that value is a double,
        # and every value after it must be a Java double too.
        wide = np.asarray(pc.greater(widths, 38).to_numpy(zero_copy_only=False), dtype=bool)
        return ("double",) if _all(java.slice(int(wide.argmax()))) else ("string",)
    plain = pc.match_substring_regex(text, pattern=r"^[+-]?[0-9]+$")
    is_int = pc.and_(plain, _fits(text, digits, widths, 2**31 - 1))
    is_long = pc.and_(plain, _fits(text, digits, widths, 2**63 - 1))
    decimal_like = pc.invert(is_long)  # past a long, or a decimal form
    if not pc.any(decimal_like).as_py():
        return ("int",) if _all(is_int) else ("long",)
    flags = np.asarray(decimal_like.to_numpy(zero_copy_only=False), dtype=bool)
    first = int(flags.argmax())
    before_long = not _all(is_int.slice(0, first)) if first else False
    precision = max(
        pc.max(widths.slice(first)).as_py(),
        20 if before_long else (10 if first else 0),
    )
    return ("double",) if precision > 38 else ("decimal", precision)


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
        # One value with digits after its point and no exponent (1.5) is a true
        # double (BigDecimal scale 1 or more), so the column is double. Without one
        # (whole numbers with a sign or past 64 bits, 5., 1.5E1) Spark's integer
        # and decimal rules decide.
        fraction = pc.and_(
            pc.match_substring(col, "."),
            pc.invert(
                pc.or_(
                    pc.match_substring(col, "e", ignore_case=True),
                    pc.ends_with(col, pattern="."),
                )
            ),
        )
        if not pc.any(fraction).as_py():
            return None
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


def _slow_kind(values: Any, text: Any = None) -> Tuple[Any, ...]:
    """The kind by Java's rules, value by value (as Arrow arrays).

    ``text`` is ``values`` with Unicode digits made ASCII (see ascii_digits).
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    number = _number_kind(values if text is None else text, values)
    if number is not None:
        return number
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

    # A value Double.parseDouble cannot read is null (Spark's PERMISSIVE mode).
    col = pc.if_else(pc.fill_null(_java_number(col), False), col, pa.scalar(None, pa.string()))
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
    if tag in ("int", "long"):
        text = pc.replace_substring_regex(ascii_digits(col), pattern=r"^\+", replacement="")
        return text.cast(pa.int32() if tag == "int" else pa.int64())
    if tag == "decimal":
        # The unscaled digits: the scale is 0, so 1.5E1 is 15 and 5. is 5.
        parts = pc.extract_regex(ascii_digits(col), pattern=_BIGDEC)
        unscaled = pc.binary_join_element_wise(
            pc.replace_substring(pc.struct_field(parts, "sign"), "+", ""),
            pc.struct_field(parts, "whole"),
            pc.struct_field(parts, "frac"),
            "",
        )
        return unscaled.cast(pa.decimal128(kind[1], 0))
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
            if fast is not None:
                kind, typed = fast
            else:
                values = pc.drop_null(col)
                kind, typed = _slow_kind(values, ascii_digits(values)), None
        if typed is None:
            typed = convert(col, kind, cast_timestamp)
        columns.append(typed)
        fields.append(pa.field(field.name, typed.type))
    return pa.table(columns, schema=pa.schema(fields))
