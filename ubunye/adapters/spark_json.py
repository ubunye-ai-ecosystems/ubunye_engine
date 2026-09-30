"""JSON records typed exactly as Spark's JSON reader types them.

Spark infers a JSON file's schema with ``JsonInferSchema`` and reads the values
with ``JacksonParser`` (Spark 3.5 and 4, ``org.apache.spark.sql.catalyst.json``).
pyarrow and pandas infer differently, and stop where Spark goes on: a field that
is a number in one record and text in the next, an object in one and text in the
next, or a whole number past 64 bits. This module is a port of Spark's rules, with
the Scala names kept so each rule can be checked against the source:

* ``infer_field``: ``null`` and an empty string are ``NullType``; ``true`` and
  ``false`` are ``BooleanType``; a whole number that fits in 64 bits is
  ``LongType``, a longer one ``DecimalType(digits, 0)`` (``DoubleType`` past 38
  digits); a number with a point or exponent is ``DoubleType``; an object is a
  struct of its fields sorted by name; an array's element type is every element's
  type merged.
* ``compatible_type``: two types merge to the tightest type that holds both
  (``LongType`` and ``DoubleType`` give ``DoubleType``; decimals widen), structs
  merge field by field, arrays by element, and anything else is ``StringType``.
* ``canonicalize_type``: fields with an empty name are dropped, a struct with no
  fields is dropped (SPARK-8093), and ``NullType`` is ``StringType``.
* ``convert``: a value read into a ``StringType`` column that is not a JSON
  string keeps its JSON text, as Jackson copies it (``1``, ``1.5``, ``true``,
  ``{"k":1}``, ``[1,"a"]``).

Names are sorted the Java way (by UTF-16 code unit), as ``String.compareTo`` does.
"""

from __future__ import annotations

import decimal
import json
import math
from typing import Any, Dict, List, Optional, Tuple

# Spark types, held as small tuples:
#   ("null",) ("long",) ("double",) ("boolean",) ("string",) ("decimal", p, s)
#   ("array", element) ("struct", ((name, type), ...))  names sorted
NULL = ("null",)
LONG = ("long",)
DOUBLE = ("double",)
BOOLEAN = ("boolean",)
STRING = ("string",)

_LONG_MIN, _LONG_MAX = -(2**63), 2**63 - 1
_MAX_PRECISION = 38


def java_order(name: str) -> bytes:
    """The sort key Java's ``String.compareTo`` gives: UTF-16 code units."""
    return name.encode("utf-16-be", "surrogatepass")


def _decimal_of_integer(value: int) -> Tuple[Any, ...]:
    digits = len(str(abs(value)))
    return ("decimal", digits, 0) if digits <= _MAX_PRECISION else DOUBLE


def infer_field(value: Any) -> Tuple[Any, ...]:
    """``JsonInferSchema.inferField`` for one parsed JSON value."""
    kind = type(value)
    if kind is str:
        # An empty string is taken for null while types are merged.
        return NULL if value == "" else STRING
    if kind is float:
        return DOUBLE
    if kind is int and _LONG_MIN <= value <= _LONG_MAX:
        return LONG
    if value is None:
        return NULL
    if value is True or value is False:
        return BOOLEAN
    if isinstance(value, int):
        # Jackson: INT and LONG are LongType; BIG_INTEGER is a decimal.
        if _LONG_MIN <= value <= _LONG_MAX:
            return LONG
        return _decimal_of_integer(value)
    if isinstance(value, float):
        return DOUBLE
    if isinstance(value, str):
        # An empty string is taken for null while types are merged.
        return NULL if value == "" else STRING
    if isinstance(value, dict):
        fields = sorted(((str(k), infer_field(v)) for k, v in value.items()), key=_by_name)
        return ("struct", _dedupe(fields))
    if isinstance(value, list):
        element = NULL
        for item in value:
            element = compatible_type(element, infer_field(item))
        return ("array", element)
    return STRING


def _by_name(field: Tuple[str, Any]) -> bytes:
    return java_order(field[0])


def _dedupe(fields: List[Tuple[str, Any]]) -> Tuple[Tuple[str, Any], ...]:
    """A struct's fields with a repeated key merged (a JSON object may repeat one)."""
    out: List[Tuple[str, Any]] = []
    for name, kind in fields:
        if out and out[-1][0] == name:
            out[-1] = (name, compatible_type(out[-1][1], kind))
        else:
            out.append((name, kind))
    return tuple(out)


def _decimal_for_integral(kind: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """``DecimalType.forType`` for Spark's integral types (only LongType here)."""
    return ("decimal", 20, 0)


def _wider_decimal(a: Tuple[Any, ...], b: Tuple[Any, ...]) -> Tuple[Any, ...]:
    scale = max(a[2], b[2])
    whole = max(a[1] - a[2], b[1] - b[2])
    if whole + scale > _MAX_PRECISION:
        return DOUBLE
    return ("decimal", whole + scale, scale)


def _tightest(a: Tuple[Any, ...], b: Tuple[Any, ...]) -> Optional[Tuple[Any, ...]]:
    """``TypeCoercion.findTightestCommonType`` for the types JSON inference makes."""
    if a == b:
        return a
    if a == NULL:
        return b
    if b == NULL:
        return a
    if {a, b} == {LONG, DOUBLE}:
        return DOUBLE
    for d, other in ((a, b), (b, a)):
        # A decimal that holds every long (20 whole digits or more) takes a long.
        if d[0] == "decimal" and other == LONG and d[1] - d[2] >= 20:
            return d
    if a[0] == "array" and b[0] == "array":
        element = _tightest(a[1], b[1])
        return None if element is None else ("array", element)
    if a[0] == "struct" and b[0] == "struct":
        return _same_shape(a[1], b[1])
    return None


def _same_shape(a: Tuple[Any, ...], b: Tuple[Any, ...]) -> Optional[Tuple[Any, ...]]:
    """``TypeCoercion.findTypeForComplex`` for two structs (spark.sql.caseSensitive false).

    Structs with as many fields, whose names (in sorted order) are equal ignoring
    case, and whose field types each have a tightest common type, are one struct
    with the first struct's names. So ``{"Id":1}`` then ``{"id":2}`` is one field
    ``Id`` (and Spark then reads ``id`` as a different name: null).
    """
    if len(a) != len(b):
        return None
    fields = []
    for (name, x), (other, y) in zip(a, b):
        if name.lower() != other.lower():
            return None
        common = _tightest(x, y)
        if common is None:
            return None
        fields.append((name, common))
    return ("struct", tuple(fields))


def compatible_type(a: Tuple[Any, ...], b: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """``JsonInferSchema.compatibleType``: the type that holds values of both."""
    tight = _tightest(a, b)
    if tight is not None:
        return tight
    if (a == DOUBLE and b[0] == "decimal") or (a[0] == "decimal" and b == DOUBLE):
        return DOUBLE
    if a[0] == "decimal" and b[0] == "decimal":
        return _wider_decimal(a, b)
    if a[0] == "struct" and b[0] == "struct":
        merged: Dict[str, Tuple[Any, ...]] = dict(a[1])
        for name, kind in b[1]:
            merged[name] = compatible_type(merged[name], kind) if name in merged else kind
        return ("struct", tuple(sorted(merged.items(), key=_by_name)))
    if a[0] == "array" and b[0] == "array":
        return ("array", compatible_type(a[1], b[1]))
    if a == LONG and b[0] == "decimal":
        return compatible_type(_decimal_for_integral(a), b)
    if a[0] == "decimal" and b == LONG:
        return compatible_type(a, _decimal_for_integral(b))
    return STRING


def canonicalize_type(kind: Tuple[Any, ...]) -> Optional[Tuple[Any, ...]]:
    """``JsonInferSchema.canonicalizeType``: None means the field is dropped."""
    if kind[0] == "array":
        element = canonicalize_type(kind[1])
        return None if element is None else ("array", element)
    if kind[0] == "struct":
        fields = []
        for name, sub in kind[1]:
            if not name:
                continue
            canon = canonicalize_type(sub)
            if canon is not None:
                fields.append((name, canon))
        return ("struct", tuple(fields)) if fields else None
    if kind == NULL:
        return STRING
    return kind


def infer_schema(rows: List[Dict[str, Any]]) -> Tuple[Tuple[str, Any], ...]:
    """The top level fields (name, type) of a list of JSON objects, as Spark infers them.

    The same as merging ``infer_field`` of every row, kept per field so a flat
    record costs one type lookup per value.
    """
    kinds: Dict[str, Tuple[Any, ...]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(
                f"a JSON record must be an object; found {type(row).__name__} "
                "(Spark reads it as a _corrupt_record, which the pandas backend does not)"
            )
        found = {key: infer_field(value) for key, value in row.items()}
        if kinds and found.keys() != kinds.keys() and len(found) == len(kinds):
            # The whole record against the type so far, as Spark folds the root:
            # a record whose names differ only by case merges into the names seen
            # first (findTypeForComplex).
            same = _same_shape(
                tuple(sorted(kinds.items(), key=_by_name)),
                tuple(sorted(found.items(), key=_by_name)),
            )
            if same is not None:
                kinds = dict(same[1])
                continue
        for key, kind in found.items():
            seen = kinds.get(key)
            if seen is None:
                kinds[key] = kind
            elif seen != kind:
                kinds[key] = compatible_type(seen, kind)
    canon = canonicalize_type(("struct", tuple(sorted(kinds.items(), key=_by_name))))
    return canon[1] if canon is not None else ()


def column(values: List[Any], kind: Tuple[Any, ...], raws: Any = None) -> Any:
    """One column of parsed values as an Arrow array of ``kind``, converted as Spark reads it.

    Arrow takes most columns as they are; a column that needs Spark's conversion
    (a number in a text column, an empty string in a number column) is
    converted value by value. ``raws``, when given, is a function that returns
    each value's source text (see :func:`raw_tree`), for the text columns.
    """
    import pyarrow as pa

    target = arrow_type(kind)
    try:
        return pa.array(values, type=target)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError):
        sources = raws() if raws is not None else [None] * len(values)
        return pa.array([convert(v, kind, r) for v, r in zip(values, sources)], type=target)


def raw_tree(text: str, start: int = 0) -> Tuple[Any, int]:
    """The source text of every value in one JSON document: (tree, end).

    An object gives ``{key: (text, tree)}``, an array ``[(text, tree), ...]``, and
    anything else None. Spark 4 reads a JSON lines value that lands in a text
    column as its exact source text (``1.50``, ``1e2``, ``{ "k" : 1 }`` with its
    spaces and escapes as written); this is how the pandas reader finds that text.
    """
    import json.decoder

    decoder = json.decoder.JSONDecoder()
    ws = json.decoder.WHITESPACE

    def skip(i: int) -> int:
        return ws.match(text, i).end()

    def value(i: int) -> Tuple[Any, int]:
        i = skip(i)
        if text.startswith("{", i):
            members: Dict[str, Any] = {}
            i = skip(i + 1)
            if text.startswith("}", i):
                return members, i + 1
            while True:
                key, i = json.decoder.scanstring(text, skip(i) + 1)
                i = skip(skip(i) + 1)  # past the colon
                sub, end = value(i)
                members[key] = (text[i:end], sub)
                i = skip(end)
                if text.startswith("}", i):
                    return members, i + 1
                i += 1  # the comma
        if text.startswith("[", i):
            items: List[Any] = []
            i = skip(i + 1)
            if text.startswith("]", i):
                return items, i + 1
            while True:
                i = skip(i)
                sub, end = value(i)
                items.append((text[i:end], sub))
                i = skip(end)
                if text.startswith("]", i):
                    return items, i + 1
                i += 1
        _, end = decoder.raw_decode(text, i)
        return None, end

    tree, end = value(start)
    return tree, end


def arrow_type(kind: Tuple[Any, ...]) -> Any:
    import pyarrow as pa

    tag = kind[0]
    if tag == "long":
        return pa.int64()
    if tag == "double":
        return pa.float64()
    if tag == "boolean":
        return pa.bool_()
    if tag == "string":
        return pa.string()
    if tag == "decimal":
        return pa.decimal128(kind[1], kind[2])
    if tag == "array":
        return pa.list_(arrow_type(kind[1]))
    if tag == "struct":
        return pa.struct([pa.field(n, arrow_type(t)) for n, t in kind[1]])
    raise ValueError(f"no Arrow type for {kind}")


def jackson_text(value: Any) -> str:
    """A parsed JSON value written back compactly, as Jackson's copyCurrentStructure writes it."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return _java_double_text(value)
    if isinstance(value, str):
        return _jackson_string(value)
    if isinstance(value, dict):
        return (
            "{"
            + ",".join(f"{_jackson_string(str(k))}:{jackson_text(v)}" for k, v in value.items())
            + "}"
        )
    if isinstance(value, list):
        return "[" + ",".join(jackson_text(v) for v in value) + "]"
    return json.dumps(value)


def _java_double_text(x: float) -> str:
    if math.isnan(x):
        return '"NaN"'  # Jackson quotes the non numeric numbers by default
    if math.isinf(x):
        return '"Infinity"' if x > 0 else '"-Infinity"'
    from ubunye.lineage.content_hash import java_double

    return java_double(x)


_SHORT_ESCAPES = {"\b": "\\b", "\t": "\\t", "\n": "\\n", "\f": "\\f", "\r": "\\r"}


def _jackson_string(text: str) -> str:
    """A JSON string as Jackson escapes it: quote, backslash, control characters."""
    out = ['"']
    for ch in text:
        if ch == '"' or ch == "\\":
            out.append("\\" + ch)
        elif ch < " ":
            out.append(_SHORT_ESCAPES.get(ch) or f"\\u{ord(ch):04X}")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def convert(value: Any, kind: Tuple[Any, ...], raw: Any = None) -> Any:
    """One parsed value as the Python value Arrow takes for ``kind`` (JacksonParser).

    ``raw`` is the value's ``(source text, tree)`` from :func:`raw_tree` when the
    source text is known: a value that is not a string, read into a text column,
    is then that exact text (Spark 4, JSON lines); without it, the text Jackson
    writes back (Spark 4 multiLine, and Spark 3.5).
    """
    if value is None:
        return None
    tag = kind[0]
    if tag == "string":
        if isinstance(value, str):
            return value
        return raw[0] if raw is not None else jackson_text(value)
    if isinstance(value, str) and value == "":
        # Spark cannot read "" as a number or a struct; in PERMISSIVE mode the
        # field is null and the rest of the record is kept.
        return None
    if tag == "long":
        return int(value)
    if tag == "double":
        return float(value)
    if tag == "boolean":
        return bool(value)
    if tag == "decimal":
        return decimal.Decimal(value) if isinstance(value, int) else decimal.Decimal(repr(value))
    sub = raw[1] if raw is not None else None
    if tag == "array":
        return [
            convert(v, kind[1], sub[i] if sub is not None else None) for i, v in enumerate(value)
        ]
    if tag == "struct":
        return {
            name: convert(value.get(name), k, sub.get(name) if sub is not None else None)
            for name, k in kind[1]
        }
    raise ValueError(f"cannot convert to {kind}")
