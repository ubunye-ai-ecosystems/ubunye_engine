"""Python records into a pandas frame, and back, by Spark's rules (F-015).

The ``rest_api`` connector gets a list of dicts (parsed JSON) and needs a frame.
On Spark that is ``spark.createDataFrame(records)``. This module gives the pandas
backend the same frame: the same columns in the same order, the same types and
the same values. It is a port of PySpark's own code (``pyspark.sql.types``:
``_infer_type``, ``_infer_schema``, ``_merge_type``, ``_create_converter`` and
``_make_type_verifier``), read from the source of pyspark 4.2 and 3.5.8.

Spark's rules, as ported:

* Each record's keys are sorted by name. Keys that first appear in a later record
  are added at the end, in that record's sorted order.
* ``int`` is ``bigint``, ``float`` is ``double``, ``bool`` is ``boolean``, ``str``
  is ``string``. A JSON object is a ``map<string, ...>`` (not a struct), and a JSON
  array is an ``array<...>``.
* A field that is a number in one record and text in another is ``string``; the
  number becomes its text (``7`` becomes ``"7"``, ``True`` becomes ``"true"``).
* A whole number in one record and a decimal number in another is an error, as
  is a field that is null in every record. Spark refuses both; so does this.
* With an explicit schema, a value must already have the column's Python type
  (Spark does not turn ``1`` into ``1.0`` for a ``double``), and a ``string``
  column takes anything, as its text.

Not ported, on purpose: a map's entry order. A map has no order, and Spark's is
not stable: Spark classic takes it from the JVM's ``HashMap`` (as Pyrolite fills
it, which changed between Java 11 and Java 21), Spark Connect keeps the JSON's.
This module keeps the JSON's. The run record's hash sorts every map by key, so
it does not depend on the order (ADR 006).

Known differences, all in values Spark 3.5 and 4 also disagree on: Spark 3.5
writes a decimal number in a text column the JVM's way (``1.0E7``) where Spark 4
and this module write Python's (``10000000.0``). A text column given a JSON
object or array by an explicit schema gets Python's text of it, as on Spark 4.
Spark 3.5 takes a map's value type from its first non-null entry only; Spark 4
and this module merge every entry. Spark reads a record that is a JSON array as
a row of columns ``_1``, ``_2``...; this module refuses it.
"""

from __future__ import annotations

from functools import reduce
from typing import Any, Dict, Iterator, List, Optional, Tuple

# Spark types, held as small tuples:
#   ("null",) ("long",) ("double",) ("string",) ("boolean",)
#   ("map", key, value) ("array", element) ("struct", [(name, type), ...])
NULL = ("null",)
LONG = ("long",)
DOUBLE = ("double",)
STRING = ("string",)
BOOLEAN = ("boolean",)
_ATOMIC = {"long", "double", "string", "boolean"}
_NAMES = {
    "null": "NullType",
    "long": "LongType",
    "double": "DoubleType",
    "string": "StringType",
    "boolean": "BooleanType",
    "map": "MapType",
    "array": "ArrayType",
    "struct": "StructType",
}


def _spark_name(t: Tuple) -> str:
    return _NAMES[t[0]]


# --------------------------------------------------------------------------- #
# Inference (pyspark.sql.types._infer_type, _infer_schema, _merge_type)
# --------------------------------------------------------------------------- #


def infer_type(value: Any) -> Tuple:
    """The Spark type of one JSON value, as ``createDataFrame`` infers it."""
    if value is None:
        return NULL
    kind = type(value)  # exact type: a bool is not an int here, as in Spark
    if kind is bool:
        return BOOLEAN
    if kind is int:
        return LONG
    if kind is float:
        return DOUBLE
    if kind is str:
        return STRING
    if isinstance(value, dict):
        key_type, value_type = NULL, NULL
        for k, v in value.items():
            if k is not None:
                key_type = merge_type(key_type, infer_type(k))
            if v is not None:
                value_type = merge_type(value_type, infer_type(v))
        return ("map", key_type, value_type)
    if isinstance(value, list):
        if not value:
            return ("array", NULL)
        return ("array", reduce(merge_type, (infer_type(v) for v in value)))
    raise TypeError(f"[UNSUPPORTED_DATA_TYPE] Unsupported DataType `{kind.__name__}`.")


def infer_row(record: Any) -> Tuple:
    """A record's struct type: its fields sorted by name."""
    if not isinstance(record, dict):
        raise TypeError(
            f"[CANNOT_INFER_SCHEMA_FOR_TYPE] Can not infer schema for type: "
            f"`{type(record).__name__}`. Each record must be a JSON object."
        )
    fields = []
    for name, value in sorted(record.items()):
        try:
            fields.append((name, infer_type(value)))
        except TypeError:
            raise TypeError(
                f"[CANNOT_INFER_TYPE_FOR_FIELD] Unable to infer the type of the field `{name}`."
            ) from None
    return ("struct", fields)


def merge_type(a: Tuple, b: Tuple, name: Optional[str] = None) -> Tuple:
    """Two inferred types as one, or ``TypeError`` where Spark raises one."""

    def new_name(n: str) -> str:
        return f"field {n}" if name is None else f"field {n} in {name}"

    if a == NULL:
        return b
    if b == NULL:
        return a
    if a[0] in _ATOMIC and b == STRING:
        return b
    if a == STRING and b[0] in _ATOMIC:
        return a
    if a[0] != b[0]:
        raise TypeError(
            f"[CANNOT_MERGE_TYPE] Can not merge type `{_spark_name(a)}` and "
            f"`{_spark_name(b)}`" + (f" ({name})." if name else ".")
        )
    if a[0] == "struct":
        theirs = dict(b[1])
        fields = [(n, merge_type(t, theirs.get(n, NULL), name=new_name(n))) for n, t in a[1]]
        known = {n for n, _ in fields}
        fields += [(n, t) for n, t in b[1] if n not in known]
        return ("struct", fields)
    if a[0] == "array":
        return ("array", merge_type(a[1], b[1], name=f"element in array {name}"))
    if a[0] == "map":
        return (
            "map",
            merge_type(a[1], b[1], name=f"key of map {name}"),
            merge_type(a[2], b[2], name=f"value of map {name}"),
        )
    return a


def has_null(t: Tuple) -> bool:
    """Whether a type still holds a null type anywhere (Spark cannot use it)."""
    if t[0] == "struct":
        return any(has_null(ft) for _, ft in t[1])
    if t[0] == "array":
        return has_null(t[1])
    if t[0] == "map":
        return has_null(t[1]) or has_null(t[2])
    return t == NULL


def infer_schema(records: List[Any]) -> Tuple:
    """The struct type Spark infers for a list of records (never empty)."""
    schema = reduce(merge_type, (infer_row(r) for r in records))
    if has_null(schema):
        nulls = [n for n, t in schema[1] if has_null(t)]
        raise ValueError(
            "[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring "
            f"(null in every record: {', '.join(nulls)})."
        )
    return schema


# --------------------------------------------------------------------------- #
# Values (pyspark.sql.types._create_converter, Spark 4)
# --------------------------------------------------------------------------- #


def _as_text(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"  # Spark: lower case
    return str(value)


def convert(value: Any, t: Tuple) -> Any:
    """One value as the frame holds it; maps become Arrow's (key, value) pairs."""
    if value is None:
        return None
    kind = t[0]
    if kind == "string":
        return _as_text(value)
    if kind == "null":
        return None
    if kind == "array":
        return [convert(v, t[1]) for v in value]
    if kind == "map":
        # The JSON's order. Spark's order is not stable (see the module notes).
        return [(convert(k, t[1]), convert(v, t[2])) for k, v in value.items()]
    return value


# --------------------------------------------------------------------------- #
# Frames
# --------------------------------------------------------------------------- #


def arrow_type(t: Tuple) -> Any:
    import pyarrow as pa

    kind = t[0]
    if kind == "long":
        return pa.int64()
    if kind == "double":
        return pa.float64()
    if kind == "string":
        return pa.string()
    if kind == "boolean":
        return pa.bool_()
    if kind == "array":
        return pa.list_(arrow_type(t[1]))
    if kind == "map":
        return pa.map_(arrow_type(t[1]), arrow_type(t[2]))
    raise TypeError(f"no Arrow type for {_spark_name(t)}")  # pragma: no cover


def _inferred_table(records: List[Dict[str, Any]]) -> Any:
    import pyarrow as pa

    schema = infer_schema(records)
    columns = {}
    for name, t in schema[1]:
        values = [convert(r.get(name), t) for r in records]
        try:
            columns[name] = pa.array(values, type=arrow_type(t))
        except (OverflowError, pa.ArrowInvalid) as exc:
            # A whole number past 64 bits. Spark classic stores null there without
            # a word (its bigint converter takes no BigInteger); this refuses.
            raise ValueError(
                f"[VALUE_OUT_OF_BOUNDS] field {name}: a whole number is outside "
                f"{-(2**63)} to {2**63 - 1} ({exc}). Spark stores null there; the "
                "pandas backend refuses. Declare the field as string to keep it as text."
            ) from None
    if not columns:  # records with no fields: one row each, as on Spark (struct<>)
        from ubunye.adapters import pandas_io

        return pandas_io.no_columns(len(records))
    return pa.table(columns)


# What each Arrow type accepts from Python, as Spark's _acceptable_types says.
_INT_RANGES = {8: 2**7, 16: 2**15, 32: 2**31, 64: 2**63}


def _check(value: Any, target: Any, name: str) -> None:
    """``TypeError`` or ``ValueError`` where Spark's schema check refuses a value."""
    import pyarrow as pa

    if value is None or pa.types.is_string(target):
        return
    if pa.types.is_integer(target):
        ok = type(value) is int
    elif pa.types.is_floating(target):
        ok = type(value) is float
    elif pa.types.is_boolean(target):
        ok = type(value) is bool
    else:  # timestamp, date, decimal, binary: JSON never gives the Python type
        ok = False
    if not ok:
        raise TypeError(
            f"[FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME] field {name}: {target} can not "
            f"accept object {value!r} in type {type(value)}."
        )
    if pa.types.is_integer(target):
        bound = _INT_RANGES[target.bit_width]
        if not -bound <= value < bound:
            raise ValueError(
                f"[VALUE_OUT_OF_BOUNDS] field {name}: {value} is outside "
                f"{-bound} to {bound - 1}."
            )


def _typed_table(records: List[Dict[str, Any]], schema: Any) -> Any:
    import pyarrow as pa

    columns = []
    for field in schema:
        values = []
        for record in records:
            if not isinstance(record, dict):
                raise TypeError(
                    f"[CANNOT_INFER_SCHEMA_FOR_TYPE] a record is a {type(record).__name__}; "
                    "each record must be a JSON object."
                )
            value = record.get(field.name)
            _check(value, field.type, field.name)
            values.append(_as_text(value) if pa.types.is_string(field.type) else value)
        columns.append(pa.array(values, type=field.type))
    return pa.table(columns, schema=schema)


def frame_from_records(
    records: List[Dict[str, Any]], schema: Optional[str] = None, *, timezone: str = "UTC"
) -> Any:
    """A list of dicts as an Arrow-backed pandas frame, typed as Spark types it.

    ``schema`` is a Spark DDL string; without one the types are inferred. No
    records and no schema give a frame with no columns, as on Spark.
    """
    import pyarrow as pa

    from ubunye.adapters import ddl, pandas_io

    if schema:
        table = _typed_table(records, ddl.parse(schema, timezone=timezone))
    elif not records:
        table = pa.table({})
    else:
        table = _inferred_table(records)
    return pandas_io.to_pandas(table)


def iter_records(frame: Any, *, timezone: str = "UTC", batch_rows: int = 10_000) -> Iterator[Dict]:
    """Each row of a frame as a plain dict, as Spark's ``Row.asDict(recursive=True)``.

    A map comes back as a dict (Arrow holds it as pairs). Rows are produced a
    batch at a time, so a large frame is never turned into Python all at once.
    """
    from ubunye.adapters import pandas_io

    table = pandas_io.to_arrow(frame, timezone)
    if table.num_columns == 0:  # rows with no fields: an empty object each, as on Spark
        for _ in range(table.num_rows):
            yield {}
        return
    for batch in table.to_batches(max_chunksize=batch_rows):
        kinds = [f.type for f in batch.schema]
        for row in batch.to_pylist():
            yield {name: _plain(value, t) for (name, value), t in zip(row.items(), kinds)}


def _plain(value: Any, t: Any) -> Any:
    import pyarrow as pa

    if value is None:
        return None
    if pa.types.is_map(t):
        return {k: _plain(v, t.item_type) for k, v in value}
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return [_plain(v, t.value_type) for v in value]
    if pa.types.is_struct(t):
        return {f.name: _plain(value.get(f.name), f.type) for f in t}
    return value
