"""Spark DDL schema strings, read into Arrow schemas.

A reader's ``schema:`` in config.yaml is a Spark DDL string such as
``"id INT, name STRING, amount DECIMAL(10,2)"``. Spark parses it itself; any
other engine needs the same string to mean the same columns and types, so it is
parsed once here into an Arrow schema (Arrow is the interchange format between
engines).

Only flat, scalar types are accepted. Anything else raises ``ValueError`` naming
the column and the type, so a caller can refuse clearly instead of guessing.
"""

from __future__ import annotations

import re
from typing import Any, List, Tuple

# Spark SQL type name -> Arrow type factory. Aliases follow Spark's parser.
_SCALARS = {
    "BOOLEAN": "bool_",
    "TINYINT": "int8",
    "BYTE": "int8",
    "SMALLINT": "int16",
    "SHORT": "int16",
    "INT": "int32",
    "INTEGER": "int32",
    "BIGINT": "int64",
    "LONG": "int64",
    "FLOAT": "float32",
    "REAL": "float32",
    "DOUBLE": "float64",
    "STRING": "string",
    "BINARY": "binary",
    "DATE": "date32",
}

_DECIMAL = re.compile(r"^(?:DECIMAL|DEC|NUMERIC)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?$")
_CHARS = re.compile(r"^(?:VAR)?CHAR\s*\(\s*\d+\s*\)$")


def _split_top_level(text: str) -> List[str]:
    """Split on commas that are not inside parentheses, brackets or backticks."""
    parts: List[str] = []
    current: List[str] = []
    depth, quoted = 0, False
    for ch in text:
        if ch == "`":
            quoted = not quoted
        elif not quoted and ch in "(<":
            depth += 1
        elif not quoted and ch in ")>":
            depth -= 1
        elif not quoted and depth == 0 and ch == ",":
            parts.append("".join(current))
            current = []
            continue
        current.append(ch)
    parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


def _field(item: str) -> Tuple[str, str]:
    """``"`my col` STRING"`` -> ``("my col", "STRING")``; also ``name: type``."""
    item = item.strip()
    if item.startswith("`"):
        end = item.index("`", 1)
        name, rest = item[1:end], item[end + 1 :]
    else:
        match = re.match(r"^([^\s:]+)\s*:?\s*(.*)$", item, re.S)
        if not match:
            raise ValueError(f"cannot read the column definition '{item}'")
        name, rest = match.group(1), match.group(2)
    return name, rest.lstrip(":").strip()


def arrow_type(spark_type: str, *, timezone: str = "UTC") -> Any:
    """One Spark SQL type name as an Arrow type, or ``ValueError``."""
    import pyarrow as pa

    upper = re.sub(r"\s+", " ", spark_type.strip().upper())
    if upper in _SCALARS:
        return getattr(pa, _SCALARS[upper])()
    if upper == "TIMESTAMP" or upper == "TIMESTAMP_LTZ":
        return pa.timestamp("us", tz=timezone)
    if upper == "TIMESTAMP_NTZ":
        return pa.timestamp("us")
    if _CHARS.match(upper):
        return pa.string()
    decimal = _DECIMAL.match(upper)
    if decimal:
        precision = int(decimal.group(1) or 10)
        scale = int(decimal.group(2) or 0)
        return pa.decimal128(precision, scale)
    raise ValueError(f"unsupported type {spark_type.strip()}")


def parse(ddl: str, *, timezone: str = "UTC") -> Any:
    """A Spark DDL schema string as a ``pyarrow.Schema``, or ``ValueError``.

    ``timezone`` is the zone ``TIMESTAMP`` columns are held in; Arrow stores the
    instant in UTC either way, so this only affects how values print.
    """
    import pyarrow as pa

    items = _split_top_level(ddl)
    if not items:
        raise ValueError("the schema is empty")
    fields = []
    for item in items:
        name, type_text = _field(item)
        if not type_text:
            raise ValueError(f"column '{name}' has no type")
        if re.search(r"\bNOT\s+NULL\b|\bCOMMENT\b", type_text, re.I):
            raise ValueError(f"column '{name}': NOT NULL and COMMENT are not supported")
        try:
            fields.append(pa.field(name, arrow_type(type_text, timezone=timezone)))
        except ValueError as exc:
            raise ValueError(f"column '{name}': {exc}") from None
    names = [f.name for f in fields]
    if len(set(names)) != len(names):
        raise ValueError(f"duplicate column names in {names}")
    return pa.schema(fields)
