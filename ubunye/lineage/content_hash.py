"""The content hash of a table: every row, any order, the same on every engine (ADR 006).

A run record is only worth keeping if its data hash means "these exact rows".
The hash this replaced read a 1 percent sample, depended on row order on Spark,
and on pandas quietly recorded the schema hash as the data hash. This one:

* reads **every row**, in the same pass as the row count;
* ignores **row order** (a shuffle or a repartition changes nothing);
* ignores **column order** (columns are taken sorted by name);
* changes when **any value** changes, and tells null from NaN;
* does not depend on the machine's **timezone**;
* is **the same on Spark and on pandas** for the same data.

Method ``rows-v1``
------------------
Each row becomes one canonical line: a JSON object with its columns sorted by
name, written the way Spark's ``to_json`` writes it with :data:`JSON_OPTIONS`
(timestamps as UTC text to the microsecond, dates as ``yyyy-MM-dd``, doubles the
Java way, NaN as ``"NaN"``, null fields left out). The line's SHA-256 is cut into
two unsigned 64-bit numbers, and each is summed over all rows, modulo 2**64.
Addition does not care about order, so neither does the hash. The data hash is
the SHA-256 of the method, the canonical schema, the row count and the two sums.

Spark computes the same thing in one distributed aggregation
(:mod:`ubunye.adapters.spark.content_hash`); pandas and Arrow compute it here.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

METHOD = "rows-v1"
_MASK = (1 << 64) - 1

#: The to_json options that make Spark write the canonical line.
JSON_OPTIONS = {
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'",
    "timestampNTZFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "dateFormat": "yyyy-MM-dd",
    "timeZone": "UTC",
}


@dataclass(frozen=True)
class Fingerprint:
    """What a run record says about one table."""

    schema_hash: str
    data_hash: Optional[str]
    row_count: Optional[int]
    method: str = METHOD
    error: Optional[str] = None

    @property
    def is_complete(self) -> bool:
        """False when the rows could not be read: the data hash is then absent."""
        return self.data_hash is not None and self.error is None


def _sha256(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def schema_hash(schema: Sequence[Tuple[str, str]]) -> str:
    """The hash of a canonical schema: ``(name, type)`` pairs, sorted by name."""
    return _sha256(json.dumps(sorted([list(p) for p in schema]), separators=(",", ":")))


def data_hash(schema: Sequence[Tuple[str, str]], rows: int, sums: Tuple[int, int]) -> str:
    """The data hash from the parts every engine computes the same way."""
    payload = {
        "method": METHOD,
        "rows": int(rows),
        "schema": sorted([list(p) for p in schema]),
        "sums": [int(sums[0]) & _MASK, int(sums[1]) & _MASK],
    }
    return _sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def lanes(line: str) -> Tuple[int, int]:
    """A canonical line's SHA-256 as two unsigned 64-bit numbers (big endian)."""
    digest = hashlib.sha256(line.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big"), int.from_bytes(digest[8:16], "big")


# --------------------------------------------------------------------------- #
# Canonical values, written as Spark's to_json writes them
# --------------------------------------------------------------------------- #


def java_double(x: float) -> str:
    """A finite float as Java's ``Double.toString`` writes it."""
    if x == 0:
        return "-0.0" if math.copysign(1.0, x) < 0 else "0.0"
    text = repr(x)
    if 1e-3 <= abs(x) < 1e7:
        return text if "." in text else text + ".0"
    sign, digits, exponent = decimal.Decimal(text).as_tuple()
    ds = "".join(map(str, digits)).rstrip("0") or "0"
    power = len(digits) + int(exponent) - 1
    return f"{'-' if sign else ''}{ds[0]}.{ds[1:] or '0'}E{power}"


def _float_text(x: float) -> str:
    if math.isnan(x):
        return '"NaN"'
    if math.isinf(x):
        return '"Infinity"' if x > 0 else '"-Infinity"'
    return java_double(x)


def _timestamp_text(value: dt.datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(dt.timezone.utc)
        return json.dumps(value.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    return json.dumps(value.strftime("%Y-%m-%dT%H:%M:%S.%f"))


def value_text(value: Any, kind: str = "") -> str:
    """One value as canonical JSON. ``kind`` is a canonical type where known."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if kind == "float32" and math.isfinite(value):
            import numpy as np

            value = float(str(np.float32(value)))  # Java's Float.toString
        return _float_text(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, dt.datetime):
        return _timestamp_text(value)
    if isinstance(value, dt.date):
        return json.dumps(value.isoformat())
    if isinstance(value, (bytes, bytearray, memoryview)):
        return json.dumps(base64.b64encode(bytes(value)).decode("ascii"))
    if isinstance(value, dict):
        return _object_text(value.items(), _child_kinds(kind))
    if isinstance(value, (list, tuple)):
        if kind.startswith("map<"):
            return _object_text(value, _child_kinds(kind))  # Arrow maps are (key, value) pairs
        inner = kind[5:-1] if kind.startswith("list<") else ""
        return "[" + ",".join(value_text(v, inner) for v in value) + "]"
    return json.dumps(str(value), ensure_ascii=False)


def _child_kinds(kind: str) -> Dict[str, str]:
    """``struct<a:int32,b:string>`` -> ``{"a": "int32", "b": "string"}``."""
    if not kind.startswith("struct<"):
        if kind.startswith("map<"):
            return {"*": _split_top(kind[4:-1])[1] if kind.endswith(">") else ""}
        return {}
    out = {}
    for part in _split_top(kind[7:-1]):
        name, _, child = part.partition(":")
        out[name] = child
    return out


def _split_top(text: str) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    for ch in text:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _object_text(items: Iterable[Tuple[Any, Any]], kinds: Dict[str, str]) -> str:
    members = []
    for key, val in items:
        if val is None:
            continue  # Spark leaves null fields out
        kind = kinds.get(str(key), kinds.get("*", ""))
        members.append(f"{json.dumps(str(key), ensure_ascii=False)}:{value_text(val, kind)}")
    return "{" + ",".join(members) + "}"


def canonical_line(row: Dict[str, Any], kinds: Optional[Dict[str, str]] = None) -> str:
    """One row as its canonical line: columns sorted by name, nulls left out."""
    kinds = kinds or {}
    return _object_text(((k, row[k]) for k in sorted(row)), kinds)


# --------------------------------------------------------------------------- #
# Arrow (and so pandas)
# --------------------------------------------------------------------------- #


def arrow_kind(t: Any) -> str:
    """An Arrow type as a canonical type name, the same names Spark's map to."""
    import pyarrow as pa

    if pa.types.is_int8(t):
        return "int8"
    if pa.types.is_int16(t):
        return "int16"
    if pa.types.is_int32(t):
        return "int32"
    if pa.types.is_int64(t):
        return "int64"
    if pa.types.is_float32(t) or pa.types.is_float16(t):
        return "float32"
    if pa.types.is_float64(t):
        return "float64"
    if pa.types.is_boolean(t):
        return "bool"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "timestamp" if t.tz else "timestamp_ntz"
    if pa.types.is_decimal(t):
        return f"decimal({t.precision},{t.scale})"
    if pa.types.is_map(t):
        return f"map<{arrow_kind(t.key_type)},{arrow_kind(t.item_type)}>"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return f"list<{arrow_kind(t.value_type)}>"
    if pa.types.is_struct(t):
        return "struct<" + ",".join(f"{f.name}:{arrow_kind(f.type)}" for f in t) + ">"
    if pa.types.is_null(t):
        return "null"
    return str(t)


def fingerprint_arrow(table: Any) -> Fingerprint:
    """The ``rows-v1`` fingerprint of an Arrow table."""
    schema = [(f.name, arrow_kind(f.type)) for f in table.schema]
    kinds = dict(schema)
    names = sorted(table.column_names)
    total_a = total_b = 0
    for batch in table.select(names).to_batches():
        columns = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
        for values in zip(*columns):
            a, b = lanes(_object_text(zip(names, values), kinds))
            total_a += a
            total_b += b
    return Fingerprint(
        schema_hash=schema_hash(schema),
        data_hash=data_hash(schema, table.num_rows, (total_a, total_b)),
        row_count=table.num_rows,
    )


def fingerprint_rows(
    rows: Sequence[Dict[str, Any]], schema: Sequence[Tuple[str, str]]
) -> Fingerprint:
    """The fingerprint of rows already in memory, for a port that only offers collect()."""
    kinds = dict(schema)
    total_a = total_b = 0
    for row in rows:
        a, b = lanes(canonical_line(dict(row), kinds))
        total_a += a
        total_b += b
    return Fingerprint(
        schema_hash=schema_hash(schema),
        data_hash=data_hash(schema, len(rows), (total_a, total_b)),
        row_count=len(rows),
    )


# --------------------------------------------------------------------------- #
# Any frame the engine hands over
# --------------------------------------------------------------------------- #


def _package(obj: Any) -> str:
    """The top level package an object's type comes from ("pandas", "pyspark", ...).

    Compared by name so nothing is imported to ask. The whole name is not used:
    pandas 3 reports ``DataFrame.__module__`` as plain ``"pandas"``.
    """
    return (getattr(type(obj), "__module__", "") or "").split(".")[0]


def fingerprint(frame: Any) -> Fingerprint:
    """The fingerprint of whatever frame a run produced, or an honest failure.

    Spark DataFrames are hashed where they live, in one distributed pass. pandas
    frames and Arrow tables are hashed here. Anything else that offers the
    ``DataFramePort`` is collected and hashed by its values. Nothing is ever
    guessed: if the rows cannot be read, ``data_hash`` is ``None`` and ``error``
    says why.
    """
    try:
        package = _package(frame)
        if package == "pyspark":
            from ubunye.adapters.spark.content_hash import fingerprint_spark

            return fingerprint_spark(frame)

        from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter

        if isinstance(frame, PandasDataFrameAdapter) or package == "pandas":
            from ubunye.adapters import pandas_io

            timezone = getattr(frame, "timezone", None) or "UTC"
            return fingerprint_arrow(pandas_io.to_arrow(frame, timezone))
        if package == "pyarrow":
            return fingerprint_arrow(frame)

        rows = [
            r.asDict(recursive=True) if hasattr(r, "asDict") else dict(r) for r in frame.collect()
        ]
        port_schema = getattr(frame, "schema", {}) or {}
        schema = [(str(k), str(v)) for k, v in dict(port_schema).items()]
        return fingerprint_rows(rows, schema)
    except Exception as exc:  # a receipt must never fail a run that succeeded
        return Fingerprint(
            schema_hash=_sha256(str(getattr(frame, "schema", ""))),
            data_hash=None,
            row_count=None,
            error=f"{type(exc).__name__}: {exc}",
        )
