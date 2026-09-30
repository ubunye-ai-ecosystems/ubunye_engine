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
import logging
import math
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

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


_INT_KINDS = frozenset({"int8", "int16", "int32", "int64"})


def _members(name: str, kind: str, values: List[Any]) -> List[Optional[str]]:
    """One column's ``"name":value`` texts, or None where the value is null.

    The same text :func:`_object_text` writes, built a column at a time: the name
    is encoded once, and the common kinds skip the general dispatch. This is what
    made hashing about four times faster; a test holds it to the row-at-a-time
    reference, byte for byte.
    """
    prefix = json.dumps(name, ensure_ascii=False) + ":"
    if kind in _INT_KINDS:
        return [None if v is None else prefix + str(v) for v in values]
    if kind == "string":
        enc = _encode_string
        return [None if v is None else prefix + enc(v) for v in values]
    if kind == "bool":
        return [None if v is None else prefix + ("true" if v else "false") for v in values]
    if kind == "float64":
        return [None if v is None else prefix + _float_text(v) for v in values]
    if kind == "date":
        return [None if v is None else prefix + '"' + v.isoformat() + '"' for v in values]
    return [None if v is None else prefix + value_text(v, kind) for v in values]


def _dumps_string(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)


_encode_string: Callable[[str], str] = _dumps_string
try:  # the C encoder json.dumps(s, ensure_ascii=False) uses for a str
    from json.encoder import encode_basestring as _c_encode

    if _c_encode('a"b\né') == json.dumps('a"b\né', ensure_ascii=False):
        _encode_string = _c_encode
except ImportError:  # pragma: no cover
    pass


#: Rows per slice on the vectorised path. It bounds the memory of one slice's text
#: and keeps its offsets well inside Arrow's 32 bit string limit.
_SLICE_ROWS = 1 << 15

#: A float whose Java text is plain decimal (no exponent), where Arrow's shortest
#: text and Python's repr agree apart from the ".0" Java adds.
_PLAIN_FLOAT_LOW, _PLAIN_FLOAT_HIGH = 1e-3, 1e7

#: date32 days for 0001-01-01 and 9999-12-31, the dates Python can hold.
_DATE32_MIN, _DATE32_MAX = -719162, 2932896

#: Microseconds since 1970 of 1000-01-01 and 10000-01-01 (UTC): the timestamps
#: Arrow and Python both write with a four digit year.
_TS_MIN, _TS_MAX = -30610224000 * 10**6, 253402300800 * 10**6


def _python_lanes(columns: List[List[Optional[str]]]) -> Tuple[int, int]:
    """The two lane sums from members built in Python, one row at a time."""
    total_a = total_b = 0
    for members in zip(*columns):
        line = "{" + ",".join([m for m in members if m is not None]) + "}"
        digest = hashlib.sha256(line.encode("utf-8")).digest()
        total_a += int.from_bytes(digest[:8], "big")
        total_b += int.from_bytes(digest[8:16], "big")
    return total_a, total_b


def _arrow_members(name: str, kind: str, col: Any) -> Optional[Any]:
    """One column's ``,"name":value`` texts as an Arrow string array, or None.

    Built with Arrow compute instead of Python, for the kinds where Arrow's text
    is provably the canonical text: integers, booleans, strings, dates, doubles
    and microsecond (or coarser) timestamps. Values those rules do not cover (a
    string with a control character, a double outside the plain decimal range,
    NaN, a timestamp before year 1000) are written by :func:`_members`, the Python
    path, and put back in place. A null stays null. Every member starts with a
    comma, which the caller drops for the first one. None means the kind is not
    vectorised: the caller uses :func:`_members` for the whole column.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    t = col.type
    head = "," + json.dumps(name, ensure_ascii=False) + ":"
    tail = ""
    mask = None  # True where the Python path writes the value
    if pa.types.is_integer(t):
        text = pc.cast(col, pa.string())
    elif pa.types.is_boolean(t):
        text = pc.if_else(col, "true", "false")
    elif pa.types.is_string(t) or pa.types.is_large_string(t):
        col = col.cast(pa.string())
        # Invalid UTF-8 (a string column cast from bytes unchecked) raises here, so
        # the slice goes to the Python path, which fails with UnicodeDecodeError
        # and records no digest, as before; Arrow would hash the raw bytes and
        # disagree with Spark.
        col.validate(full=True)
        head, tail, text = head + '"', '"', col
        if pc.any(pc.match_substring_regex(col, r'[\x00-\x1f"\\]')).as_py():
            # JSON escapes: the backslash first, then the quote. A control
            # character has its own escapes, so those strings go to Python.
            mask = pc.match_substring_regex(col, r"[\x00-\x1f]")
            text = pc.replace_substring(col, "\\", "\\\\")
            text = pc.replace_substring(text, '"', '\\"')
    elif pa.types.is_date32(t):
        days = col.cast(pa.int32())
        low, high = pc.min(days).as_py(), pc.max(days).as_py()
        if low is not None and (low < _DATE32_MIN or high > _DATE32_MAX):
            return None  # outside Python's dates: the Python path fails as before
        head, tail, text = head + '"', '"', pc.cast(col, pa.string())
    elif pa.types.is_float64(t):
        size = pc.abs(col)
        plain = pc.or_(
            pc.equal(size, 0.0),
            pc.and_(pc.greater_equal(size, _PLAIN_FLOAT_LOW), pc.less(size, _PLAIN_FLOAT_HIGH)),
        )
        mask = pc.invert(plain)  # NaN compares false, so it is not plain either
        # Arrow writes the shortest digits, as Python's repr does, but drops ".0".
        text = pc.cast(col, pa.string())
        text = pc.if_else(
            pc.match_substring(text, "."), text, pc.binary_join_element_wise(text, ".0", "")
        )
    elif pa.types.is_timestamp(t) and t.unit in ("s", "ms", "us"):
        # An aware timestamp holds UTC microseconds: read them as a naive one, which
        # is then the UTC wall time. (Arrow's strftime and time zone kernels are
        # hundreds of times slower on Windows; a plain cast to text is not.)
        micros = col.cast(pa.timestamp("us", tz=t.tz)).cast(pa.int64())
        wall = micros.cast(pa.timestamp("us"))
        # Arrow writes "yyyy-MM-dd HH:mm:ss.SSSSSS" for microseconds; outside years
        # 1000 to 9999 the year's width may differ, so Python writes those.
        mask = pc.or_(pc.less(micros, _TS_MIN), pc.greater_equal(micros, _TS_MAX))
        text = pc.replace_substring(pc.cast(wall, pa.string()), " ", "T", max_replacements=1)
        head, tail = head + '"', 'Z"' if t.tz else '"'
    else:
        return None

    text = pc.binary_join_element_wise(head, text, tail, "")
    if mask is not None:
        mask = mask.fill_null(False)
        if pc.any(mask).as_py():
            values = col.filter(mask).to_pylist()
            written = ["," + m for m in _members(name, kind, values) if m is not None]
            text = pc.replace_with_mask(text, mask, pa.array(written, pa.string()))
    return text


def _slice_lines(names: List[str], kinds: Dict[str, str], batch: Any) -> Optional[Any]:
    """One slice of rows as an Arrow string array of canonical lines.

    None when Arrow cannot build it (a slice too large for 32 bit offsets, an old
    pyarrow without a kernel): the caller then hashes the slice the Python way.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    try:
        members = []
        for i, name in enumerate(names):
            col = batch.column(i)
            kind = kinds.get(name, "")
            text = _arrow_members(name, kind, col)
            if text is None:
                written = _members(name, kind, col.to_pylist())
                text = pa.array([None if m is None else "," + m for m in written], pa.string())
            members.append(text)
        # A null member is left out. (Not null_handling="skip": pyarrow 25 drops
        # the rows where every input is null, so the array comes back shorter.)
        lines = pc.binary_join_element_wise(
            "{", *members, "}", "", null_handling="replace", null_replacement=""
        )
        # Every member starts with a comma: drop the one after the brace. A row
        # with no members is "{}", which holds no "{," to replace.
        lines = pc.replace_substring(lines, "{,", "{", max_replacements=1)
        if len(lines) != batch.num_rows or lines.null_count:
            return None
        return lines
    except pa.ArrowException:
        return None


def _pick_line_sha256() -> Callable[..., Any]:
    """CPython's own SHA-256 (HACL*, module ``_sha2``) when it behaves like hashlib's.

    It gives the same digest about a fifth faster on short lines. It is a private
    module, so it is used only if it imports, takes a memoryview slice as this
    module passes one, and returns hashlib's digest; anything else (missing,
    renamed, another signature) falls back to :func:`hashlib.sha256`.
    """
    try:
        from _sha2 import sha256 as builtin

        probe = memoryview(b"{rows-v1}")[1:-1]
        if builtin(probe).digest() == hashlib.sha256(probe).digest():
            return builtin
    except Exception:
        pass
    return hashlib.sha256


_line_sha256: Callable[..., Any] = _pick_line_sha256()


def _arrow_lanes(lines: Any) -> Tuple[int, int]:
    """The two lane sums of an Arrow string array of canonical lines.

    Each line is hashed straight from Arrow's buffer (no Python strings), and the
    lanes are summed by numpy, whose uint64 addition wraps modulo 2**64, the same
    as the Python sum masked at the end.
    """
    import numpy as np

    n = len(lines)
    buffers = lines.buffers()
    offsets = np.frombuffer(buffers[1], dtype=np.int32, count=n + 1, offset=lines.offset * 4)
    data = memoryview(buffers[2]) if buffers[2] is not None else memoryview(b"")
    sha = _line_sha256
    o = offsets.tolist()
    digests = b"".join([sha(data[a:b]).digest() for a, b in zip(o, o[1:])])
    words = np.frombuffer(digests, dtype=">u8").reshape(n, 4)
    a = words[:, 0].astype(np.uint64).sum(dtype=np.uint64)
    b = words[:, 1].astype(np.uint64).sum(dtype=np.uint64)
    return int(a), int(b)


def _slices(table: Any, names: List[str]) -> Iterator[Any]:
    for batch in table.select(names).to_batches():
        for start in range(0, batch.num_rows, _SLICE_ROWS):
            yield batch.slice(start, _SLICE_ROWS)


def _slice_lanes(names: List[str], kinds: Dict[str, str], piece: Any) -> Tuple[int, int]:
    """One slice's lane sums: from the lines Arrow builds, else the Python way."""
    lines = _slice_lines(names, kinds, piece)
    if lines is not None:
        try:
            return _arrow_lanes(lines)
        except ImportError:  # pragma: no cover  (no numpy)
            pass
    columns = [
        _members(name, kinds.get(name, ""), piece.column(i).to_pylist())
        for i, name in enumerate(names)
    ]
    return _python_lanes(columns)


#: The setting that caps the helper processes of the parallel hash (F-038).
#: ``1`` (or ``0``) hashes in the calling process only.
HASH_WORKERS_ENV = "UBUNYE_HASH_WORKERS"

#: Helpers used when the setting is not given: one per usable core, at most this
#: many. Each helper holds about 105 to 125 MB resident while it works (measured
#: on Windows in a venv, where a helper is the venv launcher plus Python).
_DEFAULT_MAX_WORKERS = 4

#: A table smaller than this is hashed in the calling process: a helper costs about
#: 0.5 s to start (Python, pyarrow, and Arrow's first cast), about what hashing
#: this many rows costs in one process.
_PARALLEL_MIN_ROWS = 500_000

#: Each helper gets at least this many rows.
_ROWS_PER_WORKER = 125_000

#: The stream's schema metadata: the caller's canonical kinds, the digest of the
#: caller's hash code, and its pyarrow version. A helper that differs in either
#: exits without an answer, so it can never hash with other rules.
_KINDS_KEY = b"ubunye.rows-v1.kinds"
_SOURCE_KEY = b"ubunye.rows-v1.source"
_PYARROW_KEY = b"ubunye.rows-v1.pyarrow"

#: Helper exit codes (0 is an answer on stdout).
_EXIT_FAILED, _EXIT_KINDS, _EXIT_SOURCE, _EXIT_PYARROW = 2, 3, 5, 6

#: How long helpers may take before they are stopped and this process hashes the
#: table itself: a floor, plus far more than one process needs per cell.
_DEADLINE_FLOOR_S = 60.0
_DEADLINE_PER_CELL_S = 20e-6

_log = logging.getLogger(__name__)


def _source_digest() -> Optional[str]:
    """The SHA-256 of this file's bytes, read once at import (None if unreadable)."""
    try:
        with open(os.path.abspath(__file__), "rb") as fh:
            return hashlib.sha256(fh.read()).hexdigest()
    except Exception:
        return None


_SOURCE_DIGEST = _source_digest()

#: Helpers alive in this process right now, across threads: concurrent hashes share
#: one budget instead of each starting a full set.
_budget_lock = threading.Lock()
_helpers_running = 0


def _cgroup_cpus(path: str = "/sys/fs/cgroup/cpu.max") -> Optional[int]:
    """The CPUs a cgroup v2 quota allows (``"200000 100000"`` is 2), or None."""
    try:
        with open(path, encoding="ascii") as fh:
            quota, period = fh.read().split()[:2]
        if quota == "max":
            return None
        return max(1, math.ceil(int(quota) / int(period)))
    except Exception:
        return None


def _cores() -> int:
    """The CPUs this process may use: affinity, then a container's CPU quota."""
    counter = getattr(os, "process_cpu_count", None)  # Python 3.13+
    affinity = getattr(os, "sched_getaffinity", None)
    count: Optional[int] = None
    try:
        if counter is not None:
            count = counter()
        elif affinity is not None:
            count = len(affinity(0))
    except OSError:
        count = None
    count = count or os.cpu_count() or 1
    quota = _cgroup_cpus()
    return max(1, min(count, quota) if quota else count)


def _helper_cap() -> int:
    """The most helpers this process runs at once, across every hash in it.

    The setting (or 4), never more than the usable cores. A setting that is not a
    number never starts processes.
    """
    raw = os.environ.get(HASH_WORKERS_ENV, "").strip()
    if raw:
        try:
            wanted = int(raw)
        except ValueError:
            return 1
    else:
        wanted = _DEFAULT_MAX_WORKERS
    return max(1, min(wanted, _cores()))


def _worker_count(rows: int) -> int:
    """How many helper processes hash a table of ``rows`` rows (1 means none)."""
    if rows < _PARALLEL_MIN_ROWS:
        return 1
    return max(1, min(_helper_cap(), rows // _ROWS_PER_WORKER))


def _reserve(wanted: int, cap: int) -> int:
    """Take up to ``wanted`` helpers from the process wide budget of ``cap``."""
    global _helpers_running
    with _budget_lock:
        granted = max(0, min(wanted, cap - _helpers_running))
        if granted < 2:
            return 0
        _helpers_running += granted
        return granted


def _release(granted: int) -> None:
    global _helpers_running
    with _budget_lock:
        _helpers_running -= granted


def _python_executable() -> Optional[str]:
    """``sys.executable`` if it is a Python that can run ``-c``, else None.

    A frozen app (PyInstaller and the like) or an embedding host reports its own
    program there; started with ``-c`` it would run the app again, not the helper.
    """
    if getattr(sys, "frozen", False):
        return None
    exe = sys.executable or ""
    name = os.path.basename(exe).lower()
    if name.endswith(".exe"):
        name = name[:-4]
    if not re.fullmatch(r"(python|pypy)[0-9.]*t?w?(_d)?", name) or not os.path.isfile(exe):
        return None
    return exe


def _plain_type(t: Any) -> bool:
    """False for a type the IPC round trip might not bring back the same (extensions)."""
    import pyarrow as pa

    if isinstance(t, pa.BaseExtensionType):
        return False
    return all(_plain_type(t.field(i).type) for i in range(t.num_fields))


#: How a helper starts: drop the working folder from the path (a stray module there
#: must not shadow the standard library), load this file by its path, run it.
_WORKER_BOOT = (
    "import sys\n"
    "if not getattr(sys.flags, 'safe_path', False) and sys.path and sys.path[0] == '':\n"
    "    del sys.path[0]\n"
    "import importlib.util as u\n"
    "s = u.spec_from_file_location('_ubunye_rows_v1', sys.argv[1])\n"
    "m = u.module_from_spec(s)\n"
    "sys.modules[s.name] = m\n"
    "s.loader.exec_module(m)\n"
    "sys.exit(m._worker_main())\n"
)


def _worker_main() -> int:
    """A helper's work: an Arrow IPC stream on stdin, ``"<a> <b>"`` on stdout.

    The stream holds the columns sorted by name and, in its schema metadata, the
    caller's canonical kinds, code digest and pyarrow version. Any failure or
    difference exits non zero with nothing on stdout; the caller then hashes the
    table itself, so a digest never depends on whether the helpers ran.
    """
    try:
        import pyarrow as pa

        pa.set_cpu_count(1)  # one helper per core already
        reader = pa.ipc.open_stream(sys.stdin.buffer)
        meta = reader.schema.metadata or {}
        if _SOURCE_DIGEST is None or meta.get(_SOURCE_KEY) != _SOURCE_DIGEST.encode("ascii"):
            return _EXIT_SOURCE  # this file is not the code the caller runs
        if meta.get(_PYARROW_KEY) != pa.__version__.encode("ascii"):
            return _EXIT_PYARROW
        kinds = dict(json.loads(meta[_KINDS_KEY].decode("utf-8")))
        names = list(reader.schema.names)
        # The kinds must be the caller's, or the lines (and the digest) could differ.
        if any(arrow_kind(f.type) != kinds.get(f.name) for f in reader.schema):
            return _EXIT_KINDS
        total_a = total_b = 0
        for batch in reader:
            for start in range(0, batch.num_rows, _SLICE_ROWS):
                a, b = _slice_lanes(names, kinds, batch.slice(start, _SLICE_ROWS))
                total_a += a
                total_b += b
        out = sys.stdout.buffer
        out.write(f"{total_a & _MASK} {total_b & _MASK}\n".encode("ascii"))
        out.flush()
        return 0
    except BaseException:  # noqa: BLE001  (the caller falls back; say nothing)
        return _EXIT_FAILED


def _helper_env() -> Dict[str, str]:
    """The caller's environment, made safe for a helper.

    A helper never starts helpers, never stops in an interactive prompt, and does
    not turn a warning into an error.
    """
    env = dict(os.environ)
    env[HASH_WORKERS_ENV] = "1"
    env["PYTHONWARNINGS"] = "ignore"
    for key in ("PYTHONINSPECT", "PYTHONSTARTUP"):
        env.pop(key, None)
    return env


def _deadline(rows: int, columns: int) -> float:
    return _DEADLINE_FLOOR_S + rows * max(1, columns) * _DEADLINE_PER_CELL_S


def _parallel_lanes(
    table: Any, names: List[str], schema: List[Tuple[str, str]], workers: int
) -> Optional[Tuple[int, int]]:
    """The lane sums from up to ``workers`` helper processes, or None.

    The rows are cut into one run per helper, each streamed to it as Arrow IPC. A
    helper is a fresh Python that loads this file alone (not the ``ubunye``
    package, so it starts in about 0.15 s) and runs :func:`_worker_main`: it
    hashes every row of its run exactly as this process would and sends back two
    numbers. Their sums are the table's, since addition does not care how rows
    are grouped. Processes, not threads: the SHA-256 of a short line holds the GIL.
    Fresh processes, not ``multiprocessing``: that would import the caller's
    script again in every helper on Windows and macOS.

    None (the caller then hashes the table itself) when the helpers cannot be
    used, when any of them fails, or when they pass the deadline. Every helper is
    stopped and every pipe closed before this returns, including on Ctrl+C.
    """
    exe = _python_executable()
    if exe is None or _SOURCE_DIGEST is None:
        _log.debug("rows-v1: no helpers (no Python interpreter to start); hashing here")
        return None
    granted = _reserve(workers, _helper_cap())
    if not granted:
        _log.debug("rows-v1: helper budget in use by another hash; hashing here")
        return None
    try:
        return _run_helpers(exe, table, names, schema, granted)
    finally:
        _release(granted)


def _run_helpers(
    exe: str, table: Any, names: List[str], schema: List[Tuple[str, str]], workers: int
) -> Optional[Tuple[int, int]]:
    import subprocess

    import pyarrow as pa

    sub = table.select(names)
    meta = {
        _KINDS_KEY: json.dumps(sorted([list(p) for p in schema])).encode("utf-8"),
        _SOURCE_KEY: (_SOURCE_DIGEST or "").encode("ascii"),
        _PYARROW_KEY: pa.__version__.encode("ascii"),
    }
    sub = sub.replace_schema_metadata(meta)
    command = [exe, "-c", _WORKER_BOOT, os.path.abspath(__file__)]
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    env = _helper_env()
    rows = sub.num_rows
    step = -(-rows // workers)
    outs: List[bytearray] = [bytearray() for _ in range(workers)]
    procs: List[Any] = []
    threads: List[threading.Thread] = []
    reason = ""

    def feed(proc: Any, part: Any) -> None:
        try:
            with pa.ipc.new_stream(proc.stdin, part.schema) as writer:
                for batch in part.to_batches(max_chunksize=_SLICE_ROWS):
                    writer.write_batch(batch)
        except BaseException:  # noqa: BLE001  (a dead helper; its exit code says so)
            pass
        finally:
            _close(proc.stdin)

    def drain(proc: Any, out: bytearray) -> None:
        try:
            while True:
                chunk = proc.stdout.read1(65536)
                if not chunk:
                    return
                if len(out) < 256:  # an answer is two numbers; never hold an echo
                    out.extend(chunk[:256])
        except BaseException:  # noqa: BLE001
            pass

    try:
        for i in range(workers):
            proc = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                env=env,
                creationflags=flags,
            )
            procs.append(proc)
            part = sub.slice(i * step, step)
            threads.append(threading.Thread(target=feed, args=(proc, part), daemon=True))
            threads.append(threading.Thread(target=drain, args=(proc, outs[i]), daemon=True))
        for t in threads:
            t.start()
        # Short waits, so Ctrl+C reaches this thread; a deadline, so a helper that
        # never answers cannot hold the run.
        stop = time.monotonic() + _deadline(rows, len(names))
        for proc in procs:
            while proc.poll() is None:
                if time.monotonic() > stop:
                    reason = "deadline passed"
                    return None
                time.sleep(0.05)
        for t in threads:
            t.join(timeout=5)
        codes = [p.returncode for p in procs]
        if any(codes):
            reason = f"exit codes {codes}"
            return None
        sums = []
        for out in outs:
            parts = bytes(out).split()
            if len(parts) != 2:
                reason = "an answer that is not two numbers"
                return None
            sums.append((int(parts[0]), int(parts[1])))
        return sum(s[0] for s in sums), sum(s[1] for s in sums)
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        return None
    finally:
        for proc in procs:
            _stop(proc)
        for t in threads:
            t.join(timeout=1)
        if reason:
            _log.debug("rows-v1: helpers failed (%s); hashing here", reason)


def _close(stream: Any) -> None:
    try:
        if stream is not None:
            stream.close()
    except BaseException:  # noqa: BLE001  (a broken pipe on close says nothing new)
        pass


def _stop(proc: Any) -> None:
    """Kill a helper if it still runs, reap it, and close its pipes."""
    try:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)
    except BaseException:  # noqa: BLE001
        pass
    _close(proc.stdin)
    _close(proc.stdout)


def fingerprint_arrow(table: Any) -> Fingerprint:
    """The ``rows-v1`` fingerprint of an Arrow table.

    Rows are taken in slices. Each slice's canonical lines are built by Arrow
    compute (:func:`_slice_lines`) and hashed from Arrow's buffer; a slice Arrow
    cannot build is written and hashed the Python way. A large table is hashed by
    helper processes, one per core (:func:`_parallel_lanes`, capped by
    ``UBUNYE_HASH_WORKERS``); if any helper fails, this process hashes it all. A
    test holds the result to :func:`fingerprint_rows`, the row at a time
    reference, byte for byte.
    """
    schema = [(f.name, arrow_kind(f.type)) for f in table.schema]
    kinds = dict(schema)
    names = sorted(table.column_names)
    total_a = total_b = 0
    # A table with no columns has always summed to zero here (no members to zip);
    # kept, so no recorded digest moves.
    if names:
        sums = None
        workers = _worker_count(table.num_rows)
        if workers > 1 and all(_plain_type(f.type) for f in table.schema):
            sums = _parallel_lanes(table, names, schema, workers)
        if sums is not None:
            total_a, total_b = sums
        else:
            for piece in _slices(table, names):
                a, b = _slice_lanes(names, kinds, piece)
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


def _contract_kind(t: Any) -> str:
    """An Arrow column type by its real name: a category is named by its values."""
    import pyarrow as pa

    if pa.types.is_dictionary(t):
        return _contract_kind(t.value_type)
    return arrow_kind(t)


def frame_kinds(frame: Any) -> Dict[str, str]:
    """Each column's type as an input contract names it (``columns`` rule).

    The run record's names (ADR 006), with one difference: a timestamp is named
    by what it is, on every backend and at every depth. With a zone it is
    ``timestamp``, without one ``timestamp_ntz``, as Spark 3.4 and later name
    them. The record itself writes every pandas timestamp as an instant
    (``timestamp``); that is unchanged, so its schema hashes stay the same.

    Read from the schema: a Spark frame is not computed. A pandas column of
    Python objects is inferred by Arrow; one holding values of mixed types is
    named ``mixed``. A column of nulls only is ``null``.
    """
    package = _package(frame)
    if package == "pyspark":
        from ubunye.adapters.spark.content_hash import canonical_schema

        return dict(canonical_schema(frame))

    from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter

    if isinstance(frame, PandasDataFrameAdapter):
        frame, package = frame.native, "pandas"
    if package == "pyarrow":
        return {f.name: _contract_kind(f.type) for f in frame.schema}
    if package == "pandas":
        import pyarrow as pa

        if any(name is not None for name in frame.index.names):
            frame = frame.reset_index()
        kinds: Dict[str, str] = {}
        for column in frame.columns:
            try:
                field = pa.Schema.from_pandas(frame[[column]], preserve_index=False)[0]
                kinds[str(column)] = _contract_kind(field.type)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                kinds[str(column)] = "mixed"
        return kinds
    port_schema = getattr(frame, "schema", {}) or {}
    return {str(k): str(v) for k, v in dict(port_schema).items()}


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
