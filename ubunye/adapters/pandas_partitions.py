"""Spark's hive style partition folders, written and read without Spark (F-012).

``partitionBy: [dt]`` makes Spark write ``out/dt=2024-01-02/part-....parquet``.
This module holds the rules for those folders, ported from Spark 4.2's own
source (``ExternalCatalogUtils``, ``PartitioningUtils``, ``FileFormatWriter``)
and checked against live Spark, so the pandas backend writes the folders Spark
writes and reads them back with the types Spark infers.

Writing:

* One folder level per partition column, in ``partitionBy`` order:
  ``escape(name)=escape(value)``. A null or empty value is
  ``__HIVE_DEFAULT_PARTITION__``.
* ``escape`` turns control characters and ``" # % ' * / : = ? \\ { [ ] ^`` and
  DEL into ``%`` plus two upper case hex digits; on Windows also space
  ``< > |`` (Spark does the same on the same host).
* The partition columns are left out of the data files.
* Values are Spark's text for the type: ``true``, ``2024-01-02``, and a
  timestamp as wall clock time in the session zone (``2024-01-02 10:05:06.5``).

Reading: every folder that holds a data file is parsed from the bottom up
(``name=value`` segments), each value is given the narrowest type Spark would
infer (int, bigint, decimal, double, timestamp, date, else text), the types of a
column are widened across folders as Spark widens them, and the partition
columns come after the data columns.

What Spark itself does not read back as the same type (double, decimal, binary,
time) is refused on write, with the reason; so is anything Spark refuses.
"""

from __future__ import annotations

import datetime as dt
import decimal
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ubunye.core.errors import SinkWriteError, SourceReadError

logger = logging.getLogger(__name__)

#: The folder name Spark gives a null (or empty) value.
HIVE_DEFAULT = "__HIVE_DEFAULT_PARTITION__"

# ExternalCatalogUtils.charToEscape: 0x01 to 0x1F, the characters below, and DEL.
_ESCAPED = frozenset(chr(c) for c in range(1, 0x20)) | frozenset("\"#%'*/:=?\\\x7f{[]^")
# Only when the JVM runs on Windows (Shell.WINDOWS).
_ESCAPED_ON_WINDOWS = frozenset(" <>|")
_HEX_ESCAPE = re.compile(r"%([0-9A-Fa-f]{2})")


def _windows() -> bool:
    return os.name == "nt"


def escape(text: str, windows: Optional[bool] = None) -> str:
    """Spark's ``escapePathName``: the characters a folder name cannot hold, as ``%XX``."""
    extra = _ESCAPED_ON_WINDOWS if (_windows() if windows is None else windows) else frozenset()
    return "".join(f"%{ord(c):02X}" if (c in _ESCAPED or c in extra) else c for c in text)


def unescape(text: str) -> str:
    """Spark's ``unescapePathName``: every ``%XX`` (either case) back to its character.

    A ``%`` not followed by two hex digits stays as it is.
    """
    return _HEX_ESCAPE.sub(lambda m: chr(int(m.group(1), 16)), text)


def _segment(name: str, value: Optional[str]) -> str:
    return f"{escape(name)}={HIVE_DEFAULT if not value else escape(value)}"


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #


def _refuse(message: str, hint: str, **context: Any) -> SinkWriteError:
    return SinkWriteError(message, context={"Backend": "pandas", **context}, hint=hint)


_CAST_HINT = (
    "Cast the column to a string (or to a date) in the transform before writing, "
    "or partition by another column."
)


def _type_problem(name: str, t: Any) -> Optional[SinkWriteError]:
    """Why a column of Arrow type ``t`` cannot be a partition column here, or None."""
    import pyarrow as pa

    if (
        pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or pa.types.is_integer(t)
        or pa.types.is_boolean(t)
        or pa.types.is_date(t)
        or pa.types.is_timestamp(t)
    ):
        return None
    if pa.types.is_nested(t):
        return _refuse(
            f'Cannot use "{t}" for partition column `{name}`: Spark refuses nested '
            "partition columns too (INVALID_PARTITION_COLUMN_DATA_TYPE).",
            "Partition by a plain column, such as a date or a string.",
            column=name,
        )
    return _refuse(
        f"The pandas backend does not partition by `{name}` of type {t}. Spark writes "
        "such folders, but does not read them back as the same type: a double comes "
        "back as text or a decimal (1.5 and 1.0E7 in one column), a decimal as a "
        "double, binary as text.",
        _CAST_HINT,
        column=name,
        type=str(t),
    )


def resolve_columns(names: Sequence[str], partition_by: Sequence[str]) -> List[str]:
    """The frame's own column for each ``partitionBy`` entry (matched ignoring case).

    Raises what Spark raises: a column named twice, a missing column, or every
    column a partition column.
    """
    seen: Dict[str, str] = {}
    for col in partition_by:
        key = str(col).lower()
        if key in seen:
            raise _refuse(
                f"The column `{col}` already exists (partitionBy names it twice, "
                "ignoring case; Spark: COLUMN_ALREADY_EXISTS).",
                "Name each partition column once.",
                partition_by=list(partition_by),
            )
        seen[key] = str(col)
    by_lower = {}
    for n in names:
        by_lower.setdefault(str(n).lower(), str(n))
    actual = []
    for col in partition_by:
        found = by_lower.get(str(col).lower())
        if found is None:
            raise _refuse(
                f"Partition column `{col}` not found in the frame's columns {list(names)}.",
                "Check the spelling in partitionBy, or add the column in the transform.",
                partition_by=list(partition_by),
            )
        actual.append(found)
    if len(actual) == len(names):
        raise _refuse(
            "Cannot use all columns for partition columns (Spark: "
            "ALL_PARTITION_COLUMNS_NOT_ALLOWED).",
            "Keep at least one column out of partitionBy: it is what the data files hold.",
            partition_by=list(partition_by),
        )
    return actual


def _render(values: Any, timezone: str) -> List[Optional[str]]:
    """Spark's text for each value (``CAST(value AS STRING)`` in the session zone).

    Null and the empty string are both None: Spark writes both as
    ``__HIVE_DEFAULT_PARTITION__``.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    t = values.type
    if pa.types.is_timestamp(t):
        local = values.cast(pa.timestamp("us", tz="UTC")).cast(pa.timestamp("us", tz=timezone))
        parts = [
            pc.year(local).to_pylist(),
            pc.month(local).to_pylist(),
            pc.day(local).to_pylist(),
            pc.hour(local).to_pylist(),
            pc.minute(local).to_pylist(),
            pc.second(local).to_pylist(),
            pc.millisecond(local).to_pylist(),
            pc.microsecond(local).to_pylist(),
        ]
        out: List[Optional[str]] = []
        for y, mo, d, h, mi, s, ms, us in zip(*parts):
            if y is None:
                out.append(None)
                continue
            text = f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}"
            fraction = ms * 1000 + us
            if fraction:
                text += "." + f"{fraction:06d}".rstrip("0")
            out.append(text)
        return out
    result: List[Optional[str]] = []
    for v in values.to_pylist():
        if v is None:
            result.append(None)
        elif isinstance(v, bool):
            result.append("true" if v else "false")
        elif isinstance(v, dt.date):
            result.append(f"{v.year:04d}-{v.month:02d}-{v.day:02d}")
        else:
            text = str(v)
            result.append(text or None)
    return result


@dataclass
class Split:
    """A frame cut into Spark's partition folders."""

    data: Any
    """The frame without its partition columns (what the data files hold)."""
    leaves: List[Tuple[str, Any]]
    """(relative folder, row indices) for each leaf folder, sorted by folder."""


def split(table: Any, partition_by: Sequence[str], timezone: str) -> Split:
    """Cut an Arrow table into the leaf folders Spark would write for ``partition_by``."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc

    actual = resolve_columns(table.column_names, partition_by)
    for name in actual:
        problem = _type_problem(name, table.schema.field(name).type)
        if problem is not None:
            raise problem

    codes, texts = [], []
    for name, spelled in zip(actual, partition_by):
        col = table.column(name).combine_chunks()
        encoded = pc.dictionary_encode(col, null_encoding="encode")
        raw = encoded.dictionary.to_pylist()
        rendered = _render(encoded.dictionary, timezone)
        for value in rendered:
            _check_value(name, value)
        _refuse_shared_folders(spelled, raw, rendered)
        distinct: Dict[Optional[str], int] = {}
        mapping = [distinct.setdefault(v, len(distinct)) for v in rendered]
        codes.append(pc.take(pa.array(mapping, pa.int32()), encoded.indices))
        texts.append(list(distinct))

    keys = [f"k{i}" for i in range(len(actual))]
    grouped = (
        pa.table(dict(zip(keys, codes), row=pa.array(np.arange(table.num_rows, dtype=np.int64))))
        .group_by(keys)
        .aggregate([("row", "list")])
    )
    leaves: Dict[str, Any] = {}
    folded: Dict[str, str] = {}
    key_columns = [grouped.column(k).to_pylist() for k in keys]
    rows = grouped.column("row_list")
    for g in range(grouped.num_rows):
        values = tuple(texts[i][key_columns[i][g]] for i in range(len(keys)))
        segments = [_segment(n, v) for n, v in zip(partition_by, values)]
        # Every level, compared ignoring case on every system: on a disk that
        # ignores case (Windows, macOS by default) p=A and p=a are one folder, and
        # the second write would land on the first one's file.
        for depth in range(1, len(segments) + 1):
            prefix = "/".join(segments[:depth])
            seen = folded.setdefault(prefix.casefold(), prefix)
            if seen != prefix:
                raise _refuse(
                    f"The folders {seen} and {prefix} differ only in case. On a disk "
                    "that ignores case (Windows, macOS by default) they are one folder, "
                    "and one write would replace the other's file; Spark's write fails "
                    "there too.",
                    "Make the values differ by more than case (for example lower case them).",
                    folder=prefix,
                )
        leaves[os.path.join(*segments)] = rows[g].values
    data = table.drop_columns(actual)
    return Split(data=data, leaves=[(p, leaves[p]) for p in sorted(leaves)])


def _refuse_shared_folders(name: str, raw: List[Any], rendered: List[Optional[str]]) -> None:
    """Refuse when one folder name would hold two different values of a column.

    Spark sorts rows by value, so two values that render to the same text (a null
    and the text ``__HIVE_DEFAULT_PARTITION__``; two instants an hour apart that
    read the same on the wall clock when daylight saving ends) are two partitions
    with one path, and its write fails (FileAlreadyExistsException). Writing them
    to one folder would silently change a value, so the pandas backend refuses
    too. A null and an empty string are one value to Spark, and are allowed.
    """
    groups: Dict[str, List[Any]] = {}
    for value, text in zip(raw, rendered):
        folder = _segment(name, text)
        groups.setdefault(folder, [])
        if value is None or value == "":
            value = None  # Spark turns "" into null before it sorts
        if value not in groups[folder]:
            groups[folder].append(value)
    for folder, values in groups.items():
        if len(values) > 1:
            shown = ", ".join("null" if v is None else repr(v) for v in values[:3])
            raise _refuse(
                f"Different values of `{name}` ({shown}) map to the one folder {folder}. "
                "Spark's write fails on this frame too (it names both the same file); "
                "writing both into one folder would change a value without a word.",
                "Change the values in the transform so each has its own folder (for a "
                "timestamp in a daylight saving change, partition by a UTC text or a date).",
                folder=folder,
            )


def check_existing(root: str, leaves: Sequence[str]) -> None:
    """Refuse leaf folders that match a folder already in ``root`` only ignoring case.

    For a write that keeps what is there (append, overwrite_partitions): on a disk
    that ignores case ``p=A`` would land in the existing ``p=a``, and on one that
    does not, the table would then hold two folders Spark treats as one column
    value each. Checked before anything is written.
    """
    listings: Dict[str, Dict[str, str]] = {}
    for leaf in leaves:
        parent = root
        for seg in leaf.split(os.sep):
            if parent not in listings:
                try:
                    names = os.listdir(parent)
                except OSError:
                    names = []
                listings[parent] = {n.casefold(): n for n in names}
            there = listings[parent].get(seg.casefold())
            if there is not None and there != seg:
                raise _refuse(
                    f"{os.path.join(parent, there)} already exists, and the new data's "
                    f"folder {seg} differs from it only in case. On a disk that ignores "
                    "case they are one folder; on one that does not, two.",
                    "Write the values with the same case as the existing folders.",
                    folder=os.path.join(parent, seg),
                )
            parent = os.path.join(parent, seg)


def _check_value(name: str, value: Optional[str]) -> None:
    """Values the file system cannot hold as a folder name, refused before any write."""
    if value is None:
        return
    if "\x00" in value:
        raise _refuse(
            f"A value of partition column `{name}` holds a NUL character, which no folder "
            "name can hold (Spark's write fails on it too).",
            "Remove the character in the transform.",
            column=name,
        )
    if _windows() and value.endswith("."):
        raise _refuse(
            f"The value {value!r} of partition column `{name}` ends in '.', which Windows "
            "drops from a folder name, so it would be read back as a different value "
            "(Spark on Windows writes the wrong folder the same way).",
            "Change the value in the transform, or write on Linux or macOS.",
            column=name,
        )


# --------------------------------------------------------------------------- #
# Reading: partition discovery and type inference (PartitioningUtils)
# --------------------------------------------------------------------------- #


def _skipped(name: str) -> bool:
    """Spark's ``shouldFilterOutPathName``: names a listing never returns."""
    if name.startswith(("_common_metadata", "_metadata")):
        return False
    return (
        (name.startswith("_") and "=" not in name)
        or name.startswith(".")
        or name.endswith("._COPYING_")
    )


def _is_data(name: str) -> bool:
    """Spark's ``isDataPath``: a file that counts as data (not ``_metadata`` either)."""
    return not ((name.startswith("_") and "=" not in name) or name.startswith("."))


@dataclass
class Layout:
    """What a folder holds: its data files, and each file's partition values."""

    files: List[str]
    columns: List[str] = field(default_factory=list)
    types: List[Any] = field(default_factory=list)
    values: List[Tuple[Any, ...]] = field(default_factory=list)
    """For each file, its partition values (already typed)."""


def _read_error(message: str, root: str, hint: str) -> SourceReadError:
    return SourceReadError(message, context={"Backend": "pandas", "path": root}, hint=hint)


def _leaf_dirs(root: str) -> Dict[str, List[str]]:
    """Every folder under ``root`` that holds a data file, with its non empty data files.

    Relative folder -> sorted file names. A folder whose data files are all
    empty is kept (Spark still finds its partition) with no files to read.
    """
    found: Dict[str, List[str]] = {}
    for here, dirs, names in os.walk(root):
        dirs[:] = sorted(d for d in dirs if not _skipped(d))
        data = [n for n in names if not _skipped(n) and _is_data(n)]
        if not data:
            continue
        rel = os.path.relpath(here, root)
        rel = "" if rel == "." else rel
        found[rel] = sorted(n for n in data if os.path.getsize(os.path.join(here, n)) > 0)
    return found


def _parse(rel: str) -> Tuple[List[Tuple[str, str]], str]:
    """Spark's ``parsePartition``: (name, raw value) pairs and the base folder.

    Walks up from the leaf; a segment with no ``=`` ends the walk once a
    partition segment was found.
    """
    parts = rel.split(os.sep) if rel else []
    columns: List[Tuple[str, str]] = []
    i = len(parts) - 1
    while i >= 0:
        seg = parts[i]
        eq = seg.find("=")
        if eq == -1:
            if columns:
                break
        else:
            name, raw = unescape(seg[:eq]), seg[eq + 1 :]
            if not name or not raw:
                raise ValueError(seg)
            columns.append((name, raw))
        i -= 1
    if not columns:
        return [], rel
    base = os.sep.join(parts[: i + 1])
    return list(reversed(columns)), base


# Inferred kinds: ("null",), ("int",), ("long",), ("decimal", precision), ("double",),
# ("timestamp",), ("date",), ("string",).
_INT = re.compile(r"[+-]?\d+")
_BIG_DECIMAL = re.compile(r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")
_JAVA_DOUBLE = re.compile(
    r"[+-]?(NaN|Infinity|((\d+\.?\d*|\.\d+)([eE][+-]?\d+)?)[fFdD]?"
    r"|0[xX]([0-9a-fA-F]+\.?[0-9a-fA-F]*|\.[0-9a-fA-F]+)[pP][+-]?\d+[fFdD]?)",
    re.ASCII,
)
_TIMESTAMP = re.compile(r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d))?", re.ASCII)
_DATE = re.compile(r"(\d{4})-(\d{2})-(\d{2})", re.ASCII)
_JAVA_TRIM = "".join(chr(c) for c in range(0x21))
_INT32 = (-(2**31), 2**31 - 1)
_INT64 = (-(2**63), 2**63 - 1)


def _java_double(raw: str) -> Optional[float]:
    """``Double.parseDouble`` or None: Java's grammar, not Python's."""
    text = raw.strip(_JAVA_TRIM)
    if not _JAVA_DOUBLE.fullmatch(text):
        return None
    body = text.rstrip("fFdD") if not text.endswith(("NaN", "Infinity")) else text
    sign = -1.0 if body.startswith("-") else 1.0
    bare = body.lstrip("+-")
    if bare == "NaN":
        return float("nan")
    if bare == "Infinity":
        return sign * float("inf")
    if bare[:2] in ("0x", "0X"):
        return sign * float.fromhex(bare)
    return float(body)


def _whole_decimal(raw: str) -> Optional[int]:
    """``new BigDecimal(raw)`` with scale <= 0 and precision <= 38, as an int, or None."""
    if not _BIG_DECIMAL.fullmatch(raw):
        return None
    try:
        value = decimal.Decimal(raw)
    except decimal.InvalidOperation:
        return None
    if value.as_tuple().exponent < 0:  # a scale above 0: 1.5, 1.0, 0.00
        return None
    whole = int(value)
    return whole if len(str(abs(whole))) <= 38 else None


def _timestamp_text(text: str) -> Optional[dt.datetime]:
    m = _TIMESTAMP.fullmatch(text)
    if not m:
        return None
    y, mo, d, h, mi, s, f = m.groups()
    try:
        return dt.datetime(int(y), int(mo), int(d), int(h), int(mi), int(s), int(f or 0) * 100000)
    except ValueError:
        return None


def _date_text(text: str) -> Optional[dt.date]:
    m = _DATE.fullmatch(text)
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def infer(raw: str) -> Tuple[Any, ...]:
    """Spark's ``inferPartitionColumnValue`` for one folder's raw (escaped) value."""
    if _INT.fullmatch(raw):
        n = int(raw)
        if _INT32[0] <= n <= _INT32[1]:
            return ("int",)
        if _INT64[0] <= n <= _INT64[1]:
            return ("long",)
    whole = _whole_decimal(raw)
    if whole is not None:
        return ("decimal", max(1, len(str(abs(whole)))))
    if _java_double(raw) is not None:
        return ("double",)
    if _timestamp_text(unescape(raw)) is not None:
        return ("timestamp",)
    if _date_text(raw) is not None:
        return ("date",)
    # Spark 4.2 also infers `time` (HH:mm:ss); pandas has no such column type, so
    # it stays text here.
    return ("null",) if raw == HIVE_DEFAULT else ("string",)


def wider(a: Tuple[Any, ...], b: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Spark's ``findWiderTypeForPartitionColumn``: anything unlisted becomes text."""
    if a == b:
        return a
    if a[0] == "null":
        return b
    if b[0] == "null":
        return a
    kinds = {a[0], b[0]}
    if kinds == {"int", "long"}:
        return ("long",)
    if kinds == {"int", "double"}:
        return ("double",)
    if kinds <= {"int", "long", "decimal"}:
        width = {"int": 10, "long": 20}
        pa_, pb = (t[1] if t[0] == "decimal" else width[t[0]] for t in (a, b))
        return ("decimal", min(38, max(pa_, pb)))
    if kinds == {"date", "timestamp"}:
        return ("timestamp",)
    return ("string",)


def _arrow_type(kind: Tuple[Any, ...]) -> Any:
    import pyarrow as pa

    return {
        "int": pa.int32(),
        "long": pa.int64(),
        "double": pa.float64(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
        # Spark calls an all null column `void`; the nearest usable pandas column
        # is text that is all null.
        "null": pa.string(),
        "string": pa.string(),
    }.get(kind[0]) or pa.decimal128(kind[1], 0)


def _instant(local: dt.datetime, timezone: str) -> dt.datetime:
    """Wall clock time in ``timezone`` as a UTC instant (a gap moves forward, as in Java)."""
    from zoneinfo import ZoneInfo

    return local.replace(tzinfo=ZoneInfo(timezone)).astimezone(dt.timezone.utc)


def value_of(kind: Tuple[Any, ...], raw: str, timezone: str) -> Any:
    """Spark's ``castPartValueToDesiredType``: one folder's value, typed."""
    if raw == HIVE_DEFAULT or kind[0] == "null":
        return None
    k = kind[0]
    if k == "string":
        return unescape(raw)
    if k in ("int", "long"):
        return int(raw)
    if k == "decimal":
        return decimal.Decimal(int(decimal.Decimal(raw)))
    if k == "double":
        return _java_double(raw)
    if k == "date":
        return _date_text(raw)
    stamp = _timestamp_text(unescape(raw))
    if stamp is None:  # a date in a timestamp column: midnight in the session zone
        day = _date_text(raw)
        stamp = dt.datetime(day.year, day.month, day.day)
    return _instant(stamp, timezone)


def discover(root: str, timezone: str, given: Sequence[str] = ()) -> Layout:
    """The files under a folder and their partition values, as Spark finds them.

    A folder with no ``name=value`` folders is not partitioned: the data files
    directly in it are read, as before (and as Spark reads them). Raises what Spark raises for a folder it cannot make sense
    of (partition columns that differ between folders, a plain folder beside
    partition folders). A column named in ``given`` (a user schema) is not
    inferred: its values stay text, for the caller to cast to the schema's type.
    """
    given_lower = {g.lower() for g in given}
    leaves = _leaf_dirs(root)
    parsed: Dict[str, List[Tuple[str, str]]] = {}
    bases = set()
    for rel in sorted(leaves):
        try:
            columns, base = _parse(rel)
        except ValueError as exc:
            raise _read_error(
                f"The partition folder '{exc}' under {root} has an empty column name or "
                "value (Spark: EMPTY_PARTITION_COLUMN_VALUE).",
                root,
                "On Windows a value ending in '.' loses the '.'. Rename or remove the folder.",
            ) from exc
        bases.add(base.lower())
        if columns:
            parsed[rel] = columns

    def files_in(rels: Sequence[str]) -> List[str]:
        return [os.path.join(root, r, n) for r in rels for n in leaves[r]]

    if not parsed:
        # Not partitioned: Spark reads the files directly in the folder only
        # (PartitioningAwareFileIndex.allFiles), never those in plain sub folders.
        return Layout(files=files_in([""] if "" in leaves else []))
    if len(bases) != 1:
        raise _read_error(
            f"Conflicting folder layout under {root}: partition folders (name=value) and "
            f"plain folders side by side (Spark: CONFLICTING_DIRECTORY_STRUCTURES). "
            f"Base folders found: {sorted(bases)}.",
            root,
            "Read the partitioned folder itself, or move the other folder out.",
        )
    names_of = {rel: [n.lower() for n, _ in cols] for rel, cols in parsed.items()}
    if len({tuple(v) for v in names_of.values()}) != 1:
        lists = sorted({", ".join(n for n, _ in cols) for cols in parsed.values()})
        raise _read_error(
            f"Conflicting partition column names under {root}: {lists} (Spark: "
            "CONFLICTING_PARTITION_COLUMN_NAMES).",
            root,
            "Every data folder must have the same partition columns, in the same order.",
        )
    dropped = [r for r in leaves if r not in parsed and leaves[r]]
    if dropped:
        logger.warning(
            "%s is partitioned, so the data files outside its partition folders are not "
            "read (Spark drops them too, silently): %s",
            root,
            files_in(sorted(dropped)),
        )
    rels = sorted(parsed)
    names = [n for n, _ in parsed[rels[0]]]
    kinds = []
    for i, name in enumerate(names):
        kind: Tuple[Any, ...] = ("string",) if name.lower() in given_lower else ("null",)
        if kind[0] == "null":
            for rel in rels:
                kind = wider(kind, infer(parsed[rel][i][1]))
        kinds.append(kind)
    files: List[str] = []
    values: List[Tuple[Any, ...]] = []
    for rel in rels:
        typed = tuple(value_of(kind, raw, timezone) for kind, (_, raw) in zip(kinds, parsed[rel]))
        for name in leaves[rel]:
            files.append(os.path.join(root, rel, name))
            values.append(typed)
    return Layout(files=files, columns=names, types=[_arrow_type(k) for k in kinds], values=values)


def attach(
    table: Any,
    layout: Layout,
    counts: Sequence[int],
    given: Optional[Dict[str, Any]] = None,
    cast: Any = None,
) -> Any:
    """Add the partition columns to the table read from ``layout.files``.

    ``counts`` is the number of rows each file gave. A partition column the data
    files also hold keeps its place and takes the folder's type and value (as in
    Spark); the others come after the data columns, in folder order. A column in
    ``given`` (lower case name -> Arrow type, from a user schema) is cast from
    its text with ``cast(column, type)``.
    """
    import numpy as np
    import pyarrow as pa

    which = pa.array(
        np.repeat(np.arange(len(counts), dtype=np.int64), np.asarray(counts, dtype=np.int64))
    )
    given = given or {}
    lower = [c.lower() for c in table.column_names]
    for i, (name, t) in enumerate(zip(layout.columns, layout.types)):
        column = pa.array([v[i] for v in layout.values], t).take(which)
        target = given.get(name.lower())
        if target is not None and not target.equals(column.type):
            column = cast(column, target)
        f = pa.field(name, column.type)
        if name.lower() in lower:
            table = table.set_column(lower.index(name.lower()), f, column)
        else:
            table = table.append_column(f, column)
    return table
