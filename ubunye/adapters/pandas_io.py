"""pandas data-plane IO behind the Backend seam (issue #38).

No Spark, no JVM. A task reads a path, flows through the engine and writes a
path with nothing but Python. The formats are the generic path ones (csv,
parquet, json); lakehouse formats and managed tables are Spark's job.

**Spark is the reference.** The same folder must give the same data on either
backend, so every default Spark applies is applied here too, and anything this
backend cannot honour is refused by name rather than quietly ignored:

* CSV: no header unless ``header`` is true (columns are then ``_c0``, ``_c1``);
  every column is text unless ``inferSchema`` is true; an empty field is null.
  With ``inferSchema``: ``int`` when every value fits in 32 bits, else
  ``bigint``; an all-null column is text; timestamps are instants.
* JSON: one object per line unless ``multiLine`` is true; columns (and nested
  fields) sorted by name; integers are ``bigint``; timestamps stay text.
* A path may be a file, a folder of part files (Spark's layout; names starting
  with ``_`` or ``.`` and empty files are skipped, as Spark skips them) or a glob. ``name=value`` folders are partition columns, typed as
  Spark infers them (:mod:`ubunye.adapters.pandas_partitions`).
* Timestamp text is read in the backend's timezone (``UTC`` unless set), the
  way Spark reads it in ``spark.sql.session.timeZone``.

pyarrow does the IO, and the pandas frame handed to the task is Arrow-backed
(``pd.ArrowDtype``), so a nullable integer stays an integer and a decimal stays
a decimal, as in Spark. The frame is wrapped in :class:`PandasDataFrameAdapter`
so it satisfies ``DataFramePort`` exactly as a Spark DataFrame does.
"""

from __future__ import annotations

import contextlib
import glob
import json
import logging
import os
import re
from typing import Any, Dict, Iterator, List, Optional, Sequence

from ubunye.adapters import ddl, pandas_partitions
from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter
from ubunye.core import runs
from ubunye.core.errors import SinkWriteError, SourceReadError
from ubunye.core.runs import RunLeaseLost
from ubunye.core.write_modes import ResolvedWriteMode

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = frozenset({"csv", "parquet", "json"})

# The reader options each format honours, lower-cased (Spark option names are
# case blind). Anything else is refused: a silently ignored option means
# different data from the Spark backend, with no warning.
READ_OPTIONS: Dict[str, frozenset] = {
    "csv": frozenset(
        {
            "header",
            "inferschema",
            "sep",
            "delimiter",
            "nullvalue",
            "quote",
            "escape",
            "encoding",
            "multiline",
            "mode",
        }
    ),
    "json": frozenset({"multiline", "encoding", "mode"}),
    "parquet": frozenset(),
}

_INT32_MIN, _INT32_MAX = -(2**31), 2**31 - 1
# Spark reads booleans case blind. pyarrow's defaults also read "1" and "0" as
# booleans, which Spark never does.
_TRUE = ["true", "True", "TRUE"]
_FALSE = ["false", "False", "FALSE"]


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def _local_path(path: str, *, error: type) -> str:
    """A local filesystem path from a plain path or a ``file://`` URI.

    Remote schemes (s3a://, abfss://, dbfs:/ ...) are refused: this backend
    reads and writes local paths only.
    """
    if path.startswith("file://"):
        path = path[len("file://") :]
        # file:///C:/x -> /C:/x; drop the leading slash before a Windows drive.
        if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
            path = path[1:]
        return path
    if "://" in path or path.startswith("dbfs:"):
        raise error(
            f"The pandas backend reads and writes local paths only, not '{path}'.",
            context={"Backend": "pandas", "path": path},
            hint="Use a local path or a file:// URI, or the Spark backend for cloud storage.",
        )
    return path


def _data_files(local: str) -> List[str]:
    """The files behind a path: the file itself, a folder's data files, or a glob.

    Empty (zero byte) files are skipped, as Spark skips them however the path
    names them; folders go through partition discovery instead (F-012).
    """
    if glob.has_magic(local):
        candidates = sorted(glob.glob(local))
    elif os.path.isdir(local):
        candidates = sorted(os.path.join(local, n) for n in os.listdir(local))
    elif os.path.isfile(local):
        candidates = [local]
    else:
        candidates = []
    return [
        f
        for f in candidates
        if os.path.isfile(f)
        and not os.path.basename(f).startswith(("_", "."))
        and os.path.getsize(f) > 0
    ]


#: Spark's `mode` for csv and json: what to do with a malformed record.
#: FAILFAST stops, DROPMALFORMED skips it, PERMISSIVE (Spark's default) keeps a
#: CSV row with the wrong number of fields by cutting or padding it with null,
#: as Spark does. Other malformed records stop the read rather than being guessed.
PARSE_MODES = frozenset({"PERMISSIVE", "FAILFAST", "DROPMALFORMED"})


def _unknown_options(options: Dict[str, Any], allowed: frozenset) -> List[str]:
    """Options not in ``allowed``, named as the user spelled them."""
    return sorted(str(k) for k in options if str(k).lower() not in allowed)


def read_problems(
    file_format: str, options: Optional[Dict[str, Any]] = None, schema: Optional[str] = None
) -> List[str]:
    """Everything about a read this backend cannot honour, before opening a file."""
    fmt = (file_format or "parquet").lower()
    if fmt not in SUPPORTED_FORMATS:
        return [f"The pandas backend cannot read file_format '{fmt}'."]
    problems = []
    options = options or {}
    unknown = _unknown_options(options, READ_OPTIONS[fmt])
    if unknown:
        problems.append(
            f"The pandas backend does not support the {fmt} option(s) {unknown} "
            f"(it supports {sorted(READ_OPTIONS[fmt]) or 'none'})."
        )
    for key, value in options.items():
        if str(key).lower() == "mode" and str(value).upper() not in PARSE_MODES:
            problems.append(
                f"The {fmt} option mode '{value}' is not one of {', '.join(sorted(PARSE_MODES))}."
            )
    if schema:
        try:
            ddl.parse(schema)
        except ValueError as exc:
            problems.append(f"The pandas backend cannot use this schema: {exc}.")
    return problems


def write_problems(file_format: str, options: Optional[Dict[str, Any]] = None) -> List[str]:
    """Everything about a write this backend cannot honour, before writing."""
    fmt = (file_format or "parquet").lower()
    if fmt not in SUPPORTED_FORMATS:
        return [f"The pandas backend cannot write file_format '{fmt}'."]
    unknown = _unknown_options(options or {}, WRITE_OPTIONS[fmt])
    if unknown:
        return [
            f"The pandas backend does not support the {fmt} write option(s) {unknown} "
            f"(it supports {sorted(WRITE_OPTIONS[fmt]) or 'none'})."
        ]
    return []


# --------------------------------------------------------------------------- #
# Spark's type rules, applied to an Arrow table
# --------------------------------------------------------------------------- #


def _spark_type(t: Any) -> Any:
    """Map an inferred Arrow type onto the type Spark would have inferred."""
    import pyarrow as pa

    if pa.types.is_null(t) or pa.types.is_large_string(t):
        return pa.string()
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return pa.list_(_spark_type(t.value_type))
    if pa.types.is_struct(t):
        return pa.struct([pa.field(f.name, _spark_type(f.type)) for f in t])
    return t


def _to_spark_types(table: Any) -> Any:
    import pyarrow as pa

    fields = [f.with_type(_spark_type(f.type)) for f in table.schema]
    target = pa.schema(fields)
    return table if target.equals(table.schema) else table.cast(target)


def _narrow_ints(table: Any) -> Any:
    """Spark's CSV inference: ``int`` when every value fits in 32 bits."""
    import pyarrow as pa
    import pyarrow.compute as pc

    for i, field in enumerate(table.schema):
        if pa.types.is_int64(field.type):
            bounds = pc.min_max(table.column(i))
            lo, hi = bounds["min"].as_py(), bounds["max"].as_py()
            if lo is None or (_INT32_MIN <= lo and hi <= _INT32_MAX):
                col = table.column(i).cast(pa.int32())
                table = table.set_column(i, field.with_type(pa.int32()), col)
    return table


def assume_zone(col: Any, timezone: str) -> Any:
    """Wall clock timestamps as instants in ``timezone``, by Java's rule, as Spark reads them.

    Spark turns a local time into an instant with ``ZonedDateTime.of``: a time in
    a daylight saving gap (02:30 on the morning clocks go forward) moves later by
    the gap, so it takes the offset before the change; a time that happens twice
    (01:30 on the morning clocks go back) takes the earlier instant. pyarrow's
    ``assume_timezone`` stopped at both (F-063).
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    try:
        return pc.assume_timezone(col, timezone=timezone)
    except pa.ArrowInvalid:
        pass  # a time in a gap or a fold: the rule below
    early = pc.assume_timezone(col, timezone=timezone, ambiguous="earliest", nonexistent="earliest")
    late = pc.assume_timezone(col, timezone=timezone, ambiguous="earliest", nonexistent="latest")
    # Only a time in a gap gives two answers: the last instant before the gap
    # and the first after (the change itself). The offset a minute before the
    # change is the offset before the gap. (A whole minute, not the last instant:
    # pyarrow 25 finds the wrong offset for 06:59:59.999999999 in nanoseconds.)
    unit = col.type.unit
    before = pc.subtract(late, pa.scalar(60, pa.duration("s")).cast(pa.duration(unit)))
    before_utc = before.cast(pa.timestamp(unit))  # the UTC wall clock of that instant
    offset = pc.subtract(pc.local_timestamp(before), before_utc)
    moved = pc.subtract(col, offset).cast(pa.timestamp(unit, tz=timezone))
    return pc.if_else(pc.not_equal(early, late), moved, early)


def _instants(table: Any, timezone: str) -> Any:
    """Wall-clock timestamps read as instants in ``timezone``, held in UTC."""
    import pyarrow as pa

    for i, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type):
            col = table.column(i)
            if field.type.tz is None:
                col = assume_zone(col, timezone)
            col = col.cast(pa.timestamp("us", tz="UTC"))
            table = table.set_column(i, field.with_type(col.type), col)
    return table


def _cast(col: Any, target: Any, timezone: str) -> Any:
    """Cast one column to a schema type the way Spark parses text into it."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
        if pa.types.is_boolean(target):
            return pc.utf8_lower(col).cast(target)  # Spark: case blind
        if pa.types.is_timestamp(target) and target.tz is not None:
            # Text without an offset is wall clock time in the session zone;
            # text with one is already an instant. A column may hold both.
            has_offset = pc.fill_null(
                pc.match_substring_regex(col, pattern=r"(Z|[+-]\d\d:?\d\d)$"), False
            )
            null = pa.scalar(None, col.type)
            aware = pc.if_else(has_offset, col, null).cast(pa.timestamp("us", tz="UTC"))
            naive = pc.if_else(has_offset, null, col).cast(pa.timestamp("us"))
            local = assume_zone(naive, timezone).cast(pa.timestamp("us", tz="UTC"))
            return pc.coalesce(aware, local).cast(target)
    return col.cast(target)


def _apply_schema(table: Any, schema: Any, timezone: str = "UTC") -> Any:
    """Select and cast to an explicit schema; a missing column is all null."""
    import pyarrow as pa

    columns = [
        (
            _cast(table.column(f.name), f.type, timezone)
            if f.name in table.column_names
            else pa.nulls(table.num_rows, f.type)
        )
        for f in schema
    ]
    return pa.table(columns, schema=schema)


# --------------------------------------------------------------------------- #
# Readers, one per format, each returning an Arrow table
# --------------------------------------------------------------------------- #


def _read_csv(
    files: List[str],
    opts: Dict[str, Any],
    schema: Any,
    timezone: str,
    counts: Optional[List[int]] = None,
) -> Any:
    import pyarrow as pa
    import pyarrow.csv as pcsv

    header = _truthy(opts.get("header", "false"))
    infer = _truthy(opts.get("inferschema", "false")) and schema is None
    dialect = _CsvDialect(opts)
    hint = escape_hint(files[0], opts) if files else None
    if hint:
        logger.warning(hint)

    tables = []
    for f in files:
        data, encoding, parse = _csv_source(f, str(opts.get("encoding", "utf8")), dialect, opts)
        if schema is not None:
            # Spark: an explicit schema names the columns by position, and the
            # header line (if any) is skipped.
            names = list(schema.names)
        else:
            # The first record alone names the columns (Spark does the same); it is
            # read on its own so a malformed row further down cannot stop it.
            names = _first_record(data, encoding, parse)
            if not header:
                names = [f"_c{n}" for n in range(len(names))]
            else:
                names = safe_header(names, str(opts.get("nullvalue", "")))
        convert = pcsv.ConvertOptions(
            # Text unless inference is on; an explicit schema is cast below.
            column_types=None if infer else {n: pa.string() for n in names},
            null_values=[str(opts.get("nullvalue", ""))],
            strings_can_be_null=True,
            quoted_strings_can_be_null=True,
            true_values=_TRUE,
            false_values=_FALSE,
        )
        read = pcsv.ReadOptions(encoding=encoding, column_names=names, skip_rows=1 if header else 0)
        try:
            tables.append(_parse_csv(data, read, parse, convert))
        except pa.ArrowInvalid as exc:
            # PERMISSIVE (Spark's default) keeps a row with the wrong number of
            # fields: extra fields are dropped and missing ones are null. pyarrow
            # stops instead, so the file is read again with those rows evened out,
            # exactly as Spark evens them, and parsed the normal way.
            if not _permissive(opts) or "columns" not in str(exc):
                raise
            evened = _even_rows(
                data,
                encoding,
                parse,
                len(names),
                skip_header=header,
                pad=str(opts.get("nullvalue", "")),
            )
            tables.append(_parse_csv(evened.getvalue(), read, parse, convert))
        if counts is not None:
            counts.append(tables[-1].num_rows)

    table = pa.concat_tables(tables, promote_options="permissive")
    if schema is not None:
        return _instants(_apply_schema(table, schema, timezone), timezone)
    table = _to_spark_types(table)
    if infer:
        table = _narrow_ints(table)
    return _instants(table, timezone)


def _parse_csv(data: bytes, read: Any, parse: Any, convert: Any) -> Any:
    """pyarrow's CSV parse of ``data``, with no limit on the length of a row.

    pyarrow parses in blocks of 1 MB and stops at a row longer than a block
    ("straddling object straddles two block boundaries"). Spark has no such limit
    (``maxCharsPerColumn`` is -1), so a file with such a row is parsed again as
    one block (F-062).
    """
    import pyarrow as pa
    import pyarrow.csv as pcsv

    try:
        return pcsv.read_csv(
            pa.BufferReader(data), read_options=read, parse_options=parse, convert_options=convert
        )
    except pa.ArrowInvalid as exc:
        if "straddl" not in str(exc):
            raise
    # A file in another encoding is parsed after it is turned into UTF-8, which
    # takes up to three bytes for one (cp1252's euro sign).
    size = len(data) * (1 if _text_encoding(read.encoding) == "utf-8" else 3) + 1
    whole = pcsv.ReadOptions(
        encoding=read.encoding,
        column_names=read.column_names,
        skip_rows=read.skip_rows,
        block_size=max(read.block_size, min(size, 2**31 - 1)),
    )
    return pcsv.read_csv(
        pa.BufferReader(data), read_options=whole, parse_options=parse, convert_options=convert
    )


# How far into a CSV file the escape check looks.
_ESCAPE_SAMPLE_BYTES = 4 << 20


def escape_hint(path: str, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Why a CSV input is likely to be split wrongly with Spark's escape, or None.

    Spark's default escape is a backslash; files written by pandas, Excel and most
    databases escape a quote by doubling it. Read that way, some rows split in the
    wrong place and a number column silently becomes text, on Spark and so on the
    pandas backend too (it reads as Spark does). This looks for a doubled quote
    inside text in the first few MB, only when the escape was left at Spark's
    default. It never changes what is read.
    """
    opts = {str(k).lower(): v for k, v in (options or {}).items()}
    if "escape" in opts or str(opts.get("quote", '"')) != '"':
        return None
    target = path
    if os.path.isdir(path):
        found = sorted(
            os.path.join(root, name)
            for root, _, names in os.walk(path)
            for name in names
            if not name.startswith((".", "_"))
        )
        if not found:
            return None
        target = found[0]
    try:
        with open(target, "rb") as handle:
            sample = handle.read(_ESCAPE_SAMPLE_BYTES)
    except OSError:
        return None
    # Most files hold no doubled quote at all: rule them out at byte speed, before
    # any decoding or regex (the performance guard caught a +120% plain CSV read).
    if b'""' not in sample:
        return None
    text = sample.decode("utf-8", errors="replace")
    delimiter = re.escape(str(opts.get("sep") or opts.get("delimiter") or ","))
    # A doubled quote with ordinary text on both sides: `said ""great""`, never an
    # empty quoted field (`,"",`).
    if not re.search(rf'[^{delimiter}\r\n"]""[^{delimiter}\r\n"]', text):
        return None
    return (
        f'{path} doubles quotes inside quoted text (""), as pandas and Excel write '
        "CSV, but escape is Spark's default (a backslash), so some rows will be split "
        "in the wrong place. Set options.escape: '\"' on this input (and multiLine: "
        '"true" if text spans lines).'
    )


class _CsvDialect:
    """How Spark is told to split a CSV file: the delimiter, quote and escape."""

    def __init__(self, opts: Dict[str, Any]) -> None:
        self.delimiter = str(opts.get("sep") or opts.get("delimiter") or ",")
        self.quote = str(opts.get("quote", '"'))
        # Spark's default escape character is a backslash, not a doubled quote.
        self.escape = str(opts.get("escape", "\\"))
        self.multiline = _truthy(opts.get("multiline", "false"))
        self.null_text = str(opts.get("nullvalue", ""))


def _csv_source(path: str, encoding: str, dialect: _CsvDialect, opts: Dict[str, Any]) -> Any:
    """A CSV file's bytes, their encoding, and the pyarrow options that split them as Spark does.

    pyarrow splits a file exactly as Spark does unless the file has a quote or
    escape character that is not simply the two ends of a quoted field (a
    doubled quote, a backslash, a quote in mid value), or a line of blanks.
    Those files are split by :mod:`ubunye.adapters.spark_csv`, a port of
    Spark's own parser, and handed back as plain CSV. Compressed files (.gz and
    so on) are opened by their extension, as pyarrow and Spark both do.
    """
    import pyarrow as pa
    import pyarrow.csv as pcsv

    from ubunye.adapters import spark_csv

    with pa.input_stream(path, compression="detect") as stream:
        data = stream.read()
    if _text_encoding(encoding) == "utf-8" and data.startswith(b"\xef\xbb\xbf"):
        # Spark drops a UTF-8 byte order mark at the start of each file (header
        # or data, quoted or not) and keeps one anywhere else. Left in, it became
        # part of the first column's name.
        data = data[3:]
    try:
        text = data.decode(_text_encoding(encoding))
    except UnicodeDecodeError:
        # Spark decodes with Java's defaults: a byte that is not valid in the
        # encoding becomes U+FFFD and the read goes on. pyarrow and Python stop
        # instead (F-061), so the text is decoded here the Java way and handed on
        # as UTF-8.
        text = data.decode(_text_encoding(encoding), errors="replace")
        data, encoding = text.encode("utf-8"), "utf8"
    d = dialect
    plain = not spark_csv.needs_spark_split(text, d.delimiter, d.quote, d.escape, d.multiline)
    parse = pcsv.ParseOptions(
        delimiter=d.delimiter,
        quote_char=d.quote or (False if plain else '"'),
        # A plain file has nothing to unescape, and a split one is written back
        # with doubled quotes only.
        escape_char=False,
        double_quote=True,
        newlines_in_values=d.multiline,
        # DROPMALFORMED skips a row with the wrong number of fields, as Spark does;
        # FAILFAST and PERMISSIVE stop at it.
        invalid_row_handler=_skip if _drop_malformed(opts) else None,
    )
    if plain:
        return bytes(data), encoding, parse
    text = spark_csv.respell(text, d.delimiter, d.quote, d.escape, d.multiline, d.null_text)
    return text.encode("utf-8"), "utf8", parse


def _csv_dialect(parse: Any) -> Dict[str, Any]:
    """pyarrow's parse options as keyword arguments for Python's csv module."""
    return {
        "delimiter": parse.delimiter,
        "quotechar": parse.quote_char or '"',
        "escapechar": parse.escape_char or None,
        "doublequote": True,
    }


def _text_encoding(encoding: str) -> str:
    return "utf-8" if encoding.lower().replace("-", "") == "utf8" else encoding


def _text(data: bytes, encoding: str) -> Any:
    import io

    return io.TextIOWrapper(io.BytesIO(data), encoding=_text_encoding(encoding), newline="")


def safe_header(names: List[str], null_text: str = "") -> List[str]:
    """Column names from a CSV header, as Spark's ``CSVUtils.makeSafeHeader`` makes them.

    A blank name (or the ``nullValue`` text) becomes ``_c<index>``. A name that
    appears more than once, ignoring case (Spark's default
    ``spark.sql.caseSensitive=false``), gets its index appended to every copy:
    ``a,a,A`` becomes ``a0,a1,A2``. Left as they were, pyarrow refused the file
    ("duplicate field names") and a blank name became a column called ``""``
    (F-060).
    """
    from collections import Counter

    counts = Counter(n.lower() for n in names if n)
    dupes = {n for n, seen in counts.items() if seen > 1}
    safe = []
    for index, name in enumerate(names):
        if not name or name == null_text:
            safe.append(f"_c{index}")
        elif name.lower() in dupes:
            safe.append(f"{name}{index}")
        else:
            safe.append(name)
    return safe


@contextlib.contextmanager
def _no_field_limit() -> Iterator[None]:
    """Python's csv module with no limit on a field's length, as Spark has none.

    The module stops at 131,072 characters by default (F-062).
    """
    import csv

    before = csv.field_size_limit()
    csv.field_size_limit(2**31 - 1)  # the largest a C long holds on every platform
    try:
        yield
    finally:
        csv.field_size_limit(before)


def _first_record(data: bytes, encoding: str, parse: Any) -> List[str]:
    """The first non blank record of a CSV file's bytes, as its fields."""
    import csv

    with _text(data, encoding) as handle, _no_field_limit():
        for row in csv.reader(handle, **_csv_dialect(parse)):
            if row:
                return row
    return []


def _permissive(opts: Dict[str, Any]) -> bool:
    return str(opts.get("mode", "PERMISSIVE")).upper() == "PERMISSIVE"


def _even_rows(
    data: bytes, encoding: str, parse: Any, width: int, *, skip_header: bool, pad: str = ""
) -> Any:
    """The CSV in ``data`` with every row cut or padded to ``width`` fields.

    What Spark's PERMISSIVE mode does with a row that has too many or too few
    fields. Returned as bytes to parse again, so types are inferred exactly as
    for a clean file.
    """
    import csv
    import io

    text_encoding = _text_encoding(encoding)
    dialect = _csv_dialect(parse)
    out = io.StringIO()
    writer = csv.writer(out, lineterminator=chr(10), **dialect)
    with _text(data, encoding) as handle, _no_field_limit():
        reader = csv.reader(handle, **dialect)
        for number, row in enumerate(reader):
            if not row:
                continue  # blank line: Spark skips it
            if not (skip_header and number == 0):
                row = (row + [pad] * width)[:width]  # pad with the null marker: read as null
            writer.writerow(row)
    return io.BytesIO(out.getvalue().encode(text_encoding))


def _drop_malformed(opts: Dict[str, Any]) -> bool:
    return str(opts.get("mode", "PERMISSIVE")).upper() == "DROPMALFORMED"


def _skip(row: Any) -> str:
    return "skip"


def _read_json(
    files: List[str],
    opts: Dict[str, Any],
    schema: Any,
    timezone: str,
    counts: Optional[List[int]] = None,
) -> Any:
    import pyarrow as pa

    from ubunye.adapters import spark_json

    encoding = str(opts.get("encoding", "utf-8"))
    rows: List[Dict[str, Any]] = []
    for f in files:
        before = len(rows)
        with open(f, encoding=encoding) as handle:
            if _truthy(opts.get("multiline", "false")):
                doc = json.load(handle)
                rows.extend(doc if isinstance(doc, list) else [doc])
            else:
                drop = _drop_malformed(opts)
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        if not drop:
                            raise
                        continue  # DROPMALFORMED: skip the record, as Spark does
                    # A line holding an array of objects is one row per object.
                    rows.extend(record if isinstance(record, list) else [record])
        if counts is not None:
            counts.append(len(rows) - before)
    # Parsed with the json module, not pyarrow's reader: pyarrow turns ISO text
    # into timestamps and keeps key order, and Spark does neither. Typed by a port
    # of Spark's JSON inference (F-064).
    if schema is not None:
        columns = {}
        for f in schema:
            values = [r.get(f.name) for r in rows]
            if pa.types.is_string(f.type):
                # Spark keeps the JSON text of a value that is not a string.
                values = [spark_json.convert(v, spark_json.STRING) for v in values]
            columns[f.name] = pa.array(values)
        table = pa.table(columns) if columns else no_columns(len(rows))
        return _instants(_apply_schema(table, schema, timezone), timezone)
    fields = spark_json.infer_schema(rows)
    if not fields:
        # Records with no fields ({}) are still rows: Spark keeps one row per
        # record, with no columns. pa.table({}) has none (F-065).
        return no_columns(len(rows))
    return pa.table(
        {name: spark_json.column([r.get(name) for r in rows], kind) for name, kind in fields}
    )


def _read_parquet(files: List[str], schema: Any, counts: Optional[List[int]] = None) -> Any:
    import pyarrow as pa
    import pyarrow.parquet as pq

    tables = []
    for f in files:
        handle = pq.ParquetFile(f)
        # Spark's legacy INT96 timestamps hold UTC instants; say so.
        legacy = {c.name for c in handle.schema if c.physical_type == "INT96"}
        table = handle.read()
        for name in legacy & set(table.column_names):
            i = table.column_names.index(name)
            col = table.column(i).cast(pa.timestamp("us")).cast(pa.timestamp("us", tz="UTC"))
            table = table.set_column(i, pa.field(name, col.type), col)
        tables.append(table)
        if counts is not None:
            counts.append(table.num_rows)
    table = pa.concat_tables(tables, promote_options="permissive")
    table = _signed(table)
    return _apply_schema(table, schema) if schema is not None else table


def _signed_type(t: Any) -> Any:
    """Spark's type for a parquet unsigned integer, at any depth (F-066).

    ``ParquetSchemaConverter`` (SPARK-34817): UINT_8 is ``smallint``, UINT_16
    ``int``, UINT_32 ``bigint`` and UINT_64 ``decimal(20,0)``. Spark has no
    unsigned type, so the same file read as ``uint64`` on pandas gave another
    schema and another run record hash.
    """
    import pyarrow as pa

    if pa.types.is_unsigned_integer(t):
        return {8: pa.int16(), 16: pa.int32(), 32: pa.int64()}.get(
            t.bit_width, pa.decimal128(20, 0)
        )
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        inner = _signed_type(t.value_type)
        if inner.equals(t.value_type):
            return t
        return (pa.large_list if pa.types.is_large_list(t) else pa.list_)(inner)
    if pa.types.is_struct(t):
        fields = [f.with_type(_signed_type(f.type)) for f in t]
        return t if all(a.equals(b) for a, b in zip(fields, t)) else pa.struct(fields)
    if pa.types.is_map(t):
        key, item = _signed_type(t.key_type), _signed_type(t.item_type)
        if key.equals(t.key_type) and item.equals(t.item_type):
            return t
        return pa.map_(key, item)
    return t


def _signed(table: Any) -> Any:
    import pyarrow as pa

    target = pa.schema([f.with_type(_signed_type(f.type)) for f in table.schema])
    return table if target.equals(table.schema) else table.cast(target)


def to_pandas(table: Any) -> Any:
    """An Arrow table as an Arrow-backed pandas frame (types kept exactly)."""
    import pandas as pd

    return table.to_pandas(types_mapper=pd.ArrowDtype)


def read_frame(
    file_format: str,
    path: str,
    *,
    options: Optional[Dict[str, Any]] = None,
    schema: Optional[str] = None,
    timezone: str = "UTC",
) -> PandasDataFrameAdapter:
    """Read a path into an Arrow-backed pandas frame, wrapped as a ``DataFramePort``."""
    fmt = (file_format or "parquet").lower()
    problems = read_problems(fmt, options, schema)
    if problems:
        raise SourceReadError(
            problems[0],
            context={"Backend": "pandas", "file_format": fmt, "Also": problems[1:] or "none"},
            hint="Change the input's options or schema, or run this task on the Spark backend.",
        )
    opts = {str(k).lower(): v for k, v in (options or {}).items()}
    arrow_schema = ddl.parse(schema) if schema else None

    local = _local_path(path, error=SourceReadError)
    layout = None
    if not glob.has_magic(local) and os.path.isdir(local):
        # A folder is listed the way Spark lists it: name=value folders become
        # partition columns (F-012); otherwise only its top level files are read.
        given = [f.name for f in arrow_schema] if arrow_schema is not None else []
        layout = pandas_partitions.discover(local, timezone, given)
        files = layout.files
    else:
        files = _data_files(local)
    if not files:
        raise SourceReadError(
            f"Path does not exist or holds no data files: {path}",
            context={"Backend": "pandas", "path": local},
            hint="Check the path. A folder must hold data files, not only _ or . files.",
        )

    data_schema, given_types = arrow_schema, {}
    if layout is not None and layout.columns and arrow_schema is not None:
        import pyarrow as pa

        # The data files hold the other columns; the schema's partition columns
        # come from the folder names.
        names = {c.lower() for c in layout.columns}
        data_schema = pa.schema([f for f in arrow_schema if f.name.lower() not in names])
        given_types = {f.name.lower(): f.type for f in arrow_schema if f.name.lower() in names}
    counts: List[int] = []
    try:
        if fmt == "csv":
            table = _read_csv(files, opts, data_schema, timezone, counts)
        elif fmt == "json":
            table = _read_json(files, opts, data_schema, timezone, counts)
        else:
            table = _read_parquet(files, data_schema, counts)
        if layout is not None and layout.columns:
            table = pandas_partitions.attach(
                table,
                layout,
                counts,
                given_types,
                cast=lambda col, target: _cast(col, target, timezone),
            )
    except Exception as exc:  # pyarrow and json errors, with the path named
        raise SourceReadError(
            f"The pandas backend could not read {fmt} at {path}: {exc}",
            context={"Backend": "pandas", "path": local, "file_format": fmt},
        ) from exc

    frame = PandasDataFrameAdapter(to_pandas(table))
    # The files read, for the run record's source version (F-046); never read again.
    frame.source_files = [os.path.abspath(f) for f in files]
    return frame


# --------------------------------------------------------------------------- #
# Writes: Spark's folder layout and Spark's text formats
# --------------------------------------------------------------------------- #

# The writer options each format honours, lower-cased. Anything else is refused.
WRITE_OPTIONS: Dict[str, frozenset] = {
    "csv": frozenset({"header", "sep", "delimiter"}),
    "json": frozenset(),
    "parquet": frozenset({"compression"}),
}
_PARQUET_CODECS = {
    "none": None,
    "uncompressed": None,
    "snappy": "snappy",
    "gzip": "gzip",
    "lz4": "lz4",
    "zstd": "zstd",
    "brotli": "brotli",
}


def _refuse(message: str, **context: Any) -> SinkWriteError:
    return SinkWriteError(message, context={"Backend": "pandas", **context})


def no_columns(rows: int) -> Any:
    """An Arrow table with ``rows`` rows and no columns (``pa.table({})`` has none)."""
    import pyarrow as pa

    empty = pa.array([{}] * rows, pa.struct([]))
    return pa.Table.from_batches([pa.RecordBatch.from_struct_array(empty)])


def to_arrow(df: Any, timezone: str) -> Any:
    """What a task returned, as an Arrow table with the types Spark writes.

    Takes the ``DataFramePort`` adapter, a pandas frame or an Arrow table. A
    named index (what ``groupby`` leaves) is kept as columns; an unnamed one
    (what a filter leaves) is dropped, since Spark has no row index.
    """
    import pandas as pd
    import pyarrow as pa

    frame = df.native if isinstance(df, PandasDataFrameAdapter) else df
    if isinstance(frame, pd.DataFrame):
        if frame.columns.duplicated().any():
            dupes = sorted({str(c) for c in frame.columns[frame.columns.duplicated()]})
            raise _refuse(f"The frame has duplicate column names {dupes}.")
        if any(name is not None for name in frame.index.names):
            frame = frame.reset_index()
        if len(frame.columns) == 0:
            # Rows with no columns (records with no fields): from_pandas loses the
            # rows, and Spark keeps them (one struct<> row each).
            return no_columns(len(frame))
        table = pa.Table.from_pandas(frame, preserve_index=False)
    elif isinstance(frame, pa.Table):
        table = frame
    else:
        raise _refuse(
            f"The pandas backend writes pandas DataFrames, not {type(frame).__name__}.",
        )

    fields, columns = [], []
    for field, col in zip(table.schema, table.columns):
        t = field.type
        if pa.types.is_dictionary(t):  # a pandas category
            col, t = col.cast(t.value_type), t.value_type
        if pa.types.is_null(t) or pa.types.is_large_string(t):
            col, t = col.cast(pa.string()), pa.string()
        if pa.types.is_timestamp(t):
            if t.tz is None:
                col = assume_zone(col, timezone)
            # Spark holds microseconds and cannot read nanosecond parquet.
            col = col.cast(pa.timestamp("us", tz="UTC"), safe=False)
            t = col.type
        fields.append(field.with_type(t))
        columns.append(col)
    return pa.table(columns, schema=pa.schema(fields))


def _spark_timestamp_text(col: Any, timezone: str) -> Any:
    """Spark's default text for a timestamp: ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.

    Milliseconds, in the session zone, with a ``+02:00`` style offset or ``Z``.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    local = col.cast(pa.timestamp("ms", tz=timezone), safe=False)
    body = pc.strftime(local, format="%Y-%m-%dT%H:%M:%S")
    offset = pc.strftime(local, format="%z")
    offset = pc.replace_substring_regex(offset, pattern=r"^([+-]\d\d)(\d\d)$", replacement=r"\1:\2")
    offset = pc.replace_substring(offset, pattern="+00:00", replacement="Z")
    return pc.binary_join_element_wise(body, offset, "")


def _texts_for_timestamps(table: Any, timezone: str) -> Any:
    import pyarrow as pa

    for i, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type):
            text = _spark_timestamp_text(table.column(i), timezone)
            table = table.set_column(i, pa.field(field.name, pa.string()), text)
    return table


def _json_value(value: Any, arrow_type: Any = None) -> str:
    """One value as Spark's JSON writer (Jackson) writes it.

    Compact, no spaces; doubles the Java way (``1.0E10``), with NaN and the
    infinities as strings; null fields of an object left out (Spark's
    ``ignoreNullFields``) but nulls inside an array kept; text as UTF-8.
    ``arrow_type`` is the value's type where known: a map (which Arrow gives as
    ``(key, value)`` pairs) is then written as an object, its null values kept,
    as Spark writes a map.
    """
    import base64
    import datetime as dt
    import decimal
    import math

    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return json.dumps(_java_double(value))
        return str(_java_double(value))
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    import pyarrow as pa

    if arrow_type is not None and pa.types.is_map(arrow_type):
        item = arrow_type.item_type
        pairs = value.items() if isinstance(value, dict) else value
        members = (
            f"{json.dumps(_map_key(k), ensure_ascii=False)}:{_json_value(v, item)}"
            for k, v in pairs
        )
        return "{" + ",".join(members) + "}"  # a map keeps its null values
    if isinstance(value, dict):
        fields = (
            {f.name: f.type for f in arrow_type}
            if arrow_type is not None and pa.types.is_struct(arrow_type)
            else {}
        )
        members = (
            f"{json.dumps(str(k), ensure_ascii=False)}:{_json_value(v, fields.get(k))}"
            for k, v in value.items()
            if v is not None
        )
        return "{" + ",".join(members) + "}"
    if isinstance(value, (list, tuple)):
        element = (
            arrow_type.value_type
            if arrow_type is not None
            and (pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type))
            else None
        )
        return "[" + ",".join(_json_value(v, element) for v in value) + "]"
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (dt.date, dt.time)):
        return json.dumps(value.isoformat())
    if isinstance(value, bytes):
        return json.dumps(base64.b64encode(value).decode("ascii"))  # as Spark writes binary
    raise TypeError(f"cannot write {type(value).__name__} to JSON")


def _map_key(key: Any) -> str:
    """A map key as Spark writes it: its ``toString``."""
    if isinstance(key, bool):
        return "true" if key else "false"
    if isinstance(key, float):
        text = _java_double(key)
        return text if text is not None else str(key)
    return str(key)


def _shortest_floats(table: Any) -> Any:
    """float32 columns as the float64 of their shortest text (Java's Float.toString)."""
    import pyarrow as pa

    for i, field in enumerate(table.schema):
        if pa.types.is_float32(field.type) or pa.types.is_float16(field.type):
            import numpy as np

            values = [
                None if v is None else float(str(np.float32(v)))
                for v in table.column(i).to_pylist()
            ]
            table = table.set_column(i, pa.field(field.name, pa.float64()), pa.array(values))
    return table


def _parquet_codec(opts: Dict[str, Any]) -> Optional[str]:
    codec_name = str(opts.get("compression", "snappy")).lower()
    if codec_name not in _PARQUET_CODECS:
        raise _refuse(
            f"The pandas backend cannot write parquet compression '{codec_name}'.",
            Supported=sorted(_PARQUET_CODECS),
        )
    return _PARQUET_CODECS[codec_name]


def _write_part(
    table: Any,
    fmt: str,
    folder: str,
    opts: Dict[str, Any],
    timezone: str,
    stem: Optional[str] = None,
) -> str:
    """Write one part file into ``folder``; its name is returned.

    Spark's names: ``part-00000-<uuid>-c000.<ext>`` for an unpartitioned write,
    ``part-00000-<uuid>.c000.<ext>`` (a dot before ``c000``) in a partition folder.
    """
    import uuid

    stem = stem or f"part-00000-{uuid.uuid4()}-c000"
    if fmt == "parquet":
        import pyarrow.parquet as pq

        codec = _parquet_codec(opts)
        name = f"{stem}.{codec}.parquet" if codec else f"{stem}.parquet"
        pq.write_table(table, os.path.join(folder, name), compression=codec or "none")
        return name

    table = _texts_for_timestamps(table, timezone)
    if fmt == "csv":
        name = f"{stem}.csv"
        text = _csv_text(
            table,
            sep=str(opts.get("sep") or opts.get("delimiter") or ","),
            header=_truthy(opts.get("header", "false")),
        )
        with open(os.path.join(folder, name), "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        return name

    import pyarrow as pa

    name = f"{stem}.json"
    with open(os.path.join(folder, name), "w", encoding="utf-8", newline="\n") as handle:
        for batch in _shortest_floats(table).to_batches():
            row_type = pa.struct(list(batch.schema))
            for row in batch.to_pylist():
                handle.write(_json_value(row, row_type))
                handle.write("\n")
    return name


def _java_double(x: Any) -> Optional[str]:
    """A float as Java's ``Double.toString`` writes it, which is what Spark writes."""
    import decimal
    import math

    if x is None:
        return None
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    if x == 0:
        return "-0.0" if math.copysign(1.0, x) < 0 else "0.0"
    text = repr(x)
    if 1e-3 <= abs(x) < 1e7:
        return text if "." in text else text + ".0"
    sign, digits, exponent = decimal.Decimal(text).as_tuple()
    ds = "".join(map(str, digits)).rstrip("0") or "0"
    power = len(digits) + int(exponent) - 1
    return f"{'-' if sign else ''}{ds[0]}.{ds[1:] or '0'}E{power}"


def _csv_column(col: Any, field: Any, sep: str) -> Any:
    """One column as Spark's CSV text: null is empty, quotes only when needed."""
    import pyarrow as pa
    import pyarrow.compute as pc

    t = field.type
    if pa.types.is_floating(t):
        if pa.types.is_float64(t):
            values = [_java_double(v) for v in col.to_pylist()]
        else:  # float32 and float16: Java's Float.toString is numpy's shortest form
            import numpy as np

            values = [
                None if v is None else _java_double(float(str(np.float32(v))))
                for v in col.to_pylist()
            ]
        return pa.array(values, pa.string()).fill_null("")
    text = pc.cast(col, pa.string())
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        # Spark trims text on write (ignoreLeading/TrailingWhiteSpace default true).
        text = pc.utf8_trim_whitespace(text)
        special = "[" + "".join("\\" + c for c in sep) + '"\\r\\n]|^$'
        needs = pc.fill_null(pc.match_substring_regex(text, pattern=special), False)
        # Spark's default escape character is a backslash, not a doubled quote.
        quoted = pc.binary_join_element_wise(
            '"', pc.replace_substring(text, pattern='"', replacement='\\"'), '"', ""
        )
        text = pc.if_else(needs, quoted, text)
    return text.fill_null("")


def _csv_text(table: Any, *, sep: str, header: bool) -> str:
    """A whole table as Spark would write it to one CSV part file.

    Lines end in ``\n``, as Spark writes them on Linux and Databricks (on
    Windows Spark writes ``\r\n``; both readers accept either).
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    for field in table.schema:
        if pa.types.is_nested(field.type) or pa.types.is_binary(field.type):
            raise _refuse(
                f"csv cannot hold the nested or binary column '{field.name}'.",
                column=field.name,
            )
    lines = []
    if header:
        names = pa.array(table.column_names, pa.string())
        lines.append(sep.join(_csv_column(names, pa.field("h", pa.string()), sep).to_pylist()))
    if table.num_rows:
        cols = [_csv_column(table.column(i), f, sep) for i, f in enumerate(table.schema)]
        rows = cols[0] if len(cols) == 1 else pc.binary_join_element_wise(*cols, sep)
        # A line that renders empty (a one column row that is null) is skipped,
        # as Spark's writer skips it.
        lines.extend(line for line in rows.to_pylist() if line)
    return "".join(line + "\n" for line in lines)


def _touch(path: str) -> None:
    with open(path, "w", encoding="utf-8"):
        pass


def _staging(local: str) -> str:
    """A new hidden folder beside ``local`` (same disk, so a rename is one step)."""
    import uuid

    parent, base = os.path.split(local)
    os.makedirs(parent, exist_ok=True)
    left = sorted(glob.glob(os.path.join(glob.escape(parent), f".{glob.escape(base)}.ubunye-*")))
    if left:
        # Never deleted here: a `.old` folder may hold the only copy of a partition
        # a killed overwrite had moved aside, and another may be a live run's.
        logger.warning(
            "Found staging folders beside %s, left by a run that was killed or still "
            "being written by another: %s. A '.old' folder holds old partition folders "
            "moved aside by a partition overwrite (the path under it is the partition); "
            "the others hold new files not yet moved in. Remove them once no run is "
            "writing and nothing in them is needed.",
            local,
            left,
        )
    staging = os.path.join(parent, f".{base}.ubunye-{uuid.uuid4().hex[:12]}")
    os.makedirs(staging)
    return staging


def _commit(local: str, save_mode: str, write: Any) -> None:
    """Put freshly written part files in place, the way ``save_mode`` asks.

    ``write(folder)`` writes the part files (in partition folders, if any) and
    returns their paths relative to ``folder``. They are written into a hidden
    staging folder beside the target first, so a failed write never touches
    data already there: ``overwrite`` swaps the staged folder in only once it is
    complete, ``append`` moves each new part file into the existing folder.
    """
    import shutil

    local = os.path.normpath(os.path.abspath(local))
    exists = os.path.exists(local)
    if exists and save_mode == "ignore":
        return
    if exists and save_mode == "errorifexists":
        raise _refuse(f"Target already exists: {local}", path=local)
    if exists and save_mode == "append" and not os.path.isdir(local):
        raise _refuse(
            f"Cannot append to {local}: it is a single file, not a folder of part files.",
            path=local,
        )

    staging = _staging(local)
    try:
        parts = write(staging)
        if save_mode == "append":
            # Claimed before they land (into the folder, or with a new folder): a run
            # that fails or dies has exactly these files taken back, and no other
            # (ADR 008).
            for part in parts:
                runs.claim(os.path.join(local, part))
        if exists and save_mode == "append":
            for part in parts:
                target = os.path.join(local, part)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                os.replace(os.path.join(staging, part), target)
                runs.landed(target)
            _touch(os.path.join(local, "_SUCCESS"))
            return
        _touch(os.path.join(staging, "_SUCCESS"))
        if not exists:
            os.replace(staging, local)
            if save_mode == "append":
                for part in parts:
                    runs.landed(os.path.join(local, part))
            return
        old = staging + ".old"
        os.replace(local, old)
        try:
            os.replace(staging, local)
        except BaseException:
            os.replace(old, local)
            raise
        if os.path.isdir(old):
            shutil.rmtree(old, ignore_errors=True)
        else:
            os.remove(old)
    finally:
        if os.path.exists(staging):
            shutil.rmtree(staging, ignore_errors=True)


def _replace_partitions(local: str, write: Any) -> None:
    """Spark's dynamic partition overwrite: replace only the partitions written.

    Each leaf partition folder the new data fills (``p=2/q=a``) replaces the
    folder of the same name; every other partition, any stray file and the
    root ``_SUCCESS`` are left as they are, and a new target gets an empty folder
    and no ``_SUCCESS`` (as in Spark). The leaves are written to a hidden staging
    folder first; then, one leaf at a time, the old folder is moved aside (to
    ``<staging>.old/<leaf path>``), the new one moved in, and only when every
    leaf is in are the old ones deleted. If a move fails, the leaves already
    moved are put back. An old folder that cannot be put back is never deleted:
    the error names where it is.

    A hard kill between moving a leaf aside and moving the new one in leaves
    that partition missing, with its old files in the ``.old`` folder (Spark's
    own commit has the same window). The next write warns about such folders;
    rerunning the batch writes the partition again.
    """
    import shutil

    local = os.path.normpath(os.path.abspath(local))
    if os.path.exists(local) and not os.path.isdir(local):
        raise _refuse(
            f"Cannot replace partitions in {local}: it is a single file, not a folder.",
            path=local,
        )
    # Spark stages inside the target, so even an empty frame leaves the folder.
    os.makedirs(local, exist_ok=True)
    staging = _staging(local)
    aside = staging + ".old"
    swapped: List[Any] = []  # (target, where its old folder went, or None)
    kept: List[str] = []  # old folders that could not be put back
    try:
        parts = write(staging)
        for leaf in sorted({os.path.dirname(p) for p in parts}):
            target = os.path.join(local, leaf)
            old = None
            if os.path.lexists(target):
                old = os.path.join(aside, leaf)
                os.makedirs(os.path.dirname(old), exist_ok=True)
                os.replace(target, old)
            try:
                os.makedirs(os.path.dirname(target), exist_ok=True)
                os.replace(os.path.join(staging, leaf), target)
            except BaseException:
                if old is not None:
                    try:
                        os.replace(old, target)
                    except OSError as exc:
                        kept.append(old)
                        logger.error(
                            "Could not put back the partition %s (%s); its old files are "
                            "kept in %s.",
                            target,
                            exc,
                            old,
                        )
                raise
            swapped.append((target, old))
    except BaseException as failure:
        for target, old in reversed(swapped):
            try:
                shutil.rmtree(target)
                if old is not None:
                    os.replace(old, target)
            except OSError as exc:
                if old is not None:
                    kept.append(old)
                logger.error(
                    "Could not put back the partition %s (%s); its old files are kept in %s.",
                    target,
                    exc,
                    old or "(it had none)",
                )
        if kept:
            raise SinkWriteError(
                f"The partition overwrite of {local} failed ({failure}), and "
                f"{len(kept)} old partition folder(s) could not be put back. Their files "
                f"are kept, not deleted, in: {', '.join(kept)}",
                context={"Backend": "pandas", "path": local, "Kept": kept},
                hint="Move each kept folder back to its place under the target (the "
                "path after '.old' is the partition), or rerun the batch.",
            ) from failure
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        if not kept and os.path.exists(aside):
            shutil.rmtree(aside, ignore_errors=True)


def _write_tree(
    table: Any,
    cut: Optional["pandas_partitions.Split"],
    fmt: str,
    folder: str,
    opts: Dict[str, Any],
    timezone: str,
) -> List[str]:
    """Write ``table`` into ``folder`` as Spark lays it out; the files, relative to ``folder``.

    Unpartitioned (no ``cut``), one part file. Partitioned, one part file in
    each leaf partition folder (Spark's ``part-00000-<uuid>.c000`` name, one uuid
    per write), and none at all for an empty frame.
    """
    import uuid

    if cut is None:
        return [_write_part(table, fmt, folder, opts, timezone)]
    stem = f"part-00000-{uuid.uuid4()}.c000"
    files = []
    for leaf, rows in cut.leaves:
        where = os.path.join(folder, leaf)
        os.makedirs(where, exist_ok=True)
        name = _write_part(cut.data.take(rows), fmt, where, opts, timezone, stem=stem)
        files.append(os.path.join(leaf, name))
    return files


def execute_write(
    df: Any,
    resolved: ResolvedWriteMode,
    *,
    connector: str,
    file_format: str,
    table: Optional[str] = None,
    path: Optional[str] = None,
    partition_by: Optional[Sequence[str]] = None,
    options: Optional[Dict[str, Any]] = None,
    timezone: str = "UTC",
) -> None:
    """Write a frame to ``path`` as Spark would: a folder of part files.

    ``overwrite`` replaces the folder, ``append`` adds part files,
    ``errorifexists`` and ``ignore`` do what they say, and
    ``overwrite_partitions`` replaces only the partitions the frame fills.
    ``partition_by`` writes Spark's ``name=value`` folders (F-012). merge,
    managed tables and unknown options are refused.
    """
    if table and not path:
        raise _refuse(
            "The pandas backend writes to paths, not managed tables.",
            connector=connector,
            table=table,
        )
    if not path:
        raise _refuse("Nothing to write to: no path.", connector=connector)
    # resolve() maps merge to save_mode "overwrite" for a first run, so honouring
    # only save_mode would turn a merge into a full overwrite. Refuse it.
    if resolved.is_merge or resolved.options:
        raise SinkWriteError(
            f"The pandas backend does not support write mode '{resolved.mode}'"
            + (f" with {sorted(resolved.options)}." if resolved.options else "."),
            context={"Backend": "pandas", "connector": connector, "mode": resolved.mode},
            hint="merge and replace_where are Delta modes. Use the Spark backend, or "
            "append, overwrite or overwrite_partitions with partitionBy.",
        )

    fmt = (file_format or "parquet").lower()
    if fmt not in SUPPORTED_FORMATS:
        raise SinkWriteError(
            f"The pandas backend cannot write file_format '{fmt}'.",
            context={
                "Backend": "pandas",
                "file_format": fmt,
                "Supported": sorted(SUPPORTED_FORMATS),
            },
            hint="Write csv/parquet/json, or use the Spark backend.",
        )
    problems = write_problems(fmt, options)
    if problems:
        raise SinkWriteError(
            problems[0],
            context={"Backend": "pandas", "Supported": sorted(WRITE_OPTIONS[fmt]) or "none"},
            hint="Remove the option, or run this task on the Spark backend.",
        )
    opts = {str(k).lower(): v for k, v in (options or {}).items()}

    if fmt == "parquet":
        _parquet_codec(opts)  # refused before anything is written

    local = _local_path(path, error=SinkWriteError)
    arrow = to_arrow(df, timezone)
    # Partition columns and values are checked before anything is written, as
    # Spark checks them before its job starts (F-012).
    cut = pandas_partitions.split(arrow, partition_by, timezone) if partition_by else None
    keeps = resolved.is_overwrite_partitions or resolved.save_mode == "append"
    if cut is not None and keeps and os.path.isdir(local):
        # What stays must not meet a new folder that differs from it only in case.
        pandas_partitions.check_existing(local, [leaf for leaf, _ in cut.leaves])

    def write(folder: str) -> List[str]:
        return _write_tree(arrow, cut, fmt, folder, opts, timezone)

    try:
        if resolved.is_overwrite_partitions and cut is not None:
            _replace_partitions(local, write)  # Spark's dynamic partition overwrite
        else:
            # overwrite_partitions without partitionBy is a plain overwrite, as in Spark.
            _commit(local, resolved.save_mode, write)
    except (SinkWriteError, RunLeaseLost):
        raise
    except Exception as exc:  # pyarrow and filesystem errors, with the path named
        raise SinkWriteError(
            f"The pandas backend could not write {fmt} to {path}: {exc}",
            context={"Backend": "pandas", "path": local, "file_format": fmt},
        ) from exc
