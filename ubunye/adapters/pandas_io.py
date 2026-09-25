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
* A path may be a file, a folder of part files (Spark's layout; files starting
  with ``_`` or ``.`` are skipped, as Spark skips them) or a glob.
* Timestamp text is read in the backend's timezone (``UTC`` unless set), the
  way Spark reads it in ``spark.sql.session.timeZone``.

pyarrow does the IO, and the pandas frame handed to the task is Arrow-backed
(``pd.ArrowDtype``), so a nullable integer stays an integer and a decimal stays
a decimal, as in Spark. The frame is wrapped in :class:`PandasDataFrameAdapter`
so it satisfies ``DataFramePort`` exactly as a Spark DataFrame does.
"""

from __future__ import annotations

import glob
import json
import os
from typing import Any, Dict, List, Optional, Sequence

from ubunye.adapters import ddl
from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter
from ubunye.core.errors import SinkWriteError, SourceReadError
from ubunye.core.write_modes import ResolvedWriteMode

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
    """The files behind a path: the file itself, a folder's data files, or a glob."""
    if glob.has_magic(local):
        candidates = sorted(glob.glob(local))
    elif os.path.isdir(local):
        candidates = sorted(os.path.join(local, n) for n in os.listdir(local))
    elif os.path.isfile(local):
        return [local]
    else:
        candidates = []
    return [
        f
        for f in candidates
        if os.path.isfile(f) and not os.path.basename(f).startswith(("_", "."))
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


def _instants(table: Any, timezone: str) -> Any:
    """Wall-clock timestamps read as instants in ``timezone``, held in UTC."""
    import pyarrow as pa
    import pyarrow.compute as pc

    for i, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type):
            col = table.column(i)
            if field.type.tz is None:
                col = pc.assume_timezone(col, timezone=timezone)
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
            local = pc.assume_timezone(naive, timezone=timezone).cast(pa.timestamp("us", tz="UTC"))
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


def _read_csv(files: List[str], opts: Dict[str, Any], schema: Any, timezone: str) -> Any:
    import pyarrow as pa
    import pyarrow.csv as pcsv

    header = _truthy(opts.get("header", "false"))
    infer = _truthy(opts.get("inferschema", "false")) and schema is None
    dialect = _CsvDialect(opts)

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
            tables.append(
                pcsv.read_csv(
                    pa.BufferReader(data),
                    read_options=read,
                    parse_options=parse,
                    convert_options=convert,
                )
            )
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
            tables.append(
                pcsv.read_csv(
                    evened, read_options=read, parse_options=parse, convert_options=convert
                )
            )

    table = pa.concat_tables(tables, promote_options="permissive")
    if schema is not None:
        return _instants(_apply_schema(table, schema, timezone), timezone)
    table = _to_spark_types(table)
    if infer:
        table = _narrow_ints(table)
    return _instants(table, timezone)


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
    text = data.decode(_text_encoding(encoding), errors="replace")
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


def _first_record(data: bytes, encoding: str, parse: Any) -> List[str]:
    """The first non blank record of a CSV file's bytes, as its fields."""
    import csv

    with _text(data, encoding) as handle:
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
    with _text(data, encoding) as handle:
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


def _sorted_keys(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _sorted_keys(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_sorted_keys(v) for v in value]
    return value


def _read_json(files: List[str], opts: Dict[str, Any], schema: Any, timezone: str) -> Any:
    import pyarrow as pa

    encoding = str(opts.get("encoding", "utf-8"))
    rows: List[Dict[str, Any]] = []
    for f in files:
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
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        if not drop:
                            raise
                        # DROPMALFORMED: skip the record, as Spark does
    # Parsed with the json module, not pyarrow's reader: pyarrow turns ISO text
    # into timestamps and keeps key order, and Spark does neither.
    rows = [_sorted_keys(r) for r in rows]
    names = sorted({k for r in rows for k in r})
    table = pa.table({n: pa.array([r.get(n) for r in rows]) for n in names})
    if schema is not None:
        return _instants(_apply_schema(table, schema, timezone), timezone)
    return _to_spark_types(table)


def _read_parquet(files: List[str], schema: Any) -> Any:
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
    table = pa.concat_tables(tables, promote_options="permissive")
    return _apply_schema(table, schema) if schema is not None else table


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
    files = _data_files(local)
    if not files:
        raise SourceReadError(
            f"Path does not exist or holds no data files: {path}",
            context={"Backend": "pandas", "path": local},
            hint="Check the path. A folder must hold data files, not only _ or . files.",
        )

    try:
        if fmt == "csv":
            table = _read_csv(files, opts, arrow_schema, timezone)
        elif fmt == "json":
            table = _read_json(files, opts, arrow_schema, timezone)
        else:
            table = _read_parquet(files, arrow_schema)
    except Exception as exc:  # pyarrow and json errors, with the path named
        raise SourceReadError(
            f"The pandas backend could not read {fmt} at {path}: {exc}",
            context={"Backend": "pandas", "path": local, "file_format": fmt},
        ) from exc

    return PandasDataFrameAdapter(to_pandas(table))


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
                import pyarrow.compute as pc

                col = pc.assume_timezone(col, timezone=timezone)
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


def _json_value(value: Any) -> str:
    """One value as Spark's JSON writer (Jackson) writes it.

    Compact, no spaces; doubles the Java way (``1.0E10``), with NaN and the
    infinities as strings; null fields of an object left out (Spark's
    ``ignoreNullFields``) but nulls inside an array kept; text as UTF-8.
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
    if isinstance(value, dict):
        members = (
            f"{json.dumps(str(k), ensure_ascii=False)}:{_json_value(v)}"
            for k, v in value.items()
            if v is not None
        )
        return "{" + ",".join(members) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_json_value(v) for v in value) + "]"
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (dt.date, dt.time)):
        return json.dumps(value.isoformat())
    if isinstance(value, bytes):
        return json.dumps(base64.b64encode(value).decode("ascii"))  # as Spark writes binary
    raise TypeError(f"cannot write {type(value).__name__} to JSON")


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


def _write_part(table: Any, fmt: str, folder: str, opts: Dict[str, Any], timezone: str) -> str:
    """Write one ``part-00000-<uuid>-c000.<ext>`` file into ``folder``."""
    import uuid

    stem = f"part-00000-{uuid.uuid4()}-c000"
    if fmt == "parquet":
        import pyarrow.parquet as pq

        codec_name = str(opts.get("compression", "snappy")).lower()
        if codec_name not in _PARQUET_CODECS:
            raise _refuse(
                f"The pandas backend cannot write parquet compression '{codec_name}'.",
                Supported=sorted(_PARQUET_CODECS),
            )
        codec = _PARQUET_CODECS[codec_name]
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

    name = f"{stem}.json"
    with open(os.path.join(folder, name), "w", encoding="utf-8", newline="\n") as handle:
        for batch in _shortest_floats(table).to_batches():
            for row in batch.to_pylist():
                handle.write(_json_value(row))
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


def _commit(local: str, save_mode: str, write: Any) -> None:
    """Put a freshly written part file in place, the way ``save_mode`` asks.

    The part is written into a hidden staging folder beside the target first,
    so a failed write never touches data already there: ``overwrite`` swaps the
    staged folder in only once it is complete, ``append`` moves the one new part
    file into the existing folder.
    """
    import shutil
    import uuid

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

    parent, base = os.path.split(local)
    os.makedirs(parent, exist_ok=True)
    staging = os.path.join(parent, f".{base}.ubunye-{uuid.uuid4().hex[:12]}")
    os.makedirs(staging)
    try:
        part = write(staging)
        if exists and save_mode == "append":
            os.replace(os.path.join(staging, part), os.path.join(local, part))
            _touch(os.path.join(local, "_SUCCESS"))
            return
        _touch(os.path.join(staging, "_SUCCESS"))
        if not exists:
            os.replace(staging, local)
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

    ``overwrite`` replaces the folder, ``append`` adds a part file,
    ``errorifexists`` and ``ignore`` do what they say. Lakehouse modes,
    partitioned writes, managed tables and unknown options are refused.
    """
    if table and not path:
        raise _refuse(
            "The pandas backend writes to paths, not managed tables.",
            connector=connector,
            table=table,
        )
    if not path:
        raise _refuse("Nothing to write to: no path.", connector=connector)
    # resolve() maps merge and overwrite_partitions to save_mode "overwrite" for
    # a first run, so honouring only save_mode would turn a merge into a full
    # overwrite. They are lakehouse modes; refuse them.
    if resolved.is_merge or resolved.is_overwrite_partitions:
        raise SinkWriteError(
            f"The pandas backend does not support write mode '{resolved.mode}'.",
            context={"Backend": "pandas", "connector": connector, "mode": resolved.mode},
            hint="merge and overwrite_partitions are lakehouse modes. Use the Spark "
            "backend, or a native mode (append / overwrite).",
        )
    if partition_by:
        raise SinkWriteError(
            "The pandas backend does not write partitioned folders (partition_by).",
            context={"Backend": "pandas", "partition_by": list(partition_by)},
            hint="Remove partition_by, or use the Spark backend.",
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

    local = _local_path(path, error=SinkWriteError)
    arrow = to_arrow(df, timezone)
    try:
        _commit(
            local,
            resolved.save_mode,
            lambda folder: _write_part(arrow, fmt, folder, opts, timezone),
        )
    except SinkWriteError:
        raise
    except Exception as exc:  # pyarrow and filesystem errors, with the path named
        raise SinkWriteError(
            f"The pandas backend could not write {fmt} to {path}: {exc}",
            context={"Backend": "pandas", "path": local, "file_format": fmt},
        ) from exc
