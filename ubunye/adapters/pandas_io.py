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
        }
    ),
    "json": frozenset({"multiline", "encoding"}),
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


def _check_options(fmt: str, options: Dict[str, Any], allowed: frozenset) -> None:
    """Refuse any option not in ``allowed``, named as the user spelled it."""
    unknown = sorted(str(k) for k in options if str(k).lower() not in allowed)
    if unknown:
        raise SourceReadError(
            f"The pandas backend does not support the {fmt} option(s) {unknown}.",
            context={
                "Backend": "pandas",
                "file_format": fmt,
                "Supported": sorted(allowed) or "none",
            },
            hint="Remove the option, or run this task on the Spark backend.",
        )


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


def _apply_schema(table: Any, schema: Any) -> Any:
    """Select and cast to an explicit schema; a missing column is all null."""
    import pyarrow as pa

    columns = [
        (
            table.column(f.name).cast(f.type)
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
    encoding = str(opts.get("encoding", "utf8"))
    parse = pcsv.ParseOptions(
        delimiter=str(opts.get("sep") or opts.get("delimiter") or ","),
        quote_char=str(opts.get("quote", '"')) or False,
        escape_char=str(opts["escape"]) if opts.get("escape") else False,
        newlines_in_values=_truthy(opts.get("multiline", "false")),
    )

    tables = []
    for f in files:
        if schema is not None:
            # Spark: an explicit schema names the columns by position, and the
            # header line (if any) is skipped.
            names = list(schema.names)
        else:
            probe = pcsv.open_csv(
                f,
                read_options=pcsv.ReadOptions(
                    encoding=encoding, autogenerate_column_names=not header
                ),
                parse_options=parse,
            )
            names = list(probe.schema.names)
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
        tables.append(
            pcsv.read_csv(f, read_options=read, parse_options=parse, convert_options=convert)
        )

    table = pa.concat_tables(tables, promote_options="permissive")
    if schema is not None:
        return _instants(_apply_schema(table, schema), timezone)
    table = _to_spark_types(table)
    if infer:
        table = _narrow_ints(table)
    return _instants(table, timezone)


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
                rows.extend(json.loads(line) for line in handle if line.strip())
    # Parsed with the json module, not pyarrow's reader: pyarrow turns ISO text
    # into timestamps and keeps key order, and Spark does neither.
    rows = [_sorted_keys(r) for r in rows]
    names = sorted({k for r in rows for k in r})
    table = pa.table({n: pa.array([r.get(n) for r in rows]) for n in names})
    if schema is not None:
        return _instants(_apply_schema(table, schema), timezone)
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
    if fmt not in SUPPORTED_FORMATS:
        raise SourceReadError(
            f"The pandas backend cannot read file_format '{fmt}'.",
            context={
                "Backend": "pandas",
                "file_format": fmt,
                "Supported": sorted(SUPPORTED_FORMATS),
            },
            hint="Read csv/parquet/json with pandas, or use the Spark backend for "
            "delta/orc/avro.",
        )
    _check_options(fmt, options or {}, READ_OPTIONS[fmt])
    opts = {str(k).lower(): v for k, v in (options or {}).items()}

    arrow_schema = None
    if schema:
        try:
            arrow_schema = ddl.parse(schema)
        except ValueError as exc:
            raise SourceReadError(
                f"The pandas backend cannot use this schema: {exc}.",
                context={"Backend": "pandas", "schema": schema},
                hint="Use flat scalar types (INT, BIGINT, DOUBLE, STRING, DATE, "
                "TIMESTAMP, DECIMAL(p,s) ...), or the Spark backend.",
            ) from None

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


def _read_existing(pd: Any, fmt: str, local: str) -> Any:
    if fmt == "csv":
        return pd.read_csv(local)
    if fmt == "parquet":
        return pd.read_parquet(local)
    return pd.read_json(local)


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
) -> None:
    """Write a pandas frame to ``path``, honouring the native save modes."""
    import pandas as pd

    if table and not path:
        raise SinkWriteError(
            "The pandas backend writes to paths, not managed tables.",
            context={"Backend": "pandas", "connector": connector, "table": table},
            hint="Use a path output, or the Spark backend for catalog tables.",
        )
    if not path:
        raise SinkWriteError(
            "Nothing to write to — no path.",
            context={"Backend": "pandas", "connector": connector},
            hint="Set a path on this output.",
        )
    if resolved.is_merge or resolved.is_overwrite_partitions:
        raise SinkWriteError(
            f"The pandas backend does not support write mode '{resolved.mode}'.",
            context={"Backend": "pandas", "connector": connector, "mode": resolved.mode},
            hint="merge and overwrite_partitions are lakehouse modes — use the Spark "
            "backend, or a native mode (append / overwrite).",
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

    frame = df.native if isinstance(df, PandasDataFrameAdapter) else df
    local = _local_path(path, error=SinkWriteError)
    save_mode = resolved.save_mode
    exists = os.path.exists(local)

    if exists and save_mode == "ignore":
        return
    if exists and save_mode == "errorifexists":
        raise SinkWriteError(
            f"Target already exists: {local}",
            context={"Backend": "pandas", "path": local},
            hint="Use mode: overwrite or append.",
        )
    if save_mode == "append" and exists:
        frame = pd.concat([_read_existing(pd, fmt, local), frame], ignore_index=True)

    parent = os.path.dirname(local)
    if parent:
        os.makedirs(parent, exist_ok=True)

    if fmt == "csv":
        frame.to_csv(local, index=False)
    elif fmt == "parquet":
        frame.to_parquet(local, index=False)
    else:  # json
        frame.to_json(local, orient="records")
