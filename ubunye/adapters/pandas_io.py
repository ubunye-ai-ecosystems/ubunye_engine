"""Pandas data-plane IO behind the Backend seam (issue #38).

No Spark, no JVM. This is what lets a task read a path, flow through the engine,
and write a path with nothing but Python. Only the generic path-based file
formats are supported (csv, parquet, json); lakehouse formats (delta) and
managed tables are Spark's job, and their connectors declare so.

A pandas frame is returned wrapped in :class:`PandasDataFrameAdapter` so it
satisfies ``DataFramePort`` (``schema`` / ``count`` / ``collect``) exactly like
a Spark DataFrame does — the engine core cannot tell them apart.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Sequence

from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter
from ubunye.core.errors import SinkWriteError, SourceReadError
from ubunye.core.write_modes import ResolvedWriteMode

SUPPORTED_FORMATS = frozenset({"csv", "parquet", "json"})


def _local_path(path: str) -> str:
    """Strip a ``file://`` scheme to a local filesystem path.

    pandas has no Hadoop filesystem: it reads and writes local paths (and, with
    fsspec, remote URLs). A ``file:///C:/x`` or ``file:///tmp/x`` URI is turned
    back into the plain local path the OS understands.
    """
    if path.startswith("file://"):
        path = path[len("file://") :]
        # file:///C:/x -> /C:/x; drop the leading slash before a Windows drive.
        if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
            path = path[1:]
    return path


def read_frame(
    file_format: str,
    path: str,
    *,
    options: Optional[Dict[str, Any]] = None,
    schema: Optional[str] = None,
) -> PandasDataFrameAdapter:
    """Read a path into a pandas frame, wrapped as a ``DataFramePort``."""
    import pandas as pd

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

    local = _local_path(path)
    opts = {str(k).lower(): v for k, v in (options or {}).items()}

    if fmt == "csv":
        kw: Dict[str, Any] = {}
        # Spark's 'header' is "true"/"false"; pandas wants a row index or None.
        kw["header"] = 0 if str(opts.get("header", "true")).lower() == "true" else None
        sep = opts.get("sep") or opts.get("delimiter")
        if sep:
            kw["sep"] = sep
        frame = pd.read_csv(local, **kw)
    elif fmt == "parquet":
        frame = pd.read_parquet(local)
    else:  # json
        frame = pd.read_json(local)

    return PandasDataFrameAdapter(frame)


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
    """Write a pandas frame to ``path``, honouring the native save modes.

    Supports ``append`` / ``overwrite`` / ``errorifexists`` / ``ignore`` — the
    modes a plain file target can mean. ``merge`` and ``overwrite_partitions``
    are lakehouse modes the pandas backend does not offer; the s3 pandas writer
    declares only the native modes as supported, so ``resolve()`` rejects the
    others before a write is ever attempted.
    """
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

    # merge / overwrite_partitions are lakehouse modes. resolve() maps both to a
    # save_mode of "overwrite" for the create-on-first-run case, so honouring only
    # save_mode here would silently turn a merge into a full overwrite. Refuse
    # them explicitly instead — the pandas backend does not do lakehouse writes.
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
    local = _local_path(path)
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
