"""The version of an input's source: taken when it is read, and again after its hash.

On Spark an input's hash reads the source again at task end (F-046). If the
source changed in between, the digest is of a later state than the one the task
read. The run record cannot stop that, but it can say so. So the engine takes the
source's own version right after the read, and the recorder takes it again right
after the hash. Equal: the digest is of what was read. Different: it is not.

Two kinds of version, both cheap, neither reads the data:

- ``delta``: the table version (and its time) from the Delta log. A read pinned
  with ``version_as_of`` or ``timestamp_as_of`` is that version, which never changes.
- ``files``: the files the frame reads (Spark's own file index, ``inputFiles()``;
  the list the pandas backend read), with each file's size and modification time.
  Recorded as a count, total bytes, the latest time and one hash of the sorted
  ``(relative path, size, time)`` list. A file added to the folder after the read
  is not in the frame's file index, so the hash does not read it either; a file
  rewritten or deleted is.

Anything else (a SQL query, a JDBC table, a catalog table that is not Delta) is
``none`` with the reason, so the record never claims more than it knows.
"""

from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import unquote

DELTA, FILES, NONE = "delta", "files", "none"

#: A file's (size in bytes, modification time in ms since the epoch), or None
#: when the file is gone.
Stat = Optional[Tuple[int, int]]


class _NoVersion(Exception):
    """This source has no version Ubunye can read; the message says why."""


def _none(reason: str) -> Dict[str, Any]:
    return {"kind": NONE, "reason": reason}


# --- files -----------------------------------------------------------------------------


def _iso_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _relative(paths: Sequence[str]) -> List[str]:
    """Each path after the folder they all share, so the hash names no machine."""
    if len(paths) == 1:
        return [paths[0].rsplit("/", 1)[-1]]
    common = os.path.commonprefix(list(paths))
    cut = common.rfind("/") + 1
    return [p[cut:] for p in paths]


def files_version(stats: Dict[str, Stat]) -> Dict[str, Any]:
    """A files version from each file's (size, time); a missing file counts as missing."""
    paths = sorted(stats)
    rel = _relative([p.replace("\\", "/") for p in paths]) if paths else []
    digest = hashlib.sha256()
    total, latest, missing = 0, 0, 0
    for path, name in zip(paths, rel):
        stat = stats[path]
        if stat is None:
            missing += 1
            digest.update(f"{name}\tmissing\n".encode("utf-8"))
            continue
        size, mtime = stat
        total += size
        latest = max(latest, mtime)
        digest.update(f"{name}\t{size}\t{mtime}\n".encode("utf-8"))
    version: Dict[str, Any] = {
        "kind": FILES,
        "files": len(paths),
        "bytes": total,
        "latest_modified": _iso_ms(latest) if latest else None,
        "listing_hash": "sha256:" + digest.hexdigest(),
    }
    if missing:
        version["missing"] = missing
    return version


def _local_stats(paths: Sequence[str]) -> Dict[str, Stat]:
    out: Dict[str, Stat] = {}
    for path in paths:
        try:
            st = os.stat(path)
            out[path] = (int(st.st_size), int(st.st_mtime_ns // 1_000_000))
        except OSError:
            out[path] = None
    return out


def _hadoop_stats(frame: Any, paths: Sequence[str]) -> Dict[str, Stat]:
    """Each file's size and time from Hadoop, one listing per folder, no contents read."""
    spark = frame.sparkSession
    jvm, jsc = getattr(spark, "_jvm", None), getattr(spark, "_jsc", None)
    if jvm is None or jsc is None:
        raise _NoVersion("file sizes and times cannot be read without the JVM (Spark Connect)")
    conf = jsc.hadoopConfiguration()
    by_folder: Dict[str, Dict[str, str]] = {}
    for path in paths:
        folder, _, name = path.rpartition("/")
        by_folder.setdefault(folder, {})[unquote(name)] = path
    out: Dict[str, Stat] = {p: None for p in paths}
    for folder, names in by_folder.items():
        jpath = jvm.org.apache.hadoop.fs.Path(jvm.java.net.URI(folder))
        try:
            listed = jpath.getFileSystem(conf).listStatus(jpath)
        except Exception:  # noqa: BLE001 - the folder is gone: every file in it is missing
            continue
        for status in listed:
            path = names.get(status.getPath().getName())
            if path is not None:
                out[path] = (int(status.getLen()), int(status.getModificationTime()))
    return out


# --- delta -----------------------------------------------------------------------------


def _delta_target(io_cfg: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """(the SQL name of the Delta table, or None; a pinned version, when the read is pinned)."""
    fmt = io_cfg.get("format", "")
    if io_cfg.get("sql"):
        raise _NoVersion("a SQL query input has no single source version")
    if fmt == "delta":
        pin = None
        if io_cfg.get("version_as_of") is not None:
            pin = {"kind": DELTA, "version": int(io_cfg["version_as_of"]), "pinned": True}
        elif io_cfg.get("timestamp_as_of") is not None:
            pin = {"kind": DELTA, "timestamp_as_of": str(io_cfg["timestamp_as_of"]), "pinned": True}
        table = io_cfg.get("table")
        if not table and io_cfg.get("db_name") and io_cfg.get("tbl_name"):
            table = f"{io_cfg['db_name']}.{io_cfg['tbl_name']}"
        return (table or f"delta.`{io_cfg.get('path', '')}`"), pin
    if fmt == "s3" and str(io_cfg.get("file_format", "")).lower() == "delta":
        return f"delta.`{io_cfg.get('path', '')}`", None
    return None, None


def _catalog_table(io_cfg: Dict[str, Any]) -> Optional[str]:
    """The table name of a hive or unity input (a catalog table), or None."""
    if io_cfg.get("format") not in ("hive", "unity"):
        return None
    if io_cfg.get("sql"):
        raise _NoVersion("a SQL query input has no single source version")
    if io_cfg.get("table"):
        return str(io_cfg["table"])
    if io_cfg.get("catalog") and io_cfg.get("schema") and io_cfg.get("tbl_name"):
        return f"{io_cfg['catalog']}.{io_cfg['schema']}.{io_cfg['tbl_name']}"
    if io_cfg.get("db_name") and io_cfg.get("tbl_name"):
        return f"{io_cfg['db_name']}.{io_cfg['tbl_name']}"
    return None


def _delta_version(spark: Any, table: str) -> Dict[str, Any]:
    """The table's latest version and its time, from the Delta log (no data read)."""
    row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]
    stamp = row["timestamp"]
    return {
        "kind": DELTA,
        "version": int(row["version"]),
        "timestamp": stamp.isoformat() if hasattr(stamp, "isoformat") else str(stamp),
    }


# --- the one entry point ----------------------------------------------------------------


def capture(
    frame: Any,
    io_cfg: Dict[str, Any],
    *,
    hadoop_stats: Optional[Callable[[Any, Sequence[str]], Dict[str, Stat]]] = None,
) -> Optional[Dict[str, Any]]:
    """The source's version now, as a JSON-safe dict; never raises.

    ``None`` when the frame is neither a Spark frame nor a pandas frame read from
    files (a test double): there is nothing to say. ``seconds`` is how long it took.
    """
    t0 = time.perf_counter()
    try:
        version = _capture(frame, io_cfg or {}, hadoop_stats or _hadoop_stats)
    except _NoVersion as exc:
        version = _none(str(exc))
    except Exception as exc:  # noqa: BLE001 - a record is never worth a failed run
        version = _none(f"could not be read: {type(exc).__name__}: {str(exc)[:200]}")
    if version is not None:
        version["seconds"] = round(time.perf_counter() - t0, 6)
    return version


def _capture(frame: Any, io_cfg: Dict[str, Any], hadoop_stats: Callable) -> Optional[Dict]:
    local = getattr(frame, "source_files", None)
    if isinstance(local, list):  # the pandas backend's read: the files it read
        return files_version(_local_stats(local))
    spark = getattr(frame, "sparkSession", None)
    if spark is None or not callable(getattr(frame, "inputFiles", None)):
        return None
    table, pin = _delta_target(io_cfg)
    if pin is not None:
        return pin
    if table is not None:
        return _delta_version(spark, table)
    catalog = _catalog_table(io_cfg)
    if catalog is not None:
        try:
            return _delta_version(spark, catalog)
        except Exception as exc:  # noqa: BLE001
            raise _NoVersion(
                "a catalog table that is not Delta has no version "
                f"({type(exc).__name__}: {str(exc)[:120]})"
            ) from exc
    paths = sorted(set(frame.inputFiles()))
    if not paths:
        raise _NoVersion(f"a '{io_cfg.get('format', '')}' input reads no files")
    return files_version(hadoop_stats(frame, paths))


# --- the verdict --------------------------------------------------------------------------

#: The fields that identify a version; ``seconds`` (the cost) is not one of them.
_IDENTITY = {
    DELTA: ("version", "timestamp_as_of"),
    FILES: ("files", "bytes", "latest_modified", "listing_hash", "missing"),
}


def comparable(version: Optional[Dict[str, Any]]) -> bool:
    """Whether a version can be checked again (a Delta version or a file listing)."""
    return bool(version) and version.get("kind") in _IDENTITY


def changed(before: Dict[str, Any], after: Optional[Dict[str, Any]]) -> Optional[bool]:
    """True when the source moved between the two; None when that is not known."""
    if not comparable(before) or not comparable(after) or before["kind"] != after["kind"]:
        return None
    return any(before.get(k) != after.get(k) for k in _IDENTITY[before["kind"]])


def _describe(version: Dict[str, Any]) -> str:
    if version.get("kind") == DELTA:
        if "version" in version:
            return f"Delta version {version['version']}"
        return f"Delta as of {version.get('timestamp_as_of')}"
    text = f"{version.get('files')} files, {version.get('bytes')} bytes"
    if version.get("missing"):
        text += f", {version['missing']} missing"
    return text


def note(
    basis: Optional[str],
    before: Optional[Dict[str, Any]],
    after: Optional[Dict[str, Any]],
    moved: Optional[bool],
) -> Optional[str]:
    """One plain sentence on whether an input's digest is of what the task read."""
    if basis != "recomputed" or not before:
        return None
    if moved is True:
        return (
            f"The source changed between the read and the hash ({_describe(before)} -> "
            f"{_describe(after or {})}). This digest is of the later state, not of what "
            "the task read."
        )
    if moved is False:
        return (
            f"The source did not change between the read and the hash ({_describe(before)}). "
            "This digest is of what the task read."
        )
    reason = before.get("reason") or (after or {}).get("reason") or "no version"
    return (
        f"This source has no version to check ({reason}), so whether it changed between "
        "the read and the hash is not known."
    )
