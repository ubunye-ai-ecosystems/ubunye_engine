"""The version of an input's source: taken when it is read, and again after its hash.

On Spark an input's hash reads the source again at task end (F-046). If the
source changed in between, the digest is of a later state than the one the task
read. The run record cannot stop that, but it can say so. So the engine takes the
source's own version right after the read, and the recorder checks it again right
after the hash.

Two kinds of version, both cheap, neither reads the data:

- ``delta``: the table version (and its time) from the Delta log. A Delta read is
  pinned to one version (by the config, or by the engine when the config names
  none: ``ubunye.adapters.spark.delta_pin``), so its digest is of what was read by
  construction; the table's latest version at the hash is recorded as information.
  An unpinned Delta table (a catalog table) that moved only by commits that change
  no rows (OPTIMIZE, VACUUM, table properties) is not called changed.
- ``files``: the files the frame reads (Spark's own file index, ``inputFiles()``;
  the list the pandas backend read), with each file's size, modification time and,
  where the file system gives one (S3A, ABFS), its content tag (etag). Recorded as a
  count, total bytes, the latest time and one hash of the sorted list. A file added
  to the folder after the read is not in the frame's file index, so the hash does
  not read it either; a file rewritten or deleted is. Without etags, "unchanged"
  means names, sizes and times only, and the record says so.

Anything else (a SQL query, a JDBC table, a catalog table that is not Delta, a
listing that failed or took longer than ``UBUNYE_SOURCE_VERSION_TIMEOUT`` seconds,
30 by default) is ``none`` with the reason, so the record never claims more than it
knows.
"""

from __future__ import annotations

import functools
import hashlib
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import unquote

DELTA, FILES, NONE = "delta", "files", "none"

#: A file's (size in bytes, modification time in ms since the epoch[, etag]), or
#: None when the file is gone.
Stat = Optional[Tuple[Any, ...]]

#: Up to this many files read, each is asked for its status; above it, each folder
#: is listed once. Asking per file costs one call per file read, never a whole folder.
PER_FILE_LIMIT = 1000

#: Delta operations that change no rows: a version they add is not a change of data.
NO_DATA_OPERATIONS = frozenset(
    {
        "OPTIMIZE",
        "VACUUM START",
        "VACUUM END",
        "SET TBLPROPERTIES",
        "UNSET TBLPROPERTIES",
        "ADD CONSTRAINT",
        "DROP CONSTRAINT",
    }
)


class _NoVersion(Exception):
    """This source has no version Ubunye can read; the message says why."""


def _none(reason: str) -> Dict[str, Any]:
    return {"kind": NONE, "reason": reason}


def timeout_seconds() -> float:
    """``UBUNYE_SOURCE_VERSION_TIMEOUT``: the most one version may take (30 s)."""
    try:
        return float(os.getenv("UBUNYE_SOURCE_VERSION_TIMEOUT", "30"))
    except ValueError:
        return 30.0


def _within(deadline: Optional[float]) -> None:
    if deadline is not None and time.perf_counter() > deadline:
        raise _NoVersion(f"taking it took longer than {timeout_seconds():g} s")


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
    """A files version from each file's (size, time[, etag]); a missing file counts as missing."""
    paths = sorted(stats)
    rel = _relative([p.replace("\\", "/") for p in paths]) if paths else []
    digest = hashlib.sha256()
    total, latest, missing, tagged = 0, 0, 0, 0
    for path, name in zip(paths, rel):
        stat = stats[path]
        if stat is None:
            missing += 1
            digest.update(f"{name}\tmissing\n".encode("utf-8"))
            continue
        size, mtime = int(stat[0]), int(stat[1])
        etag = stat[2] if len(stat) > 2 else None
        tagged += etag is not None
        total += size
        latest = max(latest, mtime)
        digest.update(f"{name}\t{size}\t{mtime}\t{etag or ''}\n".encode("utf-8"))
    present = len(paths) - missing
    version: Dict[str, Any] = {
        "kind": FILES,
        "files": len(paths),
        "bytes": total,
        "latest_modified": _iso_ms(latest) if latest else None,
        "listing_hash": "sha256:" + digest.hexdigest(),
        # True when every file present has a content tag (S3A, ABFS): then an
        # unchanged listing rules out a rewrite with the same size and time.
        "etags": bool(present) and tagged == present,
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
        except FileNotFoundError:
            out[path] = None
        except OSError as exc:
            raise _NoVersion(f"listing failed: {type(exc).__name__}: {exc}") from exc
    return out


def _not_found(exc: Exception) -> bool:
    """Whether a Hadoop error says the path does not exist (a definite answer)."""
    return "FileNotFoundException" in str(exc)


class _Etags:
    """Reads a status's content tag, and stops asking once the file system has none."""

    def __init__(self) -> None:
        self.supported = True

    def __call__(self, status: Any) -> Optional[str]:
        if not self.supported:
            return None
        try:
            tag = status.getEtag()
        except Exception:  # noqa: BLE001 - this FileStatus is no EtagSource
            self.supported = False
            return None
        return str(tag) if tag else None


def _hadoop_stats(
    frame: Any, paths: Sequence[str], deadline: Optional[float] = None
) -> Dict[str, Stat]:
    """Each file's size, time and etag from Hadoop; no contents read.

    Up to ``PER_FILE_LIMIT`` files, each is asked for its own status; above it, each
    folder is listed once. A path that does not exist is missing. Any other error
    (a throttle, a 503, a permission blip) is no version, never "missing".
    """
    spark = frame.sparkSession
    jvm, jsc = getattr(spark, "_jvm", None), getattr(spark, "_jsc", None)
    if jvm is None or jsc is None:
        raise _NoVersion("file sizes and times cannot be read without the JVM (Spark Connect)")
    conf = jsc.hadoopConfiguration()
    etag = _Etags()
    systems: Dict[str, Any] = {}

    def jpath_fs(uri: str) -> Tuple[Any, Any]:
        jpath = jvm.org.apache.hadoop.fs.Path(jvm.java.net.URI(uri))
        key = uri.split("/", 3)[:3]
        fs = systems.get(str(key))
        if fs is None:
            fs = systems[str(key)] = jpath.getFileSystem(conf)
        return jpath, fs

    out: Dict[str, Stat] = {p: None for p in paths}
    if len(paths) <= PER_FILE_LIMIT:
        for path in paths:
            _within(deadline)
            jpath, fs = jpath_fs(path)
            try:
                status = fs.getFileStatus(jpath)
            except Exception as exc:  # noqa: BLE001
                if _not_found(exc):
                    continue
                raise _NoVersion(f"listing failed: {str(exc)[:200]}") from exc
            out[path] = (int(status.getLen()), int(status.getModificationTime()), etag(status))
        return out

    by_folder: Dict[str, Dict[str, str]] = {}
    for path in paths:
        folder, _, name = path.rpartition("/")
        by_folder.setdefault(folder, {})[unquote(name)] = path
    for folder, names in by_folder.items():
        _within(deadline)
        jpath, fs = jpath_fs(folder)
        try:
            listed = fs.listStatus(jpath)
        except Exception as exc:  # noqa: BLE001
            if _not_found(exc):
                continue  # the folder is gone: every file read from it is missing
            raise _NoVersion(f"listing failed: {str(exc)[:200]}") from exc
        for status in listed:
            _within(deadline)
            hit = names.get(status.getPath().getName())
            if hit is not None:
                out[hit] = (int(status.getLen()), int(status.getModificationTime()), etag(status))
    return out


# --- delta -----------------------------------------------------------------------------


def _is_delta(io_cfg: Dict[str, Any]) -> bool:
    fmt = io_cfg.get("format", "")
    return fmt == "delta" or (fmt == "s3" and str(io_cfg.get("file_format", "")).lower() == "delta")


def _delta_table(io_cfg: Dict[str, Any]) -> Optional[str]:
    """The SQL name of a Delta input (by path or table), or None if it is not one."""
    if io_cfg.get("sql"):
        raise _NoVersion("a SQL query input has no single source version")
    if not _is_delta(io_cfg):
        return None
    if io_cfg.get("format") == "delta":
        table = io_cfg.get("table")
        if not table and io_cfg.get("db_name") and io_cfg.get("tbl_name"):
            table = f"{io_cfg['db_name']}.{io_cfg['tbl_name']}"
        if table:
            return str(table)
    return f"delta.`{io_cfg.get('path', '')}`"


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
    from ubunye.adapters.spark.delta_pin import latest

    return dict(latest(spark, table), kind=DELTA)


def _commits_since(spark: Any, table: str, since: int, until: int) -> List[str]:
    """The operations of the commits after version ``since`` up to ``until``."""
    rows = spark.sql(f"DESCRIBE HISTORY {table} LIMIT {max(until - since, 1)}").collect()
    found = [(int(r["version"]), str(r["operation"])) for r in rows]
    return [op for v, op in sorted(found) if since < v <= until]


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
    stats = hadoop_stats or functools.partial(_hadoop_stats, deadline=t0 + timeout_seconds())
    try:
        version = _capture(frame, io_cfg or {}, stats)
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
    table = _delta_table(io_cfg)
    if table is not None:
        from ubunye.adapters.spark.delta_pin import ATTR, user_pin

        pin = user_pin(io_cfg)
        if pin is not None:
            return dict(pin, kind=DELTA, pinned=True, pinned_by="config")
        engine = getattr(frame, ATTR, None)
        if isinstance(engine, dict):
            return dict(engine, kind=DELTA, pinned=True)
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


def check(
    frame: Any, io_cfg: Dict[str, Any], before: Optional[Dict[str, Any]]
) -> Tuple[Optional[Dict[str, Any]], Optional[bool]]:
    """The version after the hash, and whether the source moved since ``before``.

    A pinned Delta read cannot move: it is that version. Its table's latest version
    is recorded (``latest_version``) as information only. An unpinned Delta table
    that moved only by commits that change no rows is not called changed.
    """
    if before is None or not comparable(before):
        return None, None
    io_cfg = io_cfg or {}
    if before.get("kind") == DELTA and before.get("pinned"):
        t0 = time.perf_counter()
        pinned = {k: v for k, v in before.items() if k != "seconds"}
        try:
            table = _delta_table(io_cfg) or _catalog_table(io_cfg)
            if table is not None:
                pinned["latest_version"] = _delta_version(frame.sparkSession, table)["version"]
        except Exception:  # noqa: BLE001 - information only
            pass
        pinned["seconds"] = round(time.perf_counter() - t0, 6)
        return pinned, False
    after = capture(frame, io_cfg)
    moved = changed(before, after)
    if moved and before.get("kind") == DELTA and after is not None:
        try:
            table = _delta_table(io_cfg) or _catalog_table(io_cfg)
            ops = _commits_since(
                frame.sparkSession, str(table), int(before["version"]), int(after["version"])
            )
            after["commits_since_read"] = ops
            if ops and all(op in NO_DATA_OPERATIONS for op in ops):
                moved = False
        except Exception:  # noqa: BLE001 - cannot tell: keep "changed"
            pass
    return after, moved


# --- the verdict --------------------------------------------------------------------------

#: The fields that identify a version; ``seconds`` (the cost) is not one of them.
_IDENTITY = {
    DELTA: ("version", "timestamp_as_of"),
    FILES: ("files", "bytes", "latest_modified", "listing_hash", "missing"),
}


def comparable(version: Optional[Dict[str, Any]]) -> bool:
    """Whether a version can be checked again (a Delta version or a file listing)."""
    return version is not None and version.get("kind") in _IDENTITY


def changed(before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]]) -> Optional[bool]:
    """True when the source moved between the two; None when that is not known."""
    if before is None or after is None or not comparable(before) or not comparable(after):
        return None
    if before["kind"] != after["kind"]:
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
    after = after or {}
    if moved is True:
        return (
            f"The source changed between the read and the hash ({_describe(before)} -> "
            f"{_describe(after)}). This digest is of the later state, not of what "
            "the task read."
        )
    if moved is False and before.get("pinned"):
        by = "the engine" if before.get("pinned_by") == "engine" else "the config"
        text = (
            f"The read was pinned to {_describe(before)} (by {by}), so this digest is of "
            "what the task read."
        )
        latest = after.get("latest_version")
        if latest is not None and latest != before.get("version"):
            text += f" The table was at version {latest} by the hash; the run did not read it."
        return text
    if moved is False and before.get("kind") == FILES and not before.get("etags"):
        return (
            f"File names, sizes and modification times are unchanged since the read "
            f"({_describe(before)}). This file system gives no content tags, so a file "
            "rewritten with the same size and time cannot be ruled out."
        )
    if moved is False:
        text = (
            f"The source did not change between the read and the hash ({_describe(before)}). "
            "This digest is of what the task read."
        )
        ops = after.get("commits_since_read")
        if ops:
            text += " Commits since the read change no rows: " + ", ".join(ops) + "."
        return text
    reason = before.get("reason") or after.get("reason") or "no version"
    return (
        f"This source has no version to check ({reason}), so whether it changed between "
        "the read and the hash is not known."
    )
