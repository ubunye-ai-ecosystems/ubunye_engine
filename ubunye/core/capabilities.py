"""What a backend can do, and whether a task asks for more (ADR 002).

A backend declares its :class:`Capabilities`; a connector declares what it
``REQUIRES`` of a backend. Before anything runs, :func:`check_task` compares the
two for every input and output and returns every problem at once, so a task
that cannot work on the chosen backend fails in the first second with the whole
list, not halfway through a job with the first error.

The core never assumes what an engine can do. It asks.

Feature names a backend can declare (connectors refer to the same names):

``spark``
    A live SparkSession (``backend.spark``). The hive, jdbc, delta, unity,
    binary and rest_api connectors need it.
``path_io``
    ``read_frame`` / ``execute_write`` on paths, which the ``s3`` connector uses.
``partitioned_writes``
    Honours ``partitionBy`` on a path write.
``remote_paths``
    Reads and writes cloud paths (``s3a://``, ``abfss://``, ``gs://``, ``dbfs:``).
``catalog``
    ``USE CATALOG`` / ``USE SCHEMA`` and managed tables.
"""

from __future__ import annotations

import os
import posixpath
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, FrozenSet, List, Mapping, Optional
from urllib.parse import unquote

if TYPE_CHECKING:
    from ubunye.core.runtime import Registry

SPARK = "spark"
PATH_IO = "path_io"
PARTITIONED_WRITES = "partitioned_writes"
REMOTE_PATHS = "remote_paths"
CATALOG = "catalog"

_DEFAULT_FILE_FORMAT = "parquet"


@dataclass(frozen=True)
class Capabilities:
    """A backend's declaration of what it can do.

    ``file_formats`` and ``write_modes`` of ``None`` mean "any": the backend
    passes them through to an engine that knows its own formats (Spark reads
    ``orc``, ``avro``, ``xml``... with the right packages).
    """

    features: FrozenSet[str] = frozenset()
    file_formats: Optional[FrozenSet[str]] = None
    write_modes: Optional[FrozenSet[str]] = None
    distributed: bool = False
    lazy: bool = False
    needs_jvm: bool = False
    #: False for a backend that predates capabilities: nothing is pre-checked,
    #: and it fails (or not) at run time exactly as before.
    declared: bool = True
    notes: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def unknown(cls) -> "Capabilities":
        """For a backend that has not declared anything."""
        return cls(declared=False)

    def describe(self) -> Dict[str, Any]:
        """A plain, JSON ready description (``ubunye backends``)."""
        return {
            "declared": self.declared,
            "features": sorted(self.features),
            "file_formats": sorted(self.file_formats) if self.file_formats is not None else "any",
            "write_modes": sorted(self.write_modes) if self.write_modes is not None else "any",
            "distributed": self.distributed,
            "lazy": self.lazy,
            "needs_jvm": self.needs_jvm,
        }


def is_remote(path: str) -> bool:
    """A cloud or cluster path, as opposed to a local path or ``file://`` URI."""
    return ("://" in path and not path.startswith("file://")) or path.startswith("dbfs:")


IoCheck = Callable[[str, Dict[str, Any]], List[str]]


def _connector_problems(
    caps: Capabilities,
    where: str,
    cfg: Dict[str, Any],
    connector: Optional[type],
    backend_name: str,
    *,
    is_output: bool,
    io_check: Optional[IoCheck] = None,
) -> List[str]:
    fmt = cfg.get("format")
    if connector is None:
        return []  # unknown connector: the engine reports it with the installed list
    problems = []
    needs = frozenset(getattr(connector, "REQUIRES", frozenset()))
    missing = sorted(needs - caps.features)
    if missing:
        problems.append(
            f"{where} uses the '{fmt}' connector, which needs {', '.join(missing)}; "
            f"the {backend_name} backend does not provide it."
        )
        return problems  # the details below would only repeat the same verdict

    if PATH_IO in needs:
        file_format = str(cfg.get("file_format") or _DEFAULT_FILE_FORMAT).lower()
        if caps.file_formats is not None and file_format not in caps.file_formats:
            problems.append(
                f"{where} uses file_format '{file_format}'; the {backend_name} backend "
                f"handles {', '.join(sorted(caps.file_formats))}."
            )
        path = cfg.get("path")
        # A secret:// path is only known at run time; it cannot be judged here.
        if (
            isinstance(path, str)
            and not path.startswith("secret://")
            and is_remote(path)
            and REMOTE_PATHS not in caps.features
        ):
            problems.append(
                f"{where} uses the remote path {path}; the {backend_name} backend "
                "reads and writes local paths only."
            )
        if is_output:
            mode = cfg.get("mode")
            if mode and caps.write_modes is not None and str(mode).lower() not in caps.write_modes:
                problems.append(
                    f"{where} uses mode '{mode}'; the {backend_name} backend can do "
                    f"{', '.join(sorted(caps.write_modes))}."
                )
            # Writers read ``partitionBy``; ``partition_by`` is the adapter's argument
            # name, accepted too. Checking only the latter meant this never fired (F-032).
            if (
                cfg.get("partitionBy") or cfg.get("partition_by")
            ) and PARTITIONED_WRITES not in caps.features:
                problems.append(
                    f"{where} uses partitionBy; the {backend_name} backend does not "
                    "write partitioned folders."
                )
        if io_check is not None:
            direction = "output" if is_output else "input"
            problems += [f"{where}: {p}" for p in io_check(direction, cfg)]
    return problems


def check_task(
    caps: Capabilities,
    cfg: Dict[str, Any],
    registry: "Registry",
    *,
    backend_name: str,
    io_check: Optional[IoCheck] = None,
) -> List[str]:
    """Every reason this task cannot run on a backend with ``caps``; empty if it can.

    ``io_check`` is the backend's ``check_io``, asked about each path input and
    output's details (options, schema) after the capabilities pass.
    """
    if not caps.declared:
        return []
    config = cfg.get("CONFIG", {}) or {}
    problems: List[str] = []
    for name, icfg in sorted((config.get("inputs") or {}).items()):
        reader: Optional[type] = registry.readers.get(icfg.get("format"))
        problems += _connector_problems(
            caps, f"input '{name}'", icfg, reader, backend_name, is_output=False, io_check=io_check
        )
    for name, ocfg in sorted((config.get("outputs") or {}).items()):
        writer: Optional[type] = registry.writers.get(ocfg.get("format"))
        problems += _connector_problems(
            caps, f"output '{name}'", ocfg, writer, backend_name, is_output=True, io_check=io_check
        )
    if caps.lazy:
        problems += self_overwrites(config, lambda o: _writes_path(registry, o))
    return problems


#: Write modes that delete what is at the output's path before writing.
_DELETING_MODES = frozenset({"overwrite", "overwrite_partitions"})
_GLOB = re.compile(r"[*?\[{]")
#: Schemes that reach the same store: Hadoop's s3 and s3n are s3a on Spark today.
_SCHEME_ALIASES = {"s3": "s3a", "s3n": "s3a"}
#: In every self-overwrite problem, so a caller can give it its own hint.
SELF_OVERWRITE_MARK = "the source is lost"


@dataclass(frozen=True)
class _Location:
    """A path as compared here. ``prefix``: a glob's literal start; anything under it may be read."""

    text: str
    prefix: bool = False


def self_overwrites(
    config: Dict[str, Any], writes_path: Optional[Callable[[Dict[str, Any]], bool]] = None
) -> List[str]:
    """Outputs that would delete, before the write, files an input still has to read (F-047).

    On a lazy backend the transform runs during the write. An ``overwrite`` of a
    folder the task reads (the same folder, a parent, one inside it, or one a glob
    can reach) deletes the input's files first, then the write reads files that are
    gone: the run fails and the source is lost. Delta outputs are left alone, since
    a Delta overwrite reads a fixed snapshot of the table. ``writes_path`` says
    whether an output's writer writes to its ``path`` at all (a Unity writer
    ignores it). A backend that reads into memory first (pandas) never gets here:
    only a lazy backend is checked.

    Not seen: a relative path that a cluster resolves somewhere other than this
    process's folder, two secrets that resolve to one path, and a table input
    whose storage is the output's folder.
    """
    reads = []
    for name, icfg in sorted((config.get("inputs") or {}).items()):
        loc = _location(icfg.get("path"))
        if loc:
            reads.append((name, icfg.get("path"), loc))
    problems: List[str] = []
    for name, ocfg in sorted((config.get("outputs") or {}).items()):
        mode = _mode(ocfg.get("mode"))
        if mode not in _DELETING_MODES or _is_delta(ocfg):
            continue
        if writes_path is not None and not writes_path(ocfg):
            continue
        out = _location(ocfg.get("path"))
        if not out or out.prefix:
            continue  # an output path with a glob in it is not a folder we can judge
        for iname, ipath, loc in reads:
            if _overlaps(out.text, loc):
                problems.append(
                    f"output '{name}' ({mode}) writes to '{ocfg.get('path')}', which "
                    f"input '{iname}' reads ('{ipath}'). The overwrite deletes the input's "
                    f"files before the write has read them, so the run fails and "
                    f"{SELF_OVERWRITE_MARK}. Write to a new path, or use Delta "
                    "(file_format: delta), whose overwrite reads a fixed snapshot."
                )
    return problems


def _writes_path(registry: "Registry", ocfg: Dict[str, Any]) -> bool:
    """False for a writer that declares it never writes to ``path`` (a Unity table)."""
    writer = registry.writers.get(str(ocfg.get("format") or ""))
    requires = getattr(writer, "REQUIRES", None)
    return requires is None or PATH_IO in requires


def _mode(raw: Any) -> str:
    """The mode as the writer resolves it: an enum's value, trimmed, lower case."""
    return str(getattr(raw, "value", raw) or "").strip().lower()


def _is_delta(cfg: Dict[str, Any]) -> bool:
    return "delta" in (
        str(cfg.get("format") or "").lower(),
        str(cfg.get("file_format") or "").lower(),
    )


def _location(path: Any) -> Optional[_Location]:
    """One spelling for every way of writing the same place, or ``None`` if unknowable.

    Local paths are made absolute, symlinks followed, case folded where the file
    system folds it. ``file:`` is local. ``dbfs:/x``, ``dbfs:///x`` and ``/dbfs/x``
    are one place. For other schemes s3 and s3n are s3a, the host is lower case,
    and ``//``, ``..`` and %-escapes are resolved. A glob keeps only its literal
    start, marked as a prefix.
    """
    if not isinstance(path, str) or not path.strip() or "{{" in path or "{%" in path:
        return None  # not rendered yet: nothing is known about it
    p = path.strip().replace("\\", "/")
    glob = _GLOB.search(p)
    literal = p[: glob.start()] if glob else p
    m = re.match(r"^([A-Za-z][A-Za-z0-9+.-]+):(.*)$", literal)  # one letter is a drive
    if m and m.group(1).lower() != "file":
        scheme, rest = m.group(1).lower(), m.group(2)
        if scheme == "dbfs":
            return _finish("dbfs:" + _clean("/" + rest.lstrip("/")), literal, glob)
        if not rest.startswith("//"):
            return None
        host, slash, tail = rest[2:].partition("/")
        if glob and not slash:
            return None  # a glob in the bucket or host names no one place
        scheme = _SCHEME_ALIASES.get(scheme, scheme)
        return _finish(f"{scheme}://{host.lower()}" + _clean("/" + unquote(tail)), literal, glob)
    if m:  # file:, file:/x, file:///x, file:///C:/x
        rest = m.group(2).lstrip("/")
        literal = rest if re.match(r"^[A-Za-z]:", rest) else "/" + rest
    if literal.startswith("/dbfs/") or literal == "/dbfs":
        return _finish("dbfs:" + _clean(literal[5:] or "/"), literal, glob)
    base = literal if literal.strip() else "."
    if re.fullmatch(r"[A-Za-z]:", base):
        base += "/"  # "C:" alone is the current folder on drive C, not its root
    local = os.path.normcase(os.path.realpath(os.path.abspath(base))).replace("\\", "/")
    return _finish(local, literal, glob)


def _clean(path: str) -> str:
    # normpath keeps a leading "//" (POSIX allows it); an object store does not.
    cleaned = posixpath.normpath(re.sub("/+", "/", path))
    return "/" if cleaned == "." else cleaned


def _finish(text: str, literal: str, glob: Any) -> _Location:
    text = text.rstrip("/") or "/"
    if not glob:
        return _Location(text)
    # The literal before the glob: "data/" is the folder, "data/in" also reaches "data/in2".
    return _Location(text if not literal.endswith("/") else text.rstrip("/") + "/", prefix=True)


def _overlaps(out: str, read: _Location) -> bool:
    if read.prefix:
        return out.startswith(read.text) or read.text.startswith(out.rstrip("/") + "/")
    b = read.text
    return out == b or out.startswith(b.rstrip("/") + "/") or b.startswith(out.rstrip("/") + "/")
