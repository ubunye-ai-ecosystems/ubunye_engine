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
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, FrozenSet, List, Mapping, Optional

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
        problems += self_overwrites(config)
    return problems


#: Write modes that delete what is at the output's path before writing.
_DELETING_MODES = frozenset({"overwrite", "overwrite_partitions"})
_GLOB = re.compile(r"[*?\[{]")


def self_overwrites(config: Dict[str, Any]) -> List[str]:
    """Outputs that would delete, before the write, files an input still has to read (F-047).

    On a lazy backend the transform runs during the write. An ``overwrite`` of a
    folder the task reads (the same folder, or one inside the other) deletes the
    input's files first, then the write reads files that are gone: the run fails
    and the source is lost. Delta outputs are left alone, since a Delta overwrite
    reads a fixed snapshot of the table. A backend that reads into memory first
    (pandas) never gets here: only a lazy backend is checked.
    """
    reads = []
    for name, icfg in sorted((config.get("inputs") or {}).items()):
        root = _local_root(icfg.get("path"))
        if root:
            reads.append((name, icfg.get("path"), root))
    problems: List[str] = []
    for name, ocfg in sorted((config.get("outputs") or {}).items()):
        mode = str(ocfg.get("mode") or "").lower()
        if mode not in _DELETING_MODES or _is_delta(ocfg):
            continue
        out = _local_root(ocfg.get("path"))
        if not out:
            continue
        for iname, ipath, root in reads:
            if _overlaps(out, root):
                problems.append(
                    f"output '{name}' ({mode}) writes to '{ocfg.get('path')}', which "
                    f"input '{iname}' reads ('{ipath}'). The overwrite deletes the input's "
                    "files before the write has read them, so the run fails and the source "
                    "is lost. Write to a new path, or use Delta (file_format: delta), whose "
                    "overwrite reads a fixed snapshot."
                )
    return problems


def _is_delta(cfg: Dict[str, Any]) -> bool:
    return "delta" in (
        str(cfg.get("format") or "").lower(),
        str(cfg.get("file_format") or "").lower(),
    )


def _local_root(path: Any) -> Optional[str]:
    """The folder a path names, compared as text: no scheme case, no trailing slash, no glob."""
    if not isinstance(path, str) or not path.strip() or "{{" in path or "{%" in path:
        return None  # not rendered yet: nothing is known about it
    p = path.strip().replace("\\", "/")
    glob = _GLOB.search(p)
    if glob:
        p = p[: glob.start()].rsplit("/", 1)[0]
        if not p.strip("/"):
            return None  # a glob from the root names no one folder
    scheme, sep, rest = p.partition("://")
    if not sep:  # a local path, or "file:/x"
        scheme, rest = "", p[5:] if p.lower().startswith("file:") else p
    elif scheme.lower() == "file":
        scheme = ""
    if not scheme:
        rest = os.path.normcase(os.path.abspath(rest)).replace("\\", "/")
        return rest.rstrip("/") or "/"
    return f"{scheme.lower()}://{rest.rstrip('/')}"


def _overlaps(a: str, b: str) -> bool:
    return a == b or a.startswith(b + "/") or b.startswith(a + "/")
