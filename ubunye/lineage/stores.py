"""Run record stores are plugins, found by name, chosen in one order (ADR 011).

Every store, the shipped ones included, registers in the
``ubunye.lineage_backends`` entry point group::

    [project.entry-points."ubunye.lineage_backends"]
    filesystem = "ubunye.lineage.storage:FileSystemLineageStore"
    delta = "ubunye.lineage.delta_store:DeltaLineageBackend"

A store provides :class:`~ubunye.interfaces.lineage.RunRecordStore` (``save``,
``load``, ``list_runs``, ``search``) and is built with one argument: its
location, passed unchanged.

Which store, in order:

1. the one asked for by name (``--lineage-backend``, ``lineage_backend=``);
2. the ``UBUNYE_LINEAGE_BACKEND`` environment variable;
3. :data:`DEFAULT_BACKEND` (``filesystem``).
"""

from __future__ import annotations

import importlib.metadata as md
import os
from pathlib import Path
from typing import Any, List, Optional, Union

from ubunye.core.errors import PluginNotFoundError

GROUP = "ubunye.lineage_backends"
ENV_VAR = "UBUNYE_LINEAGE_BACKEND"
DEFAULT_BACKEND = "filesystem"
#: Where the filesystem store keeps records, under the use case directory.
DEFAULT_DIR = ".ubunye/lineage"

#: What a store must provide (RunRecordStore).
REQUIRED = ("save", "load", "list_runs", "search")

# Extras that provide a shipped store's dependencies, for install hints.
_EXTRAS = {"delta": "delta"}


def _entry_points() -> List[Any]:
    return list(md.entry_points(group=GROUP))


def available() -> List[str]:
    """Registered store names, sorted. Loads nothing."""
    return sorted({ep.name.lower() for ep in _entry_points()})


def backend_name(name: Optional[str] = None) -> str:
    """The store to use: ``name`` if given, else ``$UBUNYE_LINEAGE_BACKEND``, else filesystem."""
    for choice in (name, os.environ.get(ENV_VAR)):
        if choice and choice.strip():
            return choice.strip().lower()
    return DEFAULT_BACKEND


def _hint(name: str) -> str:
    extra = _EXTRAS.get(name)
    if extra:
        return f"pip install 'ubunye-engine[{extra}]'"
    return "Install the package that provides this lineage backend."


def load_class(name: str) -> Any:
    """The store class registered as ``name`` (case blind), or a clear error."""
    wanted = name.strip().lower()
    for ep in _entry_points():
        if ep.name.lower() != wanted:
            continue
        try:
            cls = ep.load()
        except Exception as exc:  # a missing dependency, usually
            raise PluginNotFoundError(
                f"The '{wanted}' lineage backend is registered but could not be loaded: {exc}",
                context={"Lineage backend": wanted, "Entry point": ep.value},
                hint=_hint(wanted),
            ) from exc
        missing = [m for m in REQUIRED if not callable(getattr(cls, m, None))]
        if missing:
            raise PluginNotFoundError(
                f"The '{wanted}' lineage backend cannot store run records: "
                f"it has no {', '.join(missing)}.",
                context={"Lineage backend": wanted, "Entry point": ep.value},
                hint="A lineage backend provides save, load, list_runs and search "
                "(ubunye.interfaces.lineage.RunRecordStore).",
            )
        return cls
    installed = available()
    raise PluginNotFoundError(
        f"No lineage backend named '{wanted}'.",
        context={"Lineage backend": wanted, "Installed": installed, "Set by": _source()},
        hint=f"Use one of: {', '.join(installed)}. A backend from another package is "
        f"found once that package is installed (pip install <package>).",
    )


def _source() -> str:
    return f"${ENV_VAR}" if os.environ.get(ENV_VAR) else "--lineage-backend"


def location(
    usecase_dir: Union[str, Path], lineage_dir: str = DEFAULT_DIR, backend: Optional[str] = None
) -> str:
    """Where the chosen store lives.

    The filesystem store keeps today's layout, ``<usecase_dir>/<lineage_dir>``.
    Any other store gets ``lineage_dir`` exactly as given (a table name, a URL).
    """
    if backend_name(backend) == DEFAULT_BACKEND:
        return str(Path(usecase_dir) / lineage_dir)
    return lineage_dir


def open_store(where: str = DEFAULT_DIR, backend: Optional[str] = None) -> Any:
    """Build the chosen store at ``where`` (passed to its constructor unchanged)."""
    return load_class(backend_name(backend))(where)


def open_for(
    usecase_dir: Union[str, Path], lineage_dir: str = DEFAULT_DIR, backend: Optional[str] = None
) -> Any:
    """The store a command uses for a use case directory (see :func:`location`)."""
    return open_store(location(usecase_dir, lineage_dir, backend), backend)
