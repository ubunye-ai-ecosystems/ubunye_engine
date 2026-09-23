"""Backends are plugins, found by name (ADR 001) and resolved in one order (ADR 003).

Every backend, the shipped ones included, registers in the ``ubunye.backends``
entry point group::

    [project.entry-points."ubunye.backends"]
    spark = "ubunye.backends.spark_backend:SparkBackend"
    pandas = "ubunye.backends.pandas_backend:PandasBackend"

so a third party engine plugs in the same way, with no edit here. This module
never imports an engine: it loads the class a name points at, only when asked.

Which backend runs a task, in order:

1. the one asked for by name (``--backend`` on the CLI, ``backend=`` in the API);
2. the platform's: a backend that finds an active session it should attach to
   (on Databricks, the notebook's SparkSession);
3. :data:`DEFAULT_BACKEND`.

The task's ``config.yaml`` never names its engine; the same folder runs on any
backend that can do what it asks.
"""

from __future__ import annotations

import importlib.metadata as md
from typing import Any, Dict, List, Optional, Type

from ubunye.core.errors import BackendNotFoundError
from ubunye.core.interfaces import Backend

GROUP = "ubunye.backends"

#: The backend used when nothing is named and no platform session is found.
#: One named slot, so a future engine can become the default by changing this.
DEFAULT_BACKEND = "spark"

# Extras that provide a shipped backend's dependencies, for install hints.
_EXTRAS = {"spark": "spark", "databricks": "spark", "pandas": "pandas"}


def _entry_points() -> List[Any]:
    eps: Any = md.entry_points()
    return list(eps.get(GROUP, []) if hasattr(eps, "get") else eps.select(group=GROUP))


def available() -> List[str]:
    """Registered backend names, sorted. Loads nothing."""
    return sorted({ep.name.lower() for ep in _entry_points()})


def entry_point_values() -> Dict[str, str]:
    """``name -> "module:Class"`` for every registered backend. Loads nothing."""
    return {ep.name.lower(): ep.value for ep in _entry_points()}


def load_class(name: str) -> Type[Backend]:
    """The Backend class registered as ``name`` (case blind), or a clear error."""
    wanted = name.strip().lower()
    for ep in _entry_points():
        if ep.name.lower() != wanted:
            continue
        try:
            cls = ep.load()
        except Exception as exc:  # a missing dependency, usually
            extra = _EXTRAS.get(wanted)
            hint = (
                f"pip install 'ubunye-engine[{extra}]'"
                if extra
                else "Install the package that provides this backend."
            )
            raise BackendNotFoundError(
                f"The '{wanted}' backend is registered but could not be loaded: {exc}",
                context={"Backend": wanted, "Entry point": ep.value},
                hint=hint,
            ) from exc
        if not (isinstance(cls, type) and issubclass(cls, Backend)):
            raise BackendNotFoundError(
                f"The '{wanted}' entry point is not a Backend: {ep.value}",
                context={"Backend": wanted, "Entry point": ep.value},
                hint="A backend entry point must name a subclass of ubunye.core.interfaces.Backend.",
            )
        return cls
    raise BackendNotFoundError(
        f"No backend named '{wanted}'.",
        context={"Backend": wanted, "Installed": available()},
        hint=f"Use one of: {', '.join(available())}.",
    )


def create(name: str, *, app_name: str = "ubunye", conf: Optional[Dict[str, Any]] = None) -> Any:
    """Build the backend registered as ``name``."""
    return load_class(name).create(app_name=app_name, conf=dict(conf or {}))


def _platform_backend(*, app_name: str, conf: Dict[str, Any]) -> Optional[Any]:
    """The first registered backend that finds a platform session to attach to."""
    for ep in sorted(_entry_points(), key=lambda e: e.name):
        try:
            cls = ep.load()
        except Exception:
            continue  # a backend that cannot load cannot claim the platform either
        if not (isinstance(cls, type) and issubclass(cls, Backend)):
            continue
        if cls.from_platform is Backend.from_platform:
            continue  # this backend never attaches to a platform session
        backend = cls.from_platform(app_name=app_name, conf=conf)
        if backend is not None:
            return backend
    return None


def resolve(
    name: Optional[str] = None,
    *,
    app_name: str = "ubunye",
    conf: Optional[Dict[str, Any]] = None,
) -> Any:
    """The backend a run should use: the named one, else the platform's, else the default."""
    conf = dict(conf or {})
    if name:
        return create(name, app_name=app_name, conf=conf)
    platform = _platform_backend(app_name=app_name, conf=conf)
    if platform is not None:
        return platform
    return create(DEFAULT_BACKEND, app_name=app_name, conf=conf)
