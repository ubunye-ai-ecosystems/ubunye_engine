"""Model registry backends are plugins, found by name (ADR 010).

Every registry backend, the shipped ones included, registers in the
``ubunye.registry_backends`` entry point group::

    [project.entry-points."ubunye.registry_backends"]
    filesystem = "ubunye.models.registry:FilesystemRegistryBackend"
    mlflow = "ubunye.models.mlflow_registry:MLflowRegistryBackend"

so a third party registry plugs in the same way, with no edit to Ubunye.

Which backend a registry uses, in order:

1. the one asked for by name (``--backend`` on ``ubunye models``,
   ``registry.backend`` in a model transform, ``backend=`` in Python);
2. the ``UBUNYE_REGISTRY_BACKEND`` environment variable;
3. :data:`DEFAULT_REGISTRY_BACKEND` (``filesystem``), which needs no cloud.

The store location (``--store``, ``registry.store``) is passed to the backend's
constructor unchanged. What it means (a folder, a bucket, a workspace) is the
backend's business.
"""

from __future__ import annotations

import importlib.metadata as md
import os
from typing import Any, List, Optional

from ubunye.core.errors import PluginNotFoundError

GROUP = "ubunye.registry_backends"

#: The environment variable that picks a backend when none is named.
ENV_VAR = "UBUNYE_REGISTRY_BACKEND"

#: The backend used when nothing is named: a folder, no cloud, always works.
DEFAULT_REGISTRY_BACKEND = "filesystem"

# Extras that provide a shipped backend's dependencies, for install hints.
_EXTRAS = {"mlflow": "ml"}


class RegistryBackendNotFoundError(PluginNotFoundError):
    """Raised when no registry backend is registered under the name asked for."""


def _entry_points() -> List[Any]:
    return list(md.entry_points(group=GROUP))


def available() -> List[str]:
    """Registered registry backend names, sorted. Loads nothing."""
    return sorted({ep.name.lower() for ep in _entry_points()})


def choose(name: Optional[str] = None) -> str:
    """The backend name to use: the one named, else the env var, else the default."""
    if name and name.strip():
        return name.strip().lower()
    from_env = os.environ.get(ENV_VAR, "").strip()
    if from_env:
        return from_env.lower()
    return DEFAULT_REGISTRY_BACKEND


def load_class(name: str) -> Any:
    """The class registered as ``name`` (case blind), or a clear error."""
    wanted = name.strip().lower()
    for ep in _entry_points():
        if ep.name.lower() != wanted:
            continue
        try:
            return ep.load()
        except Exception as exc:  # a missing dependency, usually
            extra = _EXTRAS.get(wanted)
            raise RegistryBackendNotFoundError(
                f"The '{wanted}' registry backend is registered but could not be loaded: {exc}",
                context={"Registry backend": wanted, "Entry point": ep.value},
                hint=(
                    f"pip install 'ubunye-engine[{extra}]'"
                    if extra
                    else "Install the package that provides this registry backend."
                ),
            ) from exc
    installed = available()
    raise RegistryBackendNotFoundError(
        f"No model registry backend named '{wanted}'.",
        context={"Registry backend": wanted, "Installed": installed or ["(none)"]},
        hint=(
            f"Use one of: {', '.join(installed)}. A third party backend is a package "
            f"that registers a class under the '{GROUP}' entry point group; pip install it. "
            f"Check the {ENV_VAR} environment variable if you did not name one."
        ),
    )


def resolve_registry(name: Optional[str] = None, *, store: str) -> Any:
    """Build the registry backend to use, in the order explicit > env var > filesystem.

    Args:
        name: A backend name, or ``None`` to use ``UBUNYE_REGISTRY_BACKEND`` or the
            default.
        store: Where the models live. Passed to the backend's constructor unchanged.

    Returns:
        An object that follows :class:`ubunye.interfaces.RegistryBackend`.
    """
    return load_class(choose(name))(store)
