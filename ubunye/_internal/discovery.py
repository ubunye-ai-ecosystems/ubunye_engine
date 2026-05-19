"""Entry-point discovery for pluggable backends.

Loads adapters lazily via ``importlib.metadata`` entry-point groups and
caches the result for the lifetime of the process.

Entry-point groups:

* ``ubunye.deploy_adapters`` — :class:`~ubunye.interfaces.DeployAdapter`
* ``ubunye.registry_backends`` — :class:`~ubunye.interfaces.RegistryBackend`
* ``ubunye.lineage_backends`` — :class:`~ubunye.interfaces.LineageBackend`
* ``ubunye.auth_backends`` — :class:`~ubunye.interfaces.AuthBackend`
"""

from __future__ import annotations

import importlib.metadata as md
import logging
from typing import Any, Dict

from ubunye.core.errors import PluginNotFoundError

logger = logging.getLogger(__name__)

_cache: Dict[str, Dict[str, Any]] = {}


def _load_group(group: str) -> Dict[str, Any]:
    """Load all entry points in a group, caching the result."""
    if group in _cache:
        return _cache[group]

    eps = md.entry_points()
    if hasattr(eps, "select"):
        group_eps = eps.select(group=group)
    elif isinstance(eps, dict):
        group_eps = eps.get(group, [])
    else:
        group_eps = [ep for ep in eps if ep.group == group]

    loaded: Dict[str, Any] = {}
    for ep in group_eps:
        try:
            loaded[ep.name] = ep.load()
        except Exception:
            logger.warning("Failed to load entry point %s:%s", group, ep.name)
    _cache[group] = loaded
    return loaded


def _get(group: str, name: str) -> Any:
    """Load a specific entry point by group and name."""
    backends = _load_group(group)
    if name not in backends:
        available = sorted(backends.keys()) or ["(none registered)"]
        raise PluginNotFoundError(
            f"Backend '{name}' not found in entry-point group '{group}'.",
            context={"Name": name, "Group": group, "Available": available},
            hint=f"Install a package that registers '{name}' under '{group}', "
            f"or use one of: {', '.join(available)}",
        )
    return backends[name]


def get_deploy_adapter(name: str) -> Any:
    """Load a deploy adapter class by name."""
    return _get("ubunye.deploy_adapters", name)


def get_registry_backend(name: str) -> Any:
    """Load a registry backend class by name."""
    return _get("ubunye.registry_backends", name)


def get_lineage_backend(name: str) -> Any:
    """Load a lineage backend class by name."""
    return _get("ubunye.lineage_backends", name)


def get_auth_backend(name: str) -> Any:
    """Load an auth backend class by name."""
    return _get("ubunye.auth_backends", name)


def list_backends(group: str) -> Dict[str, Any]:
    """List all registered backends in a group."""
    return dict(_load_group(group))


def clear_cache() -> None:
    """Clear the discovery cache (useful for testing)."""
    _cache.clear()


def get_or_instantiate(
    group: str,
    name: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Load a backend class and instantiate it with the given arguments."""
    cls = _get(group, name)
    return cls(*args, **kwargs)
