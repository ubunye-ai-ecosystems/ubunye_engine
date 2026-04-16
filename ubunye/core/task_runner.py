"""Shared helper for running user-defined ``Task`` classes through the Engine.

The Engine's transform chain is config-driven (``noop``, ``model``, etc.).
User pipelines typically put their logic in a ``Task`` subclass inside
``transformations.py`` — that lives outside the plugin registry.

This module adapts a user ``Task`` as an ephemeral ``Transform`` plugin so
the same Engine can execute it. The Python API and CLI both go through
``execute_user_task`` — one code path, one set of hooks, one lifecycle.
"""

from __future__ import annotations

import importlib.util
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

from ubunye.core.hooks import Hook
from ubunye.core.interfaces import Backend, Task
from ubunye.core.runtime import Engine, EngineContext, Registry


def _load_task_class(task_dir: Path) -> type:
    """Import ``transformations.py`` and return the first Task subclass."""
    fc_path = task_dir / "transformations.py"
    if not fc_path.exists():
        raise FileNotFoundError(
            f"Missing transformations.py at {fc_path}. "
            "Each task directory must contain a transformations.py with a Task subclass."
        )

    spec = importlib.util.spec_from_file_location("transformations", str(fc_path))
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(mod)

    for attr in mod.__dict__.values():
        if isinstance(attr, type) and issubclass(attr, Task) and attr is not Task:
            return attr

    raise RuntimeError(
        f"No Task subclass found in {fc_path}. "
        "Define a class that subclasses ubunye.core.interfaces.Task."
    )


def _make_user_task_transform(task_obj: Task) -> type:
    """Return a Transform class whose ``apply`` delegates to ``task_obj.transform``."""

    class _UserTaskTransform:
        def apply(self, sources: Dict[str, Any], cfg: Dict[str, Any], backend: Backend):
            return task_obj.transform(sources)

    return _UserTaskTransform


@contextmanager
def _with_task_dir_on_path(task_dir: Path) -> Iterator[None]:
    """Put ``task_dir`` on ``sys.path`` for the duration of the run.

    Lets ``transformations.py`` import adjacent modules (``from model import ...``).

    On exit, evicts any ``sys.modules`` entries whose source file lives under
    ``task_dir``. Without this, two sequential tasks that each ship their own
    ``model.py`` (or ``utils.py``, …) would see the *first* task's module
    cached under the shared name when the second task imports it — silently
    running against stale code. Only task-local modules are evicted; stdlib
    and site-packages stay put.
    """
    path_str = str(task_dir)
    task_dir_resolved = str(task_dir.resolve())
    already = path_str in sys.path
    if not already:
        sys.path.insert(0, path_str)
    try:
        yield
    finally:
        if not already and path_str in sys.path:
            sys.path.remove(path_str)
        to_evict = []
        for name, mod in list(sys.modules.items()):
            mod_file = getattr(mod, "__file__", None)
            if not mod_file:
                continue
            try:
                mod_resolved = str(Path(mod_file).resolve())
            except (OSError, ValueError):
                continue
            if mod_resolved.startswith(task_dir_resolved):
                to_evict.append(name)
        for name in to_evict:
            sys.modules.pop(name, None)


_USER_TASK_TRANSFORM_KEY = "_ubunye_user_task"


def execute_user_task(
    backend: Backend,
    task_dir: Path,
    cfg: Any,
    context: EngineContext,
    *,
    manage_backend: bool = False,
    hooks: Optional[Iterable[Hook]] = None,
    extra_hooks: Optional[Iterable[Hook]] = None,
) -> Dict[str, Any]:
    """Run a user-defined ``Task`` via the Engine.

    Loads ``transformations.py``, wraps the Task as a Transform plugin,
    overrides ``CONFIG.transform`` to point at the wrapper, and delegates
    to ``Engine.run``.

    Parameters
    ----------
    backend
        Started backend. If ``manage_backend`` is False (default), the caller
        owns ``start()``/``stop()``.
    task_dir
        Directory containing ``config.yaml`` and ``transformations.py``.
    cfg
        Validated ``UbunyeConfig`` (a Pydantic model).
    context
        Engine context (run_id, task_name, profile).
    manage_backend
        Forwarded to ``Engine``. Default False — most callers share one backend
        across multiple tasks.
    hooks
        Replace the engine's default hooks entirely.
    extra_hooks
        Append these to the default hook set (e.g. a ``MonitorHook`` wrapping
        a lineage recorder).

    Returns
    -------
    Dict[str, Any]
        Mapping of output name → DataFrame.
    """
    cfg_dict = cfg.model_dump(mode="json")

    # The task_dir must be on sys.path *before* loading transformations.py,
    # not only during engine.run. Otherwise a top-level ``from model import
    # MyModel`` (adjacent helper modules) raises ModuleNotFoundError at
    # import time, before the engine even gets the chance to extend the path.
    with _with_task_dir_on_path(task_dir):
        task_cls = _load_task_class(task_dir)
        task_obj = task_cls(config=cfg_dict)
        task_obj.setup()

        reg = Registry.from_entrypoints()
        reg.register_transform(_USER_TASK_TRANSFORM_KEY, _make_user_task_transform(task_obj))
        cfg_dict = {
            **cfg_dict,
            "CONFIG": {**cfg_dict["CONFIG"], "transform": {"type": _USER_TASK_TRANSFORM_KEY}},
        }

        engine = Engine(
            backend=backend,
            registry=reg,
            context=context,
            hooks=hooks,
            extra_hooks=extra_hooks,
            manage_backend=manage_backend,
        )

        result = engine.run(cfg_dict)

    return result or {}
