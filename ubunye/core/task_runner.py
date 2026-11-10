"""Shared helper for running user-defined ``Task`` classes through the Engine.

The Engine's transform chain is config-driven (``noop``, ``model``, etc.).
User pipelines typically put their logic in a ``Task`` subclass inside
``transformations.py`` — that lives outside the plugin registry.

This module adapts a user ``Task`` as an ephemeral ``Transform`` plugin so
the same Engine can execute it. The Python API and CLI both go through
``execute_user_task`` — one code path, one set of hooks, one lifecycle.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import logging
import sys
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

from ubunye.config.hashing import config_hash
from ubunye.core import runs
from ubunye.core.errors import TaskClassMissingError, TaskNotFoundError
from ubunye.core.hooks import Hook
from ubunye.core.interfaces import Backend, Task
from ubunye.core.runtime import Engine, EngineContext, Registry

logger = logging.getLogger(__name__)


def _load_task_class(task_dir: Path) -> type:
    """Import ``transformations.py`` and return the first Task subclass."""
    fc_path = task_dir / "transformations.py"
    if not fc_path.exists():
        raise TaskNotFoundError(
            f"Missing transformations.py at {fc_path}.",
            context={"Task dir": str(task_dir), "Expected file": str(fc_path)},
            hint="Run 'ubunye init' to scaffold a new task, or check the directory path.",
        )

    spec = importlib.util.spec_from_file_location("transformations", str(fc_path))
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(mod)

    for attr in mod.__dict__.values():
        if isinstance(attr, type) and issubclass(attr, Task) and attr is not Task:
            return attr

    raise TaskClassMissingError(
        f"No Task subclass found in {fc_path}.",
        context={"File": str(fc_path)},
        hint="Define a class that subclasses ubunye.core.interfaces.Task.",
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


def ignored_transform_message(transform: Any) -> Optional[str]:
    """Why a task run will not run the config's ``transform.type``, or None.

    A task folder always runs its ``transformations.py``; a ``type`` other than
    ``noop`` in ``config.yaml`` never ran, and until 0.7.1 it was dropped with
    no word. ``ubunye run`` and ``ubunye plan`` both say so, in these words.
    """
    ttype = transform.get("type") if isinstance(transform, dict) else None
    if ttype in (None, "noop"):
        return None
    if ttype == "model":
        instead = (
            "To train or score a model, call ModelTransform from transformations.py "
            "(a model registered inside a run is linked to it)"
        )
    else:
        instead = "Call that logic from transformations.py"
    return (
        f"CONFIG.transform.type '{ttype}' is not run: the task still runs its "
        f"transformations.py and ignores this type. {instead}, then remove the "
        "type from config.yaml. See docs/config/transform.md."
    )


def upstream_store(task_dir: Path, context: EngineContext) -> Any:
    """The run record store an upstream check reads (ADR 011, ADR 013).

    The store this run records into when it was given one; else the filesystem
    store under the use case folder (``--lineage-dir``, or ``.ubunye/lineage``).
    """
    from ubunye.lineage import stores
    from ubunye.lineage.storage import FileSystemLineageStore
    from ubunye.lineage.upstream import UpstreamNotReady

    shared = getattr(context, "run_records", None)
    if shared is not None:
        return shared
    if context.lineage_dir:
        return FileSystemLineageStore(context.lineage_dir)
    resolved = Path(task_dir).resolve()
    root = resolved.parents[2] if len(resolved.parents) > 2 else resolved.parent
    name = stores.backend_name()
    if name != stores.DEFAULT_BACKEND:
        raise UpstreamNotReady(
            f"CONFIG.requires reads run records from the '{name}' store "
            f"(${stores.ENV_VAR}), but this run was not told where that store is.",
            context={"Task": context.task_name or str(task_dir)},
            hint="Run with --lineage --lineage-backend <name> --lineage-dir <location> "
            "(run_task(lineage=True, lineage_backend=..., lineage_dir=...)), the same "
            "store the upstream task writes to.",
        )
    return stores.open_store(str(root / stores.DEFAULT_DIR), name)


def _check_upstream(task_dir: Path, context: EngineContext, requires: Any) -> None:
    from ubunye.lineage import upstream

    store = upstream_store(task_dir, context)
    found = upstream.check(store, requires, dict(context.variables or {}), task=context.task_name)
    for r in found:
        logger.info("Upstream %s succeeded for this batch (run %s).", r.task_path, r.run_id)


def finished_run(context: EngineContext) -> Optional[Any]:
    """The run that finished this task's batch, for ``--resume`` (F-098), or None.

    That is the newest run record of the task with this run's batch (``dt`` and
    ``--var``), when it says ``success``. Only run records know that a task that
    overwrites finished, so without a store (no ``--lineage``), or for a run that
    names no batch, this is None and the task runs.
    """
    variables = dict(context.variables or {})
    recorded = context.run_records is not None or context.lineage_dir
    if not recorded or not context.task_name or not runs.names_a_batch(variables):
        return None
    from ubunye.lineage import upstream

    store = upstream_store(Path(context.task_dir or "."), context)

    req = upstream.Requirement(context.task_name)
    latest = upstream._runs_of_batch(store, req.task, upstream.batch_of(req, variables), None)
    return latest[0] if latest and latest[0].status == "success" else None


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
    if context.task_dir is None:
        context = dataclasses.replace(context, task_dir=str(task_dir))
    if context.config_hash is None:
        # Hash the config as loaded, before the transform is swapped for the task
        # wrapper below, so the run record and `ubunye plan` agree.
        context = dataclasses.replace(context, config_hash=config_hash(cfg_dict))

    # CONFIG.requires: refuse to start (or wait) until each upstream task has
    # succeeded for this batch, read from the run record store (ADR 013). A task
    # with no requires does nothing here.
    requires = cfg_dict["CONFIG"].get("requires")
    if requires:
        _check_upstream(task_dir, context, requires)

    # The task_dir must be on sys.path *before* loading transformations.py,
    # not only during engine.run. Otherwise a top-level ``from model import
    # MyModel`` (adjacent helper modules) raises ModuleNotFoundError at
    # import time, before the engine even gets the chance to extend the path.
    ignored = ignored_transform_message(cfg_dict["CONFIG"].get("transform"))
    if ignored:
        logger.warning(ignored)
        warnings.warn(ignored, FutureWarning, stacklevel=2)

    with _with_task_dir_on_path(task_dir):
        task_cls = _load_task_class(task_dir)
        task_obj = task_cls(config=cfg_dict)
        task_obj.variables = dict(context.variables or {})  # dt and --var for the Task
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

        # One live run per task and batch; a dead run's claimed appends are taken back
        # first, and a run that fails takes back its own (ubunye.core.runs).
        resolved = Path(task_dir).resolve()
        task_path = context.task_name or "/".join(resolved.parts[-3:])
        root = resolved.parents[2] if len(resolved.parents) > 2 else resolved.parent
        lineage_dir = Path(context.lineage_dir) if context.lineage_dir else None
        with runs.held(
            root,
            task_path,
            dict(context.variables or {}),
            context.run_id,
            lineage_dir,
            appends=runs.append_outputs(cfg_dict["CONFIG"].get("outputs")),
            rerun=context.rerun,
            records=getattr(context, "run_records", None),
        ):
            result = engine.run(cfg_dict)

    return result or {}
