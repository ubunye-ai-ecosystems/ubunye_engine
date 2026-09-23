"""The dry run: everything that can be checked before a single row moves.

``ubunye plan`` used to print the names of the inputs, the transform type and the
names of the outputs: the config read back, not a plan. It caught nothing.

A plan answers the question a person (or an agent) actually has: **if I run this
now, what happens, and what will stop it?** For every task, in order, it:

* looks for each local input on disk, and knows when an earlier task in the same
  plan writes it (so a pipeline of tasks plans clean before its first run);
* asks the chosen backend whether it can do what each input and output needs
  (its declared capabilities, ADR 002), rather than keeping a list of names;
* resolves the transform class from ``transformations.py``;
* asks each writer to resolve its write mode, which is where ``merge`` without
  ``merge_keys`` and ``overwrite_partitions`` without ``partitionBy`` are caught
  (they used to fail on a cluster, after the transform had run);
* lists environment variables the config references but that are not set.

Nothing here starts a backend, opens a session, reads a table or writes a file.
It returns plain dicts, so a person and a program read the same answer.

Built on the September 2026 work; the multi task check and capabilities are new.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from ubunye.config.hashing import config_hash
from ubunye.config.schema import UbunyeConfig
from ubunye.core import write_modes
from ubunye.core.capabilities import check_task
from ubunye.core.errors import UbunyeError
from ubunye.core.runtime import Registry


def _is_local(path: str) -> bool:
    return (
        bool(path)
        and (path.startswith("file://") or "://" not in path)
        and not path.startswith("dbfs:")
    )


def _local(path: str) -> str:
    """``file:///tmp/x`` and ``file:///C:/x`` back to what the OS understands."""
    if path.startswith("file://"):
        path = path[len("file://") :]
        if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
            path = path[1:]
    return path


def _key(path: str) -> str:
    """One spelling per place on disk, so two tasks naming it are matched."""
    return os.path.normcase(os.path.abspath(_local(path)))


def _on_disk(path: str) -> Dict[str, Any]:
    target = Path(_local(path))
    if not target.exists():
        return {"exists": False, "files": 0, "bytes": 0}
    if target.is_file():
        return {"exists": True, "files": 1, "bytes": target.stat().st_size}
    files = [p for p in target.rglob("*") if p.is_file() and not p.name.startswith(("_", "."))]
    return {"exists": True, "files": len(files), "bytes": sum(p.stat().st_size for p in files)}


def _location(io_cfg: Dict[str, Any]) -> str:
    from ubunye.lineage.context import _location_from_io_cfg

    return _location_from_io_cfg(io_cfg)


def _io_dict(io_cfg: Any) -> Dict[str, Any]:
    cfg: Dict[str, Any] = io_cfg.model_dump(exclude_none=True, mode="json")
    cfg.update(io_cfg.model_extra or {})
    return cfg


def _writer_modes(writer_cls: Any) -> Tuple[Any, str]:
    """What a writer supports, and its default, asked the way the writer declares it."""
    supported: Any = None
    default = "append"
    module_name = getattr(writer_cls, "__module__", None)
    if module_name:
        try:
            module = importlib.import_module(module_name)
            supported = getattr(module, "SUPPORTED_MODES", None)
            default = getattr(module, "DEFAULT_MODE", default)
        except Exception:  # a plugin that will not import is reported elsewhere
            pass
    if supported is None:
        supported = getattr(writer_cls, "SUPPORTED_MODES", write_modes.NATIVE_SAVE_MODES)
    return supported, default


def _engine_version() -> str:
    try:
        from importlib.metadata import version

        return version("ubunye-engine")
    except Exception:
        return "unknown"


def build_plan(
    cfg: UbunyeConfig,
    *,
    task_name: str,
    task_dir: Optional[Path] = None,
    backend: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
    raw_yaml: str = "",
    produced: Optional[Dict[str, str]] = None,
    registry: Optional[Registry] = None,
) -> Dict[str, Any]:
    """The plan for one task. ``produced`` maps output locations of earlier tasks to them.

    ``problems`` will stop the run; ``warnings`` will not. An empty ``problems``
    list is the closest thing to "this will work" that can be said without
    running it.
    """
    problems: List[str] = []
    warnings: List[str] = []
    produced = produced or {}
    registry = registry or Registry.from_entrypoints()
    cfg_dict = cfg.model_dump(mode="json")

    # --- what the backend can do (ADR 002) -----------------------------------
    if backend:
        from ubunye.core import backends

        try:
            cls = backends.load_class(backend)
            problems += check_task(
                cls.CAPABILITIES,
                cfg_dict,
                registry,
                backend_name=backend.lower(),
                io_check=cls.check_io,
            )
        except UbunyeError as exc:
            problems.append(f"backend: {str(exc).splitlines()[0]}")

    # --- environment ---------------------------------------------------------
    env_referenced: List[str] = []
    if raw_yaml:
        from ubunye.config.resolver import extract_env_references

        env_referenced = sorted(extract_env_references(raw_yaml))
    env_missing = [name for name in env_referenced if not os.environ.get(name)]
    if env_missing:
        warnings.append("environment variables referenced but not set: " + ", ".join(env_missing))

    # --- inputs --------------------------------------------------------------
    inputs: List[Dict[str, Any]] = []
    for name in sorted(cfg.CONFIG.inputs):
        io_cfg = _io_dict(cfg.CONFIG.inputs[name])
        fmt = io_cfg.get("format", "")
        entry: Dict[str, Any] = {
            "name": name,
            "format": fmt,
            "file_format": io_cfg.get("file_format"),
            "location": _location(io_cfg),
            "checked": False,
            "produced_by": None,
        }
        if fmt not in registry.readers:
            problems.append(f"inputs.{name}: no reader plugin registered for format '{fmt}'")
        path = io_cfg.get("path") or ""
        if path and _is_local(path):
            entry["checked"] = True
            writer = produced.get(_key(path))
            if writer:
                entry["produced_by"] = writer
            else:
                entry.update(_on_disk(path))
                if not entry["exists"]:
                    problems.append(f"inputs.{name}: nothing at {path}")
                elif entry["files"] == 0:
                    warnings.append(f"inputs.{name}: {path} exists but holds no data files")
        inputs.append(entry)

    # --- transform -----------------------------------------------------------
    transform: Dict[str, Any] = {
        "type": cfg.CONFIG.transform.type,
        "class": None,
        "source": None,
    }
    ttype = cfg.CONFIG.transform.type
    if ttype in (None, "noop") and task_dir is not None:
        module_path = task_dir / "transformations.py"
        if not module_path.exists():
            problems.append("transform: no transformations.py in the task folder")
        else:
            transform["source"] = str(module_path)
            try:
                from ubunye.core.task_runner import _load_task_class, _with_task_dir_on_path

                with _with_task_dir_on_path(task_dir):
                    transform["class"] = _load_task_class(task_dir).__name__
            except UbunyeError as exc:
                problems.append(f"transform: {str(exc).splitlines()[0]}")
            except Exception as exc:  # user code: any import error is possible
                problems.append(f"transform: transformations.py did not import ({exc})")
    elif ttype is not None and ttype not in registry.transforms:
        problems.append(f"transform: no transform plugin registered for type '{ttype}'")

    # --- outputs, with the write mode nobody resolved until write time -------
    outputs: List[Dict[str, Any]] = []
    for name in sorted(cfg.CONFIG.outputs):
        io_cfg = _io_dict(cfg.CONFIG.outputs[name])
        fmt = io_cfg.get("format", "")
        entry = {
            "name": name,
            "format": fmt,
            "file_format": io_cfg.get("file_format"),
            "location": _location(io_cfg),
            "requested_mode": io_cfg.get("mode"),
            "resolved_mode": None,
            "save_mode": None,
            "merge_keys": [],
        }
        writer_cls = registry.writers.get(fmt)
        if writer_cls is None:
            problems.append(f"outputs.{name}: no writer plugin registered for format '{fmt}'")
        else:
            supported, default = _writer_modes(writer_cls)
            try:
                resolved = write_modes.resolve(
                    io_cfg,
                    connector=fmt,
                    supported=supported,
                    default=default,
                    file_format=(io_cfg.get("file_format") or "").lower() or None,
                    merge_formats=getattr(writer_cls, "MERGE_FILE_FORMATS", None) or None,
                )
                entry["resolved_mode"] = resolved.mode
                entry["save_mode"] = resolved.save_mode
                entry["merge_keys"] = list(resolved.merge_keys)
            except UbunyeError as exc:
                problems.append(f"outputs.{name}: {str(exc).splitlines()[0]}")
        path = io_cfg.get("path") or ""
        if path and _is_local(path):
            existing = _on_disk(path)
            entry["target_exists"] = existing["exists"]
            if existing["exists"] and entry["save_mode"] == "errorifexists":
                problems.append(f"outputs.{name}: {path} already exists and the mode refuses that")
        outputs.append(entry)

    return {
        "task": task_name,
        "engine_version": _engine_version(),
        "backend": backend,
        "model": getattr(cfg.MODEL, "value", str(cfg.MODEL)),
        "config_version": cfg.VERSION,
        "config_hash": config_hash(cfg_dict),
        "variables": {k: v for k, v in (variables or {}).items() if v is not None},
        "env": {"referenced": env_referenced, "missing": env_missing},
        "inputs": inputs,
        "transform": transform,
        "outputs": outputs,
        "problems": problems,
        "warnings": warnings,
        "ok": not problems,
    }


def build_plans(
    tasks: Sequence[Tuple[str, UbunyeConfig, Path]],
    *,
    backend: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
    raw_yaml: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Plans for tasks run in this order: a later task may read an earlier one's output."""
    registry = Registry.from_entrypoints()
    produced: Dict[str, str] = {}
    plans = []
    for task_name, cfg, task_dir in tasks:
        plan = build_plan(
            cfg,
            task_name=task_name,
            task_dir=task_dir,
            backend=backend,
            variables=variables,
            raw_yaml=(raw_yaml or {}).get(task_name, ""),
            produced=dict(produced),
            registry=registry,
        )
        plans.append(plan)
        short = task_name.rsplit("/", 1)[-1]
        for out in cfg.CONFIG.outputs.values():
            path = _io_dict(out).get("path") or ""
            if path and _is_local(path):
                produced[_key(path)] = short
    return plans


def plan_problems(plans: Sequence[Dict[str, Any]]) -> Set[str]:
    """Tasks whose plan has at least one problem."""
    return {p["task"] for p in plans if p["problems"]}
