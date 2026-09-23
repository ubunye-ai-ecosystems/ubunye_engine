"""The dry run: everything that can be checked before a single row moves.

``ubunye plan`` used to print the names of the inputs, the transform type and the
names of the outputs. That is the config read back to you, not a plan. It told you
nothing you did not already know, and it caught nothing.

This module answers the question a plan is supposed to answer: **if I run this
now, what happens, and what will stop it?** It resolves the config, checks the
environment variables it references, looks for the local inputs, resolves the
transform class, and asks each writer to resolve its write mode, which is where
``merge`` without ``merge_keys`` and ``overwrite_partitions`` without
``partitionBy`` are caught. Those failures used to surface on a cluster, after the
transform had already run.

Nothing here starts a backend, opens a session, reads a table or costs money. It
runs on a laptop with no Java, in about the time it takes to read a YAML file, and
it returns a plain dict so both a person and a program can read it.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ubunye.config.schema import UbunyeConfig
from ubunye.core import write_modes
from ubunye.core.errors import UbunyeError
from ubunye.core.runtime import Registry
from ubunye.lineage.context import _location_from_io_cfg

#: Connectors that need a JVM. Named here only to explain a `--backend pandas`
#: mismatch in the plan; the engine still asks the plugin, it does not decide for it.
_SPARK_ONLY_HINT = {"delta", "unity", "hive", "jdbc", "binary"}


def _is_local_path(path: str) -> bool:
    """True for a path this process can look at without a distributed filesystem."""
    if not path:
        return False
    if path.startswith("file://"):
        return True
    return "://" not in path


def _local_path(path: str) -> str:
    """``file:///tmp/x`` and ``file:///C:/x`` back to what the OS understands."""
    if path.startswith("file://"):
        path = path[len("file://") :]
        if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
            path = path[1:]
    return path


def _describe_local(path: str) -> Dict[str, Any]:
    """What is actually on disk at ``path``, if anything."""
    target = Path(_local_path(path))
    if not target.exists():
        return {"exists": False, "files": 0, "bytes": 0}
    if target.is_file():
        return {"exists": True, "files": 1, "bytes": target.stat().st_size}
    files = [p for p in target.rglob("*") if p.is_file()]
    return {
        "exists": True,
        "files": len(files),
        "bytes": sum(p.stat().st_size for p in files),
    }


def _io_dict(io_cfg: Any) -> Dict[str, Any]:
    """An IOConfig back to the plain dict the connectors are given."""
    cfg = io_cfg.model_dump(exclude_none=True, mode="json")
    cfg.update(io_cfg.model_extra or {})
    return cfg


def _writer_modes(writer_cls: Any) -> Tuple[Any, str]:
    """What this writer says it supports, and what it does when nothing is asked.

    Writers declare their supported set as a module constant and their merge
    capability on the class. Ask in that order, then fall back to the base
    contract, so a duck-typed third-party writer that declares neither still
    plans without an ``AttributeError``.
    """
    supported: Any = None
    default = "append"
    module_name = getattr(writer_cls, "__module__", None)
    if module_name:
        try:
            mod = importlib.import_module(module_name)
            supported = getattr(mod, "SUPPORTED_MODES", None)
            default = getattr(mod, "DEFAULT_MODE", default)
        except Exception:  # noqa: BLE001 — a plugin that will not import is reported elsewhere
            pass
    if supported is None:
        supported = getattr(writer_cls, "SUPPORTED_MODES", write_modes.NATIVE_SAVE_MODES)
    return supported, default


def config_hash(cfg: UbunyeConfig) -> str:
    """A stable hash of the resolved config, the same shape lineage records."""
    payload = json.dumps(cfg.model_dump(mode="json"), sort_keys=True, default=str).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def build_plan(
    cfg: UbunyeConfig,
    *,
    task_dir: Optional[Path] = None,
    task_name: str = "",
    backend: str = "spark",
    variables: Optional[Dict[str, Any]] = None,
    raw_yaml: str = "",
    sample: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a JSON-able plan for one task.

    ``problems`` are things that will stop the run. ``warnings`` are things worth
    knowing that will not. An empty ``problems`` list is the closest thing to
    "this will work" that can be said without running it.
    """
    problems: List[str] = []
    warnings: List[str] = []

    registry = Registry.from_entrypoints()
    readers, writers = registry.readers, registry.writers

    # --- the environment this config is asking for -------------------------
    env_referenced: List[str] = []
    if raw_yaml:
        from ubunye.config.resolver import extract_env_references

        env_referenced = sorted(extract_env_references(raw_yaml))
    env_missing = [name for name in env_referenced if not os.environ.get(name)]

    # --- inputs ------------------------------------------------------------
    inputs: List[Dict[str, Any]] = []
    for name in sorted(cfg.CONFIG.inputs):
        io_cfg = _io_dict(cfg.CONFIG.inputs[name])
        fmt = io_cfg.get("format", "")
        entry: Dict[str, Any] = {
            "name": name,
            "format": fmt,
            "file_format": io_cfg.get("file_format"),
            "location": _location_from_io_cfg(io_cfg),
            "checked": False,
        }
        path = io_cfg.get("path") or ""
        if path and _is_local_path(path):
            entry.update(_describe_local(path))
            entry["checked"] = True
            if not entry["exists"]:
                problems.append(f"inputs.{name}: nothing at {path}")
            elif entry["files"] == 0:
                warnings.append(f"inputs.{name}: {path} exists but holds no files")
        if backend == "pandas" and fmt in _SPARK_ONLY_HINT:
            problems.append(
                f"inputs.{name}: format '{fmt}' needs Spark, and --backend pandas has no JVM"
            )
        if fmt not in readers:
            problems.append(f"inputs.{name}: no reader plugin registered for format '{fmt}'")
        inputs.append(entry)

    # --- transform ---------------------------------------------------------
    transform: Dict[str, Any] = {
        "type": cfg.CONFIG.transform.type,
        "params": cfg.CONFIG.transform.params,
        "source": None,
    }
    if cfg.CONFIG.transform.type is None and task_dir is not None:
        module_path = task_dir / "transformations.py"
        if not module_path.exists():
            problems.append(
                "transform: no transform type declared and no transformations.py in the task folder"
            )
        else:
            transform["source"] = str(module_path)
            try:
                from ubunye.core.task_runner import _load_task_class

                transform["class"] = _load_task_class(task_dir).__name__
            except UbunyeError as exc:
                problems.append(f"transform: {str(exc).splitlines()[0]}")
            except Exception as exc:  # noqa: BLE001 — user code, any import error is possible
                problems.append(f"transform: transformations.py did not import ({exc})")
    elif cfg.CONFIG.transform.type is not None:
        if cfg.CONFIG.transform.type not in registry.transforms:
            problems.append(
                f"transform: no transform plugin registered for type "
                f"'{cfg.CONFIG.transform.type}'"
            )

    # --- outputs, including the write mode nobody resolves until write time --
    outputs: List[Dict[str, Any]] = []
    for name in sorted(cfg.CONFIG.outputs):
        io_cfg = _io_dict(cfg.CONFIG.outputs[name])
        fmt = io_cfg.get("format", "")
        entry = {
            "name": name,
            "format": fmt,
            "file_format": io_cfg.get("file_format"),
            "location": _location_from_io_cfg(io_cfg),
            "requested_mode": io_cfg.get("mode"),
            "resolved_mode": None,
            "save_mode": None,
            "merge_keys": [],
            "partition_by": io_cfg.get("partitionBy") or [],
        }
        writer_cls = writers.get(fmt)
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
                # This is the whole point of planning: `merge` with no keys and
                # `overwrite_partitions` with no partitions used to fail on a
                # cluster, after the transform had already run.
                problems.append(f"outputs.{name}: {str(exc).splitlines()[0]}")
        if backend == "pandas" and fmt in _SPARK_ONLY_HINT:
            problems.append(
                f"outputs.{name}: format '{fmt}' needs Spark, and --backend pandas has no JVM"
            )
        path = io_cfg.get("path") or ""
        if path and _is_local_path(path):
            existing = _describe_local(path)
            entry["target_exists"] = existing["exists"]
            if existing["exists"] and entry["resolved_mode"] in ("overwrite", "errorifexists"):
                warnings.append(
                    f"outputs.{name}: {path} already holds {existing['files']} file(s) "
                    f"and the mode is '{entry['resolved_mode']}'"
                )
        outputs.append(entry)

    if env_missing:
        # Missing at plan time is not always fatal (a default filter may cover it),
        # but it is the single most common reason a run dies on someone else's box.
        warnings.append("environment variables referenced but not set: " + ", ".join(env_missing))

    return {
        "task": task_name,
        "engine_version": _engine_version(),
        "backend": backend,
        "model": cfg.MODEL.value if hasattr(cfg.MODEL, "value") else str(cfg.MODEL),
        "config_version": cfg.VERSION,
        "config_hash": config_hash(cfg),
        "variables": dict(variables or {}),
        "sample_rows": sample,
        "env": {"referenced": env_referenced, "missing": env_missing},
        "inputs": inputs,
        "transform": transform,
        "outputs": outputs,
        "problems": problems,
        "warnings": warnings,
        "ok": not problems,
    }


def _engine_version() -> str:
    try:
        from importlib.metadata import version

        return version("ubunye-engine")
    except Exception:  # noqa: BLE001
        return "unknown"
