"""Dynamic loader for user-defined UbunyeModel subclasses.

Mirrors the pattern used in ubunye/cli/main.py (_run_single_task) for loading
user Task subclasses from transformations.py via importlib.util.spec_from_file_location.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Optional, Type

from ubunye.core.errors import ModelLoadError
from ubunye.models.base import UbunyeModel


def load_model_class(task_dir: Optional[str], class_name: str) -> Type[UbunyeModel]:
    """Dynamically load a UbunyeModel subclass.

    The ``class_name`` is a dotted path like ``"model.FraudRiskModel"`` where the
    first segment is the Python module filename (without ``.py``) and the rest is
    the class name.

    Resolution order:
    1. If ``task_dir`` is provided → load ``<module_segment>.py`` directly from
       that directory using ``importlib.util.spec_from_file_location``.
    2. If ``task_dir`` is ``None`` → attempt a standard ``importlib.import_module``
       (the module must already be on ``sys.path``; ``_run_single_task`` adds the
       task directory to ``sys.path`` before transforms are invoked, so this works
       automatically when called from the engine).

    Args:
        task_dir: Absolute or relative path to the directory containing the model
            file (e.g. the pipeline task directory). Pass ``None`` to rely on
            ``sys.path``.
        class_name: Dotted path to the class, e.g. ``"model.FraudRiskModel"`` or
            ``"models.risk.FraudRiskModel"``.

    Returns:
        The model *class* (not an instance). Callers should do ``cls()`` or
        ``cls.load(path)`` as needed.

    Raises:
        FileNotFoundError: The module file does not exist in ``task_dir``.
        ImportError: The class name was not found in the module.
        TypeError: The class does not subclass :class:`ubunye.models.base.UbunyeModel`.
    """
    parts = class_name.rsplit(".", 1)
    if len(parts) == 1:
        raise ModelLoadError(
            f"class_name must be in the form 'module.ClassName', got '{class_name}'.",
            context={"class_name": class_name},
            hint="Use a dotted path like 'model.MyModel' or 'models.risk.FraudModel'.",
        )
    module_path_str, cls_name = parts

    if task_dir is not None:
        module = _load_from_file(task_dir, module_path_str, class_name)
    else:
        module = _load_from_sys_path(module_path_str, class_name)

    cls = getattr(module, cls_name, None)
    if cls is None:
        raise ModelLoadError(
            f"Class '{cls_name}' not found in module '{module_path_str}'.",
            context={"Module": module_path_str, "Class": cls_name},
            hint="Make sure the class is defined at the module level.",
        )

    if not (isinstance(cls, type) and issubclass(cls, UbunyeModel)):
        raise ModelLoadError(
            f"'{cls_name}' must subclass UbunyeModel.",
            context={"Class": cls_name, "Actual base": type(cls).__name__},
            hint="Add 'from ubunye.models.base import UbunyeModel' and inherit from it.",
        )

    return cls


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_from_file(task_dir: str, module_path_str: str, full_class_name: str):
    """Load a module from a .py file in task_dir (handles nested paths like models/risk)."""
    base = Path(task_dir)
    # "models.risk" → "models/risk.py" or "models/risk/__init__.py"
    rel = Path(*module_path_str.split("."))
    candidate_file = base / rel.with_suffix(".py")
    candidate_pkg = base / rel / "__init__.py"

    if candidate_file.exists():
        module_file = candidate_file
        import_name = module_path_str.replace(".", "_ubunye_tmp_")
    elif candidate_pkg.exists():
        module_file = candidate_pkg
        import_name = module_path_str.replace(".", "_ubunye_tmp_")
    else:
        raise ModelLoadError(
            "Model file not found.",
            context={
                "Looked for": f"{candidate_file} or {candidate_pkg}",
                "class_name": full_class_name,
                "task_dir": task_dir,
            },
            hint="Check that the model file exists in your task directory.",
        )

    spec = importlib.util.spec_from_file_location(import_name, str(module_file))
    if spec is None or spec.loader is None:
        raise ModelLoadError(
            f"Could not create module spec from {module_file}.",
            context={"File": str(module_file)},
        )

    module = importlib.util.module_from_spec(spec)
    # Temporarily add task_dir to sys.path so relative imports inside the module work
    _added = False
    if str(base) not in sys.path:
        sys.path.insert(0, str(base))
        _added = True
    try:
        spec.loader.exec_module(module)
    finally:
        if _added and str(base) in sys.path:
            sys.path.remove(str(base))

    return module


def _load_from_sys_path(module_path_str: str, full_class_name: str):
    """Load via standard importlib (relies on module being on sys.path)."""
    try:
        return importlib.import_module(module_path_str)
    except ModuleNotFoundError as exc:
        raise ModelLoadError(
            f"Could not import '{module_path_str}' when loading '{full_class_name}'.",
            context={"Module": module_path_str, "class_name": full_class_name},
            hint="Provide task_dir or ensure the module is on sys.path.",
        ) from exc
