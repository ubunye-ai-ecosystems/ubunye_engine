"""What a run record needs beyond the data: the code, and the machine that ran it.

Two runs with the same config and the same input can still differ because the
transform changed or the environment did. The run record carries both, each as
a hash that is cheap to compare and a detail that says what changed.
"""

from __future__ import annotations

import hashlib
import importlib.metadata as md
import json
import platform
import sys
from pathlib import Path
from typing import Any, Dict, Optional

#: Packages whose versions can change a result. Missing ones are left out.
PACKAGES = (
    "ubunye-engine",
    "pyspark",
    "delta-spark",
    "pandas",
    "pyarrow",
    "numpy",
    "narwhals",
    "scikit-learn",
    "torch",
    "mlflow",
)


def code_hash(task_dir: Optional[str]) -> Optional[str]:
    """``sha256:<hex>`` of every Python file in the task folder, or None.

    Each file contributes its path relative to the folder and its bytes with line
    endings normalised, so a Windows and a Linux checkout of the same code agree.
    """
    if not task_dir:
        return None
    root = Path(task_dir)
    if not root.is_dir():
        return None
    files = sorted(
        p
        for p in root.rglob("*.py")
        if "__pycache__" not in p.parts
        and not any(part.startswith(".") for part in p.relative_to(root).parts)
    )
    if not files:
        return None
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode("utf-8") + b"\0")
        digest.update(path.read_bytes().replace(b"\r\n", b"\n") + b"\0")
    return "sha256:" + digest.hexdigest()


def environment() -> Dict[str, Any]:
    """The Python, the platform, and the versions of the packages that matter."""
    packages: Dict[str, str] = {}
    for name in PACKAGES:
        try:
            packages[name] = md.version(name)
        except md.PackageNotFoundError:
            continue
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": sys.platform,
        "machine": platform.machine(),
        "packages": packages,
    }


def environment_hash(env: Dict[str, Any]) -> str:
    """``sha256:<hex>`` of the environment, for a one-line comparison."""
    canonical = json.dumps(env, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
