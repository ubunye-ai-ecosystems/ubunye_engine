"""PostToolUse hook: format a Python file Claude just edited, with the repo's own hooks.

Runs the pinned black and ruff from .pre-commit-config.yaml on that one file, so an
edit never lands formatted differently from what CI checks. Quiet and never blocking:
if pre-commit is not installed, or the file is outside the repo, it does nothing.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys


def main() -> int:
    try:
        event = json.load(sys.stdin)
    except ValueError:
        return 0
    path = (event.get("tool_input") or {}).get("file_path") or ""
    root = os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
    if not path.endswith(".py") or not os.path.isfile(path):
        return 0
    try:
        rel = os.path.relpath(path, root)
    except ValueError:  # another drive on Windows
        return 0
    if rel.startswith(".."):
        return 0
    if shutil.which("pre-commit"):
        cmd = ["pre-commit"]
    elif importlib.util.find_spec("pre_commit"):
        cmd = [sys.executable, "-m", "pre_commit"]
    else:
        return 0
    for hook in ("black", "ruff-check"):
        subprocess.run(
            [*cmd, "run", hook, "--files", rel],
            cwd=root,
            capture_output=True,
            timeout=120,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
