"""Pytest fixtures: put both task directories on sys.path for imports."""

from __future__ import annotations

import sys
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parent.parent / "pipelines" / "titanic" / "ml"
for task in ("train_classifier", "predict_classifier"):
    task_dir = ML_ROOT / task
    if str(task_dir) not in sys.path:
        sys.path.insert(0, str(task_dir))
