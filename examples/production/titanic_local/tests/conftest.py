"""Put the task directory on sys.path so tests can import transformations.py."""

from pathlib import Path
import sys

TASK_DIR = (
    Path(__file__).resolve().parent.parent
    / "pipelines"
    / "titanic"
    / "analytics"
    / "survival_by_class"
)

if str(TASK_DIR) not in sys.path:
    sys.path.insert(0, str(TASK_DIR))
