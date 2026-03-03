"""Broken fixture pipeline — raises intentionally to test error handling."""

from typing import Any, Dict

from ubunye.core.interfaces import Task


class BrokenTask(Task):
    """Task that always raises RuntimeError to simulate a pipeline failure."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError("Intentional test failure from BrokenTask")
