"""Fixture pipeline transformation — pass-through (noop) for reproducibility tests."""
from typing import Dict, Any
from ubunye.core.interfaces import Task


class TestEtlTask(Task):
    """Pass-through task: returns all sources unchanged."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        return sources
