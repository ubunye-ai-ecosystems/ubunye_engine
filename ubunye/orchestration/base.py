# ubunye/orchestration/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping


class OrchestratorExporter(ABC):
    """Interface for converting Ubunye configs into scheduler artifacts."""

    @abstractmethod
    def export(
        self, config_path: Path, *, output_path: Path, options: Mapping[str, Any] | None = None
    ) -> Path:
        """Generate an artifact (DAG file, job JSON, etc.) and return its path."""
        ...
