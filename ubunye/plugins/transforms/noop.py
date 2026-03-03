"""No-op transform plugin."""
from __future__ import annotations

from typing import Any, Dict

from ubunye.core.interfaces import Backend, Transform


class NoOpTransform(Transform):
    """Returns inputs unchanged."""
    def apply(self, inputs: Dict[str, Any], cfg: dict, backend: Backend) -> Dict[str, Any]:
        return inputs
