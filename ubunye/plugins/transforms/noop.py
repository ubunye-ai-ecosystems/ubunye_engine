"""No-op transform plugin."""
from __future__ import annotations
from typing import Dict, Any
from ubunye.core.interfaces import Transform, Backend


class NoOpTransform(Transform):
    """Returns inputs unchanged."""
    def apply(self, inputs: Dict[str, Any], cfg: dict, backend: Backend) -> Dict[str, Any]:
        return inputs
