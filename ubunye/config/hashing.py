"""One config hash, computed the same way by the plan and by the run record.

The run record hashed the config the engine ran, which by then had its
transform swapped for the internal task wrapper, so its hash never matched
anything a person or a plan could compute from the task folder. Both now hash
the resolved config as loaded, here.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict


def config_hash(cfg: Dict[str, Any]) -> str:
    """``sha256:<hex>`` of a resolved config dict (``UbunyeConfig.model_dump(mode="json")``)."""
    payload = json.dumps(cfg, sort_keys=True, default=str, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
