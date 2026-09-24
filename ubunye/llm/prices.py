"""Model prices, dated and sourced: USD per million input and output tokens.

The engine ships only prices read from a provider's own public page, with the
date they were read. Everything else is yours to give, in a JSON file named by
``UBUNYE_LLM_PRICES`` (same shape as :data:`PRICES`)::

    {"openai_compatible": {"llama3.2": [0, 0], "gpt-5-mini": [0.25, 2.0]}}

or per port, ``llm.port(..., price=(input, output))``. A model id matches the
longest listed name it starts with, so ``claude-haiku-4-5-20251001`` is priced as
``claude-haiku-4-5``. A model with no price has an unknown cost, never zero: a
dollar ceiling on it fails closed.
"""

from __future__ import annotations

import json
import os
from typing import Dict, Optional, Tuple

Price = Tuple[float, float]  # USD per million (input, output) tokens

#: When the prices below were read.
AS_OF = "2026-09-24"
#: Where they were read.
SOURCES: Dict[str, str] = {"anthropic": "https://claude.com/pricing"}

#: Standard (not batch, not cached) prices, by backend then model name.
PRICES: Dict[str, Dict[str, Price]] = {
    "anthropic": {
        "claude-fable-5-1": (10.0, 50.0),
        "claude-opus-5-5": (4.0, 20.0),
        "claude-sonnet-5": (2.0, 10.0),
        "claude-haiku-4-5": (1.0, 5.0),
    },
}


def _extra() -> Dict[str, Dict[str, Price]]:
    path = os.environ.get("UBUNYE_LLM_PRICES")
    if not path:
        return {}
    from ubunye.core.errors import LLMError

    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return {
            backend: {name: (float(p[0]), float(p[1])) for name, p in models.items()}
            for backend, models in data.items()
        }
    except (OSError, ValueError, TypeError, IndexError, AttributeError) as exc:
        raise LLMError(
            f"UBUNYE_LLM_PRICES could not be read: {exc}",
            context={"File": path},
            hint='Expected {"backend": {"model": [input_usd_per_mtok, output_usd_per_mtok]}}.',
        ) from None


def lookup(backend: str, model: str) -> Optional[Price]:
    """The price of ``model`` on ``backend``, or None when it has none."""
    table = dict(PRICES.get(backend, {}))
    table.update(_extra().get(backend, {}))
    matches = [name for name in table if model == name or model.startswith(name)]
    if not matches:
        return None
    return table[max(matches, key=len)]


def cost(price: Price, input_tokens: int, output_tokens: int) -> float:
    """USD for a call."""
    return (input_tokens * price[0] + output_tokens * price[1]) / 1_000_000
