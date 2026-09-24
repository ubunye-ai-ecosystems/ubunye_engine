"""A ceiling on model calls, enforced before each call is sent.

Three limits, any of them optional: ``max_usd`` (dollars), ``max_calls`` and
``max_seconds`` (wall clock since the budget opened). The engine opens one budget
per run from ``UBUNYE_LLM_MAX_USD``, ``UBUNYE_LLM_MAX_CALLS`` and
``UBUNYE_LLM_MAX_SECONDS``, shared by every port the task makes; a port's own
``max_usd=`` / ``max_calls=`` / ``max_seconds=`` add a second, narrower one.

Before a call, :meth:`Budget.reserve` counts its worst case: the prompt's tokens
counted high (a token per 3 characters, plus 4 per message) and the whole
``max_tokens`` of output. If spent plus reserved plus that worst case would pass
``max_usd``, or the call would be one too many, or the clock has run out, the call
is refused and never sent. After it, :meth:`Budget.settle` swaps the reservation
for the real cost. Replayed calls cost nothing and are never refused.
"""

from __future__ import annotations

import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ubunye.core.errors import LLMBudgetError, LLMError
from ubunye.llm import prices

ENV = {
    "max_usd": "UBUNYE_LLM_MAX_USD",
    "max_calls": "UBUNYE_LLM_MAX_CALLS",
    "max_seconds": "UBUNYE_LLM_MAX_SECONDS",
}


def estimate_input_tokens(request: Any) -> int:
    """Counted high on purpose: a budget must not be passed by a short count."""
    chars = len(request.system or "")
    count = 0
    for message in request.messages:
        content = message.get("content")
        chars += len(content) if isinstance(content, str) else len(str(content))
        count += 1
    return math.ceil(chars / 3) + 4 * max(count, 1)


@dataclass
class Budget:
    max_usd: Optional[float] = None
    max_calls: Optional[int] = None
    max_seconds: Optional[float] = None
    spent_usd: float = 0.0
    calls: int = 0
    refused: int = 0
    _reserved: float = 0.0
    _started: float = field(default_factory=lambda: time.monotonic())
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def limited(self) -> bool:
        return any(v is not None for v in (self.max_usd, self.max_calls, self.max_seconds))

    @classmethod
    def from_env(cls) -> "Budget":
        values: Dict[str, Any] = {}
        for name, var in ENV.items():
            raw = os.environ.get(var, "").strip()
            if not raw:
                continue
            try:
                values[name] = int(raw) if name == "max_calls" else float(raw)
            except ValueError:
                raise LLMError(
                    f"{var}={raw!r} is not a number",
                    hint=f"Set {var} to a number, or unset it for no limit.",
                ) from None
        return cls(**values)

    def reserve(self, *, backend: str, request: Any, price: Optional[prices.Price]) -> float:
        """Count a call's worst case against the limits, or refuse it. Returns the reservation."""
        worst = None
        if price is not None:
            worst = prices.cost(price, estimate_input_tokens(request), request.max_tokens)
        with self._lock:
            reason = None
            if self.max_seconds is not None and time.monotonic() - self._started > self.max_seconds:
                reason = f"max_seconds={self.max_seconds:g} has passed"
            elif self.max_calls is not None and self.calls + 1 > self.max_calls:
                reason = f"max_calls={self.max_calls} reached ({self.calls} calls made)"
            elif self.max_usd is not None and worst is None:
                reason = (
                    f"max_usd={self.max_usd:g} is set but {backend}/{request.model} has no price"
                )
            elif self.max_usd is not None and worst is not None:
                if self.spent_usd + self._reserved + worst > self.max_usd:
                    reason = (
                        f"max_usd={self.max_usd:g}: spent ${self.spent_usd:.6f}, "
                        f"${self._reserved:.6f} in flight, this call could cost ${worst:.6f}"
                    )
            if reason:
                self.refused += 1
                raise LLMBudgetError(
                    f"Model call refused before sending: {reason}",
                    context={"Backend": backend, "Model": request.model},
                    hint=_hint(reason),
                )
            self.calls += 1
            self._reserved += worst or 0.0
            return worst or 0.0

    def release(self, reserved: float) -> None:
        """Undo a reservation for a call that was not sent after all."""
        with self._lock:
            self._reserved = max(0.0, self._reserved - reserved)
            self.calls -= 1

    def settle(self, reserved: float, actual: Optional[float]) -> None:
        """Swap a reservation for what the call really cost (its worst case if unknown)."""
        with self._lock:
            self._reserved = max(0.0, self._reserved - reserved)
            self.spent_usd += reserved if actual is None else actual

    def summary(self) -> Dict[str, Any]:
        return {
            "max_usd": self.max_usd,
            "max_calls": self.max_calls,
            "max_seconds": self.max_seconds,
            "spent_usd": round(self.spent_usd, 8),
            "calls": self.calls,
            "refused": self.refused,
        }


def _hint(reason: str) -> str:
    if "no price" in reason:
        return (
            "Give it a price: llm.port(..., price=(input, output)) in USD per million "
            "tokens, or a UBUNYE_LLM_PRICES file."
        )
    return "Raise the limit (UBUNYE_LLM_MAX_USD, _MAX_CALLS, _MAX_SECONDS), or send fewer calls."
