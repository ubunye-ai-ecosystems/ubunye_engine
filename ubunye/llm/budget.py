"""A ceiling on model calls, enforced before each call is sent.

Three limits, any of them optional: ``max_usd`` (dollars), ``max_calls`` and
``max_seconds`` (wall clock since the budget opened). The engine opens one budget
per run from ``UBUNYE_LLM_MAX_USD``, ``UBUNYE_LLM_MAX_CALLS`` and
``UBUNYE_LLM_MAX_SECONDS``, shared by every port the task makes; a port's own
``max_usd=`` / ``max_calls=`` / ``max_seconds=`` add a second, narrower one.
With a display currency (ADR 015, :mod:`ubunye.llm.currency`),
``UBUNYE_LLM_MAX_<CODE>`` (``UBUNYE_LLM_MAX_ZAR``) caps the run in that currency,
converted at the given rate; with ``UBUNYE_LLM_MAX_USD`` set too, both apply.

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
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ubunye.core.errors import LLMBudgetError, LLMError
from ubunye.llm import currency as fx
from ubunye.llm import prices

ENV = {
    "max_usd": "UBUNYE_LLM_MAX_USD",
    "max_calls": "UBUNYE_LLM_MAX_CALLS",
    "max_seconds": "UBUNYE_LLM_MAX_SECONDS",
}
#: A ceiling in a display currency: UBUNYE_LLM_MAX_ZAR, UBUNYE_LLM_MAX_EUR, ...
_MAX_IN = re.compile(r"^UBUNYE_LLM_MAX_([A-Z]{3})$")


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
    #: The display currency (ADR 015); None for US dollars.
    currency: Optional[fx.Currency] = None
    #: A ceiling in the display currency (``UBUNYE_LLM_MAX_<CODE>``).
    max_amount: Optional[float] = None
    _reserved: float = 0.0
    _started: float = field(default_factory=lambda: time.monotonic())
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def limited(self) -> bool:
        limits = (self.max_usd, self.max_calls, self.max_seconds, self.max_amount)
        return any(v is not None for v in limits)

    @classmethod
    def from_env(cls) -> "Budget":
        values: Dict[str, Any] = {}
        for name, var in ENV.items():
            raw = os.environ.get(var, "").strip()
            if raw:
                values[name] = _number(var, raw, int if name == "max_calls" else float)
        currency = fx.from_env()
        for var, raw in os.environ.items():
            code = _MAX_IN.match(var)
            if not code or code.group(1) == "USD" or not raw.strip():
                continue
            if currency is None or currency.code != code.group(1):
                shown = currency.code if currency else "USD"
                raise LLMError(
                    f"{var} is set but the display currency is {shown}, so it would cap nothing",
                    hint=f"Set {fx.ENV}={code.group(1)} and {fx.fx_var(code.group(1))}=RATE@DATE, "
                    f"or cap in the display currency (UBUNYE_LLM_MAX_{shown}).",
                )
            values["max_amount"] = _number(var, raw.strip(), float)
        return cls(**values, currency=currency)

    def _money_caps(self) -> List[Tuple[str, float, Optional[fx.Currency]]]:
        """Each money ceiling: its name, its value in dollars, the currency it is shown in."""
        caps: List[Tuple[str, float, Optional[fx.Currency]]] = []
        if self.max_usd is not None:
            caps.append((f"max_usd={self.max_usd:g}", self.max_usd, None))
        if self.max_amount is not None and self.currency is not None:
            name = f"max_{self.currency.code.lower()}={self.max_amount:g}"
            caps.append((name, self.max_amount / self.currency.per_usd, self.currency))
        return caps

    def reserve(self, *, backend: str, request: Any, price: Optional[prices.Price]) -> float:
        """Count a call's worst case against the limits, or refuse it. Returns the reservation."""
        worst = None
        if price is not None:
            worst = prices.cost(price, estimate_input_tokens(request), request.max_tokens)
        caps = self._money_caps()
        with self._lock:
            reason = None
            if self.max_seconds is not None and time.monotonic() - self._started > self.max_seconds:
                reason = f"max_seconds={self.max_seconds:g} has passed"
            elif self.max_calls is not None and self.calls + 1 > self.max_calls:
                reason = f"max_calls={self.max_calls} reached ({self.calls} calls made)"
            elif caps and worst is None:
                reason = f"{caps[0][0]} is set but {backend}/{request.model} has no price"
            elif worst is not None:
                for name, cap, cur in caps:
                    if self.spent_usd + self._reserved + worst > cap:
                        reason = (
                            f"{name}: spent {fx.money(self.spent_usd, cur)}, "
                            f"{fx.money(self._reserved, cur)} in flight, "
                            f"this call could cost {fx.money(worst, cur)}"
                        )
                        break
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
        out: Dict[str, Any] = {
            "max_usd": self.max_usd,
            "max_calls": self.max_calls,
            "max_seconds": self.max_seconds,
            "spent_usd": round(self.spent_usd, 8),
            "calls": self.calls,
            "refused": self.refused,
        }
        if self.currency is not None:
            if self.max_amount is not None:
                out[f"max_{self.currency.code.lower()}"] = self.max_amount
            out["currency"] = self.currency.to_dict()
        return out


def _hint(reason: str) -> str:
    if "no price" in reason:
        return (
            "Give it a price: llm.port(..., price=(input, output)) in USD per million "
            "tokens, or a UBUNYE_LLM_PRICES file. A free local model (Ollama, vLLM): "
            "price=(0, 0)."
        )
    return (
        "Raise the limit (UBUNYE_LLM_MAX_USD or _MAX_<CURRENCY>, _MAX_CALLS, _MAX_SECONDS), "
        "or send fewer calls."
    )


def _number(var: str, raw: str, kind: Any) -> Any:
    try:
        return kind(raw)
    except ValueError:
        raise LLMError(
            f"{var}={raw!r} is not a number",
            hint=f"Set {var} to a number, or unset it for no limit.",
        ) from None
