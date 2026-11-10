"""Model costs in a display currency (ADR 015): prices stay in US dollars.

``UBUNYE_LLM_CURRENCY`` names an ISO 4217 code (``ZAR``). The engine never fetches
or guesses a rate: ``UBUNYE_LLM_FX_<CODE>`` gives units of that currency per US
dollar and the day the rate was read, as ``RATE@YYYY-MM-DD``::

    UBUNYE_LLM_CURRENCY=ZAR
    UBUNYE_LLM_FX_ZAR=18.40@2026-11-10

Unset, or ``USD``, means dollars, as before. Only what is shown and capped is
converted; every recorded cost stays in dollars, next to the rate and date used.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional

from ubunye.core.errors import LLMError

ENV = "UBUNYE_LLM_CURRENCY"
_CODE = re.compile(r"^[A-Z]{3}$")


@dataclass(frozen=True)
class Currency:
    code: str  #: ISO 4217, e.g. "ZAR"
    per_usd: float  #: units of ``code`` per US dollar
    as_of: str  #: the day the rate was read, YYYY-MM-DD

    def amount(self, usd: float) -> float:
        return usd * self.per_usd

    def money(self, usd: float, places: int = 6) -> str:
        return f"{self.code} {self.amount(usd):.{places}f}"

    def note(self) -> str:
        return f"{self.code} at {self.per_usd:g} per USD, rate as of {self.as_of}"

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "per_usd": self.per_usd, "as_of": self.as_of}


def money(usd: float, currency: Optional[Currency], places: int = 6) -> str:
    """``$1.234567``, or the same amount in the display currency."""
    return f"${usd:.{places}f}" if currency is None else currency.money(usd, places)


def fx_var(code: str) -> str:
    return f"UBUNYE_LLM_FX_{code}"


def from_env() -> Optional[Currency]:
    """The display currency, or None for US dollars. A currency with no rate is an error."""
    code = os.environ.get(ENV, "").strip().upper()
    if not code or code == "USD":
        return None
    if not _CODE.match(code):
        raise LLMError(
            f"{ENV}={code!r} is not a three-letter ISO 4217 currency code",
            hint=f"Set {ENV} to a code such as ZAR, EUR or GBP, or unset it for US dollars.",
        )
    var = fx_var(code)
    raw = os.environ.get(var, "").strip()
    example = f"{var}=RATE@{date.today().isoformat()}"
    if not raw:
        raise LLMError(
            f"{ENV}={code} is set but {var} is not: the engine never fetches or guesses "
            "an exchange rate",
            hint=f"Set {example}: {code} per US dollar and the day you read it. "
            f"Or unset {ENV} for US dollars.",
        )
    rate, _, as_of = raw.partition("@")
    try:
        per_usd = float(rate)
        day = date.fromisoformat(as_of.strip())
    except ValueError:
        per_usd, day = 0.0, None
    if not per_usd > 0 or day is None:
        raise LLMError(
            f"{var}={raw!r} is not a positive rate and a date",
            hint=f"Write it as {example}, {code} per US dollar and the day you read it.",
        )
    return Currency(code=code, per_usd=per_usd, as_of=day.isoformat())


def from_dict(data: Any) -> Optional[Currency]:
    """The currency a run record kept, or None (dollars; every record before 0.9.2)."""
    if not data:
        return None
    return Currency(code=data["code"], per_usd=float(data["per_usd"]), as_of=data["as_of"])


def from_record(record: Any) -> Optional[Currency]:
    return from_dict((record.llm_budget or {}).get("currency"))
