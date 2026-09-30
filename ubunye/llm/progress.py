"""Progress for a long ``complete_many``: calls done of total, and spend so far.

A line goes to stderr (stdout may carry a protocol, as under ``ubunye mcp``)::

    LLM anthropic/claude-haiku-4-5: 90/300 calls (30%), 1m48s, $0.0412 spent of $2

A line is written only when at least :data:`EVERY_S` seconds and at least
:data:`EVERY_SHARE` of the calls have passed since the last one, so a slow run
gets about ten lines and a fast one none. A batch of fewer than :data:`MIN_CALLS`
prompts never writes. When a batch wrote a line, its end writes one more.

Calls that got no answer are named (``50 answered, 250 refused by the budget``).
The spend shows only for a budget that enforces a dollar limit. Progress is best
effort: a missing or broken stderr drops the line and never stops the batch.
``UBUNYE_LLM_PROGRESS=0`` (or false, no, off) turns it off.
"""

from __future__ import annotations

import math
import os
import sys
import threading
import time
from typing import Any, Callable, Optional, Sequence, Tuple

#: Seconds between two lines, at least.
EVERY_S = 10.0
#: Share of the calls between two lines, at least.
EVERY_SHARE = 0.10
#: A batch smaller than this never writes a line.
MIN_CALLS = 20
#: The clock lines are timed by (a test swaps it for a fake one).
CLOCK: Callable[[], float] = time.monotonic
#: Set to one of these to turn progress off.
ENV = "UBUNYE_LLM_PROGRESS"
OFF = frozenset({"0", "false", "no", "off"})

#: What a finished call was.
ANSWERED, FAILED, REFUSED = "answered", "failed", "refused"


def _stderr(line: str) -> None:
    err = sys.stderr
    if err is None:  # pythonw, some services: never fall back to stdout
        return
    print(line, file=err, flush=True)


def enabled() -> bool:
    return os.environ.get(ENV, "").strip().lower() not in OFF


class Progress:
    """Counts finished calls; thread safe, so concurrent workers can tick it.

    ``budgets`` is a list of ``(name, budget)``: the budgets that enforce limits on
    these calls (``run`` and ``port``).
    """

    def __init__(
        self,
        label: str,
        total: int,
        *,
        budgets: Sequence[Tuple[str, Any]] = (),
        clock: Optional[Callable[[], float]] = None,
        write: Callable[[str], None] = _stderr,
    ) -> None:
        self.label = label
        self.total = total
        self.budgets = [(n, b) for n, b in budgets if b is not None]
        self.clock = clock or CLOCK
        self.write = write
        self.on = total >= MIN_CALLS and enabled()
        self.done = 0
        self.counts = {ANSWERED: 0, FAILED: 0, REFUSED: 0}
        self.lines = 0
        self._lock = threading.Lock()
        self._step = max(1, math.ceil(total * EVERY_SHARE))
        self._started = self.clock()
        self._last_at = self._started
        self._last_done = 0

    def tick(self, outcome: str = ANSWERED) -> None:
        """One call finished: ``answered``, ``failed`` or ``refused`` by a budget."""
        if not self.on:
            return
        with self._lock:
            self.done += 1
            self.counts[outcome] += 1
            now = self.clock()
            if self.done >= self.total:
                if self.lines:
                    self._line(now)
                return
            if now - self._last_at >= EVERY_S and self.done - self._last_done >= self._step:
                self._line(now)

    def _line(self, now: float) -> None:
        self._last_at, self._last_done = now, self.done
        self.lines += 1
        parts = [f"{self.done}/{self.total} calls ({100 * self.done // self.total}%)"]
        if self.counts[ANSWERED] != self.done:
            parts.append(f"{self.counts[ANSWERED]} answered")
            if self.counts[FAILED]:
                parts.append(f"{self.counts[FAILED]} failed")
            if self.counts[REFUSED]:
                parts.append(f"{self.counts[REFUSED]} refused by the budget")
        parts.append(_elapsed(now - self._started))
        spent = _spent(self.budgets)
        if spent:
            parts.append(spent)
        try:
            self.write(f"LLM {self.label}: " + ", ".join(parts))
        except Exception:  # best effort: a broken stderr never stops a batch
            pass


def _elapsed(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 60}m{s % 60:02d}s" if s >= 60 else f"{s}s"


def _spent(budgets: Sequence[Tuple[str, Any]]) -> str:
    """The spend of each budget that enforces a dollar limit; empty when none does."""
    money = [(n, b) for n, b in budgets if getattr(b, "max_usd", None) is not None]
    if len(money) == 1:
        ((_, b),) = money
        return f"${b.spent_usd:.4f} spent of ${b.max_usd:g}"
    return ", ".join(f"{n} ${b.spent_usd:.4f} of ${b.max_usd:g}" for n, b in money)
