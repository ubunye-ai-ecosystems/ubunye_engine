"""Progress for a long ``complete_many``: calls done of total, and spend so far.

A line goes to stderr (stdout may carry a protocol, as under ``ubunye mcp``)::

    LLM anthropic/claude-haiku-4-5: 90/300 calls (30%), 1m48s, $0.0412 spent of $2

A line is written only when at least :data:`EVERY_S` seconds and at least
:data:`EVERY_SHARE` of the calls have passed since the last one, so a slow run
gets about ten lines and a fast one none. A batch of fewer than :data:`MIN_CALLS`
prompts never writes. When a batch wrote a line, its end writes one more.
"""

from __future__ import annotations

import math
import sys
import threading
import time
from typing import Any, Callable, Optional

#: Seconds between two lines, at least.
EVERY_S = 10.0
#: Share of the calls between two lines, at least.
EVERY_SHARE = 0.10
#: A batch smaller than this never writes a line.
MIN_CALLS = 20
#: The clock lines are timed by (a test swaps it for a fake one).
CLOCK: Callable[[], float] = time.monotonic


def _stderr(line: str) -> None:
    print(line, file=sys.stderr, flush=True)


class Progress:
    """Counts finished calls; thread safe, so concurrent workers can tick it."""

    def __init__(
        self,
        label: str,
        total: int,
        *,
        budget: Any = None,
        clock: Optional[Callable[[], float]] = None,
        write: Callable[[str], None] = _stderr,
    ) -> None:
        self.label = label
        self.total = total
        self.budget = budget
        self.clock = clock or CLOCK
        self.write = write
        self.done = 0
        self.lines = 0
        self._lock = threading.Lock()
        self._step = max(1, math.ceil(total * EVERY_SHARE))
        self._started = self.clock()
        self._last_at = self._started
        self._last_done = 0

    def tick(self) -> None:
        """One call finished (answered or failed)."""
        if self.total < MIN_CALLS:
            return
        with self._lock:
            self.done += 1
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
        pct = 100 * self.done // self.total
        text = f"LLM {self.label}: {self.done}/{self.total} calls ({pct}%), {_elapsed(now - self._started)}"
        spent = _spent(self.budget)
        if spent:
            text += f", {spent}"
        self.write(text)


def _elapsed(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 60}m{s % 60:02d}s" if s >= 60 else f"{s}s"


def _spent(budget: Optional[Any]) -> str:
    """What the budget has spent; empty when no budget is keeping count."""
    if budget is None or not getattr(budget, "limited", False):
        return ""
    text = f"${budget.spent_usd:.4f} spent"
    if budget.max_usd is not None:
        text += f" of ${budget.max_usd:g}"
    return text
