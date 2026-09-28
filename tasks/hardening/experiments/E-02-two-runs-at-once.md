# E-02: The same task and date run twice at once: does one silently overwrite or duplicate the other?

**Status:** answered (2026-09-28): overwrite safe; append doubles
**Why:** Pramen needed lease locks per (table, information date) after concurrent runs collided.

## Method
Start two overlapping runs of one task with the same variables, for each write mode.

## Pass means
The result equals one run's output, or the second run is refused with a clear message.

## Result
Harness: tests/experiments/e02_concurrent.py, 10 pairs, 1,000,000 rows, pandas backend, the second run started
0 to 0.45 s after the first.

| | Result |
|---|---|
| `overwrite` output | correct in 10 of 10 (the staging swap holds) |
| `append` output | doubled in 7 of 10: both runs succeeded (F-019) |
| closest starts | 3 of 10: second run failed with a raw `[WinError 5]` (F-020); data correct |
| debris | none |
