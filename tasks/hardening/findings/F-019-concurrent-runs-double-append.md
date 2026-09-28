# F-019: two runs of the same task and date both succeed, and an append doubles

**Status:** open
**Severity:** major
**Source:** experiment E-02 (2026-09-28)
**Promise:** 5

## What happens
Two runs of one task with the same variables, overlapping (a manual rerun while the
scheduled one runs, two schedulers). Both succeed; the `append` output holds the batch
twice. Nothing refuses the second run or warns. Pramen (AbsaOSS) added lease locks per
(table, information date) after exactly this in production.

## Repro
tests/experiments/e02_concurrent.py, 10 pairs, 1,000,000 rows, pandas backend.

## Expected
The second run of the same (task, variables) while one is live is refused with a clear
message naming the live run, or waits; a stale lease (a killed run, F-013) expires.

## Evidence
7 of 10 pairs: both exit 0, events 2,000,000 rows (the batch twice). The `overwrite`
output was correct in 10 of 10.
