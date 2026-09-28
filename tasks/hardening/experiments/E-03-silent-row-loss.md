# E-03: Does anything notice when a run reads N rows and writes fewer, without a check saying so?

**Status:** answered (2026-09-28): visible, not enforceable
**Why:** Atum (AbsaOSS) exists because rows disappeared between stages unnoticed: control totals (count, sums, hashes) compared end to end.

## Method
Transforms that drop rows by accident (an inner join on a key with nulls, a filter on a null). Read the run record.

## Pass means
The run record shows rows in and rows out per step, and a declared reconciliation rule can fail the run.

## Result
Harness: tests/experiments/e03_e05_rows_and_schema.py. An inner join drops 100 of 1,000 orders. The run record shows
rows per input and output (orders 1000, customers 100, enriched 900), so the loss is
visible after the fact; the run succeeds and there is no rule to stop it (F-017).
