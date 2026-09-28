# E-03: Does anything notice when a run reads N rows and writes fewer, without a check saying so?

**Status:** planned
**Why:** Atum (AbsaOSS) exists because rows disappeared between stages unnoticed: control totals (count, sums, hashes) compared end to end.

## Method
Transforms that drop rows by accident (an inner join on a key with nulls, a filter on a null). Read the run record.

## Pass means
The run record shows rows in and rows out per step, and a declared reconciliation rule can fail the run.

## Result
Not run yet.
