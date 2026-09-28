# F-017: rows lost between input and output are recorded but cannot stop the run

**Status:** open
**Severity:** major
**Source:** experiment E-03 (2026-09-28)
**Promise:** 5 (nothing is lost silently)

## What happens
An inner join drops the 100 orders (of 1,000) whose customer is unknown. The run
record shows it (orders 1,000 in, enriched 900 out), but the run succeeds, and a task
cannot declare "every order must reach the output" or "at most 1% may be dropped".
Expectations have `row_count` with fixed numbers only. Atum (AbsaOSS) exists because
this loss went unnoticed in production: control totals compared from input to output.

## Repro
tests/experiments/e03_e05_rows_and_schema.py, the E-03 part.

## Expected
A declared reconciliation between an input and an output (row count, and optionally a
sum of a column) with a tolerance, checked before anything is written, like the other
expectations, and shown in the record.

## Evidence
Record: input orders 1000 rows, output enriched 900 rows; run exit 0.
