# F-018: a source column that changes type can produce silently wrong output

**Status:** open
**Severity:** major
**Source:** experiment E-05 (2026-09-28)
**Promise:** 5

## What happens
After a good run, the orders source writes `price` as text instead of a number. The
run succeeds and writes output: in pandas a number times a text repeats the text, so
`total` becomes "11.011.0" and "12.012.012.0". Nothing fails. `ubunye gate` against
the previous run does catch it ("schema enriched: columns or types changed; changed:
input orders"), but only if the user runs it; nothing at run time does.

## Repro
tests/experiments/e03_e05_rows_and_schema.py, the "type changed (price to text)" case.

## Expected
A task can declare what an input must look like (columns and types, the way outputs
declare expectations), and a run whose input no longer matches stops before writing,
naming the column. The same declaration is the natural home of a data contract (ODCS).

## Evidence
total = qty * price with price "11.0": "11.011.0". Run exit 0; gate exit 1.
