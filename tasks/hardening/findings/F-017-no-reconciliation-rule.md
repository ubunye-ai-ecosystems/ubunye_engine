# F-017: rows lost between input and output are recorded but cannot stop the run

**Status:** fixed on fix/f017-f018-contracts (reconcile)
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

## Fix
`CONFIG.expectations.<output>.reconcile`, part of the existing expectations (same
check point, same `ExpectationError`, same results in the record):

```yaml
expectations:
  enriched:
    reconcile:
      - input: orders
        rows: {max_lost: 0}                    # or "1%"; max_gained too
        sum: {column: amount, tolerance: 0.01} # or "0.1%"; input_column if renamed
        severity: fail                         # default; warn allowed
```

Decisions:

- **Severity is `fail` or `warn`**, the words the rules already use; `quarantine`
  is refused (a reconcile is about the whole output, like `row_count`).
- **Quarantined rows count as carried over.** Quarantine means set aside with a
  reason, not lost. The output is counted before the split, in the same pass as
  the rules, so it costs nothing extra.
- **A bound left out is not checked**; `rows` needs at least one. `max_lost: 0`
  says exactly "every order must reach the output", and says nothing about a fan
  out, which is `max_gained` (or a `unique` rule).
- **The input is counted as the transform received it**, one pass per input
  (row count and every sum asked of it), however many outputs reconcile with it.
  On Spark that pass reads the source again: ADR 009 holds outputs, not inputs.
- **Sums leave out null and NaN** on both sides, as the rules do (F-045): Spark's
  sum of a column holding NaN is NaN, pandas skips it. Integer sums compare exactly.
- **A result carries `detail`** (a new, optional field) with the numbers in words,
  since "2 of 10" does not say which way or what was allowed. The error, `lineage
  show` (JSON), `lineage trace` and `gate` show it.
- An unknown input name is a config error at load (`ubunye validate`).
- The notebook path (`Engine.write_outputs`) takes `inputs=`; the notebook passes
  what it read. Without them a reconcile fails with a clear message, never skips.

Evidence, `tests/experiments/e03_e05_rows_and_schema.py` on the pandas backend:

| | run exit | output written | message |
|---|---|---|---|
| before (no contract, old code) | 0 | yes, 900 of 1000 orders | none |
| after (`--contracts`) | 1 | no | `rows_from_orders (reconcile): 1000 rows read from orders, 900 reached enriched: 100 lost, 0 gained (at most 0 lost)`; `price_sum_from_orders`: 12997 in, 11700 out, difference -1297 |

Tests: `tests/unit/core/test_expectations_reconcile.py` (36, with `ubunye validate`; 35 fail on the old code,
which refuses the `reconcile` key, and the one that passes checks that an unknown key
is refused). Spark parity in `tests/integration/test_expectations_spark.py`: the same
results as pandas for rows and a sum over NaN and null, the refusal, and a whole task
on Spark that drops orders and writes nothing (run by CI on Spark 4 and 3.5; not run on
the dev box).

**Status:** fixed on fix/f017-f018-contracts.
