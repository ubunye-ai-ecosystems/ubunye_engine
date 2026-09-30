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

## Skeptic review

The skeptic's probes (`p3_reconcile.py`, `p4_engine.py`, `p1_config.py`,
`p2_validate_cli.py`, `p7_notebook.py`) found five real faults in the first fix.
All fixed in the third commit on the branch; each has a unit test that fails on
813d515 and passes after.

| # | Fault | Before | After |
|---|---|---|---|
| 1 | Inputs were counted after the transform, so a pandas transform that changed its input in place hid the loss | in-place `drop` of 2 orders: "8 rows read, 8 reached: 0 lost", run wrote 8; zeroing `amount`: "sum 0 in, 0 out" | counted before the transform (`Engine._inspect_inputs`, in `run` and the notebook): "10 rows read from orders, 8 reached enriched: 2 lost", and "550 in, 0 out: difference -550"; nothing written |
| 2 | Decimal sums compared as floats | decimal(38,2) off by 0.01 on a 1.2e19 total: "difference 0", passed at tolerance 0 | exact: "12345678901234567890.31 ... 12345678901234567890.32: difference 0.01", fails; tolerance as `Decimal(str(t))` |
| 3 | `ubunye validate` crashed with a bare TypeError | blank `tolerance:`, `max_lost: [1]`: TypeError; NaN and inf tolerances accepted | "tolerance is empty; give a number or a percentage like '1%', or leave it out", "not [1]", "must be a finite number" |
| 7 | Integer sums wrapped at 2**63 | 3 x 2**62 in, 2 x 2**62 out: sums printed as negative numbers | exact: 13835058055282163712 in, 9223372036854775808 out. pandas and Arrow sum integers as decimal128(38,0) and decimals as decimal256(76,s); Spark sums integers as `decimal(38,0)` |
| 8 | Equal infinite totals failed; two checks on one input clashed by name | inf in and out: "difference nan", failed | equal totals match (both NaN too); a reconcile item takes `name`, results `<name>_rows` and `<name>_sum` |
| 6 | The notebook compared with the full read, not what the transform got | `transform(head(4))` then write: "10 read, 4 reached: 6 lost"; never read: an error hinting at `Engine.write_outputs` | frames passed to `transform()` are checked and counted when it starts; the sample writes. With nothing counted the error says "In a notebook, call read() or transform() before write()" |

The sums now take their own pass on the output side (natively: Spark
aggregation, Arrow compute), over the held output (ADR 009). Float totals are
printed as a round trip, so a float sum that differs in the 16th digit shows it
(`p3`: 100,000 floats reordered now report a difference of 7.6e-06 at tolerance
0, which the old 10 digit print hid); the docs say to give floats a tolerance.
Documented limits: counting rows sees the net change only (lose 5 and fan out 5
is 0 and 0; `max_gained` or a `unique` rule catches the fan out); a filter that
drops rows on purpose needs a share or no row reconcile; a Spark decimal total
past 38 digits fails (ANSI) or is null.

Spark parity for the exact sums is in `tests/integration/test_expectations_spark.py`
(`test_reconcile_sums_are_exact_on_spark_as_on_pandas`), for CI.
