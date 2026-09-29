# F-018: a source column that changes type can produce silently wrong output

**Status:** fixed on fix/f017-f018-contracts (input contracts)
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

## Fix
Input contracts, inside the existing expectations. `CONFIG.expectations` may name an
input; its rules run right after it is read and before the transform. One new rule,
`columns`, checks the shape:

```yaml
expectations:
  orders:                              # an input
    rules:
      - columns: {order_id: int64, qty: int64, price: float64}
        extra: allow                   # default; forbid refuses undeclared columns
```

Decisions:

- **Type names are the run record's** (ADR 006), read by the same code that
  names them for the hash: `spark_kind` on Spark; on pandas the Arrow type with
  the same steps `pandas_io.to_arrow` takes before writing (a category is its
  values, an all-null column is `string`, every timestamp is `timestamp`). A unit
  test holds `frame_kinds` equal to the names of the table pandas would write, and
  an integration test holds Spark and pandas equal on one parquet file of every
  simple type, a decimal, a list and a struct. The schema alone is read: no row.
- **Strict, no widening.** `int32` is not `int64`, `float32` is not `float64`.
  The pandas reader already gives Spark's types, so the same source gives the same
  names on both engines, and a changed width is a changed source: it changes the
  written files' schema and the record's schema hash, which `ubunye gate` already
  flags. A contract that passed where the gate fails would be two answers to one
  question. To accept several types, list them (`qty: [int32, int64]`): explicit,
  and no policy to learn. E-05's "qty int to float" is caught by this.
- **Nulls are not part of the type.** An `int64` column with gaps is `int64`
  (pandas reads Arrow backed columns, so a gap does not turn it into `float64`).
- **Unknown type names are refused at load**, with the record's name for the common
  other ones (`double` -> `float64`, `long`/`bigint` -> `int64`, `int` -> `int32`,
  `boolean` -> `bool`), and a close match otherwise.
- **A broken `fail` columns rule stops the other rules on that frame**: a `between`
  on a column that turned to text would only add an engine error.
- **No quarantine on an input (yet)**, and no `reconcile` on one (it goes on the
  output). Both are config errors. A name that is both an input and an output
  cannot have expectations: config error, rename one.
- Results carry `side: input` in the record (left out for outputs, so old records
  and tests are unchanged). `lineage trace` prints `input orders.columns`, and
  `gate` reports it like any expectation.
- The notebook checks the contract in `read()`, and its record keeps the results.
- Any rule may go on an input; beyond `columns` they cost one pass over it (a
  second read of the source on Spark; inputs are not held, ADR 009).

Evidence, `tests/experiments/e03_e05_rows_and_schema.py` on the pandas backend:

| E-05 case | before (old code, no contract) | after (`--contracts`) |
|---|---|---|
| column added | exit 0, wrote | exit 0, wrote (extra: allow) |
| column dropped (qty) | exit 1, `KeyError: 'qty'` from the transform | exit 1 before the transform: `qty: expected int64, missing` |
| column renamed | exit 1, `KeyError: 'price'` | exit 1: `price: expected float64, missing` |
| price to text | **exit 0, wrote "11.011.0"** | exit 1: `price: expected float64, found string`, transform not run |
| qty int to float | exit 0, wrote | exit 1: `qty: expected int64, found float64` |

Tests: `tests/unit/core/test_expectations_inputs.py` (38; 37 fail on the old code,
the one that passes checks that a bad `extra` value is refused). Spark, in
`tests/integration/test_expectations_spark.py` (run by CI on Spark 4 and 3.5, not
on the dev box): the same names for one parquet file on both engines, the same
verdict for a contract on both, and a whole Spark task with `price` as text that
stops before the transform and writes nothing.

Open: a parquet timestamp written without a zone (pandas `datetime64` straight to
parquet) may be `timestamp_ntz` on Spark 3.4+ and is `timestamp` on the pandas
backend, which reads every timestamp as an instant. That is the record's naming
today, not new here; a contract naming it would differ between engines.
