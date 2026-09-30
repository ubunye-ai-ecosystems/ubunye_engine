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

## Skeptic review

The skeptic's probes (`p1_config.py`, `p2_validate_cli.py`, `p5_types.py`,
`p7_notebook.py`) found four faults. All fixed in the third commit on the branch,
each with a unit test that fails on 813d515.

| # | Fault | Before | After |
|---|---|---|---|
| 3 | `columns: {qty: 5}` crashed `ubunye validate` with a TypeError | `TypeError: 'int' object is not iterable` | "'columns.qty': give a type name like int64, or a list of them, not 5" |
| 4 | Timestamps: the contract used the record's names, which call every pandas timestamp an instant | pandas: naive and UTC columns both `timestamp`, so a source switching between them passed; a Spark 3.4+ naive parquet column is `timestamp_ntz`, so one contract could not fit both engines | named by what they are, top level and nested, on pandas as on Spark: `timestamp` with a zone, `timestamp_ntz` without (`p5`: `ts_naive: timestamp_ntz`, `lst_ts: list<timestamp_ntz>`, `ts_utc: timestamp`; "naive vs utc" now fails as it should) |
| 5 | Nested names were not checked, and some kinds a frame reports could not be declared | `list<banana>` accepted; `uint8`, `time64[us]`, a null column, a mixed column: undeclarable, so `extra: forbid` could never pass | nested names are parsed part by part (`list<banana>`: "'banana' is not a type name"); `uint8` to `uint64`, `null`, `mixed`, `time32/64[unit]`, `duration[unit]`, `fixed_size_binary[n]` can be declared |
| 6 | The notebook never checked a contract on frames passed to `transform()` | `transform({"orders": frame with price as text})` then `write()`: the error was about reconcile inputs, the contract was never run | the contract runs on those frames when `transform()` starts: "orders: columns (columns): price: expected float64, found string" |

**The timestamp decision (the lead's).** Contracts name a column by its real
type. **The run record is unchanged**: it still writes every pandas timestamp as
an instant and names it `timestamp`, so no schema hash moves
(`test_the_run_record_still_names_every_pandas_timestamp_an_instant`). The docs
say plainly that the two names differ for a naive pandas timestamp. Spark 3.5 and
4 read a parquet timestamp written without a zone as `TIMESTAMP_NTZ`
(`spark.sql.parquet.inferTimestampNTZ.enabled`, on by default since 3.4); the
integration tier checks both ways for CI on 3.5 and 4
(`test_a_parquet_file_with_naive_and_zoned_timestamps_is_named_alike`, and
`TIMESTAMP_NTZ` columns added to the typed parquet test). CSV and JSON timestamps
are instants on both engines (`timestamp`).

Also: with `inferSchema`, a CSV whole number column is `int32` when it fits, on
both engines; the docs say to declare `[int32, int64]` for one that may grow.
OpenLineage now carries an input contract's results on the input dataset
(`dataQualityAssertions`), not on the outputs.
