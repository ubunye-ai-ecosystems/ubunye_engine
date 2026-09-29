# Expectations

`CONFIG.expectations` states what an output must look like. The engine checks
every output after the transform and **before anything is written**, on Spark
and on pandas alike.

```yaml
CONFIG:
  inputs:
    transactions: {format: s3, path: "data/transactions.csv", file_format: csv}
  outputs:
    clean_transactions: {format: s3, path: "out/clean", file_format: parquet}
    quarantined_transactions: {format: s3, path: "out/quarantine", file_format: parquet}

  expectations:
    clean_transactions:                      # the output being checked
      quarantine: quarantined_transactions   # where bad rows go
      max_quarantine_rate: 0.05              # more than 5% bad: stop the run
      rules:
        - not_null: transactionID            # severity fail is the default
        - unique: transactionID
        - between: {column: quantity, min: 1}
          severity: quarantine
        - one_of: {column: paymentMethod, values: [visa, mastercard, amex]}
          severity: quarantine
        - matches: {column: cardNumber, pattern: '^\d{4}$'}
          severity: warn
        - row_count: {min: 1}
```

## The rules

| Rule | Passes when | Per row? |
|---|---|---|
| `not_null: col` | the value is present | yes |
| `between: {column, min, max}` | min <= value <= max (either bound may be left out) | yes |
| `one_of: {column, values}` | the value is in the list | yes |
| `matches: {column, pattern}` | the value matches the regular expression | yes |
| `unique: col` or `[col, ...]` | no two rows share the value(s) | no |
| `row_count: {min, max}` | the number of rows is in range | no |

A missing value passes every rule except `not_null`, as in SQL. When a value must
be present and valid, write both rules. In a float column NaN counts as missing, like
null, on every backend: pandas stores a missing float as NaN and cannot tell the two
apart, so this is the one rule both backends can keep. `not_null` breaks on NaN;
`between` and `one_of` let it pass.

Each rule is named after its column and kind (`quantity_between`), or give it a
`name:`. Names must be unique within an output.

## What a broken rule does

| `severity` | Effect |
|---|---|
| `fail` (default) | The run stops. **Nothing is written**, for any output. |
| `quarantine` | The breaking rows move to the `quarantine:` output, with a `_ubunye_failed_rules` column naming every rule each row broke (`quantity_between,paymentMethod_one_of`). The rest are written. Only per-row rules can quarantine. |
| `warn` | The rows stay; the breach is logged. |

The quarantine output is always written, empty when no row broke a rule, so a
downstream job can rely on it. The transform must not return it: the engine fills
it.

`max_quarantine_rate` turns "a few bad rows" into "the source has changed": if a
larger share of rows is quarantined, the run fails and nothing is written.

## When it fails

```text
ExpectationError: Expectations failed, so nothing was written:
  clean_transactions: transactionID_not_null (not_null) broken by 1 of 891 rows
  Hint: Fix the data or the source, or change the rule's severity to quarantine
  or warn if this is expected.
```

The error carries every rule's result, passed or not (`err.results`). A rule that
breaks zero rows today and thousands tomorrow is the earliest warning that
something upstream moved.

## Reconcile: nothing lost on the way

A join can drop rows and the run still succeeds. `reconcile` compares an output
with an input the transform received, before anything is written:

```yaml
  expectations:
    enriched:
      reconcile:
        - input: orders
          rows: {max_lost: 0}                    # every order reaches enriched
          sum: {column: amount, tolerance: 0.01} # and so does its amount
```

| Key | Meaning |
|---|---|
| `input` | an input in `CONFIG.inputs` |
| `rows: {max_lost, max_gained}` | how many rows may be lost, or gained (a join that fans out). A number of rows, or a share of the input: `"1%"`. A bound left out is not checked. |
| `sum: {column, input_column, tolerance}` | the column's sum must match the input's. `input_column` is the input's name for it, if different. `tolerance` is a number, or a share of the input's sum (`"0.1%"`); default 0. |
| `severity` | `fail` (default) or `warn` |

Each check is reported like a rule, named `rows_from_orders` and
`amount_sum_from_orders`, with what was found:

```text
ExpectationError: Expectations failed, so nothing was written:
  enriched: rows_from_orders (reconcile): 1000 rows read from orders, 900 reached
  enriched: 100 lost, 0 gained (at most 0 lost)
```

- **Quarantined rows count as carried over.** They were set aside with a reason,
  not lost. The output is counted before any row moves to its quarantine output.
- **A sum leaves out missing values** (null, and NaN in a float column), on both
  sides, as the rules do. Float sums can differ in the last digits between runs
  and engines; give a float sum a small tolerance.
- **Cost:** one pass over each reconciled input, for its row count and sums, however
  many outputs reconcile with it. On pandas that is in memory. On Spark it reads the
  input again: inputs are not held (ADR 009), so a source that changes between the
  read and the count is counted as it is then. The output side is counted in the
  same pass as the other rules.

## Checked when the config loads

`ubunye validate` refuses an expectation that names an output that does not exist,
a quarantine output that does not exist or is the output itself, two outputs
sharing one quarantine output, a rule with no kind or two kinds, a `quarantine`
rule with no `quarantine:` output, a `matches` pattern that is not a valid
regular expression, and a `reconcile` that names no real input, checks nothing, or
has a bound that is not a number or a percentage.
