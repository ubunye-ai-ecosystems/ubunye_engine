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
be present and valid, write both rules.

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

## Checked when the config loads

`ubunye validate` refuses an expectation that names an output that does not exist,
a quarantine output that does not exist or is the output itself, two outputs
sharing one quarantine output, a rule with no kind or two kinds, a `quarantine`
rule with no `quarantine:` output, and a `matches` pattern that is not a valid
regular expression.
