# F-067: CSV inferSchema on pandas is pyarrow's, not Spark's

**Status:** fixed on hardening/awkward-data (2026-09-30), numbers, booleans and the
naive plus aware timestamp mix; the looser date and time forms are F-077
**Severity:** major (silent: a whole number past 64 bits loses digits)
**Source:** experiment E-09 (awkward data), shapes 4 (messy CSV) and 9 (big numbers)
**Promise:** 1 (same result anywhere), 5 (nothing lost silently)

## What happens
The pandas reader let pyarrow infer each file's types. On the E-08 files:

| Column values | pandas before | Spark (`CSVInferSchema`) |
|---|---|---|
| `9223372036854775807`, `9223372036854775808` | `double` (`9.223372036854776e18` twice: digits lost) | `decimal(20,0)`, exact |
| `12345678901234567890123` | `double` | `decimal(23,0)` |
| `1, 2` (space after the comma) | `int` | `double` (`parseInt` refuses the space, `parseDouble` trims it) |
| `+5` | `double` | `int` |
| `1.5d`, `2f` | text | `double` (Java's type suffix) |
| `Inf`, `inf` | `double` | text (`inf` is not a Java double; `Inf` alone would be) |
| `0x1F` | `int` 31 | text |
| `tRuE` | text | `boolean` |
| `2024-01-02 03:04:05` and `...+02:00` in one column | text | `timestamp` |

Each file was also inferred on its own and the tables merged, where Spark infers
once over every file.

## Repro
```python
from ubunye.backends.pandas_backend import PandasBackend
open("n.csv", "w").write("big\n9223372036854775807\n9223372036854775808\n")
PandasBackend().read_frame("csv", "n.csv", options={"header": "true", "inferSchema": "true"}).native
```

## Expected
Spark's `CSVInferSchema` (3.5 and 4): per value `tryParseInteger` (`Integer.parseInt`),
`tryParseLong`, `tryParseDecimal` (`new BigDecimal`, scale 0 only, precision 38 at
most), `tryParseDouble` (`Double.parseDouble`, or `NaN` / `Inf` / `-Inf`), date,
timestamp, `tryParseBoolean` (case blind), text; merged with `compatibleType` (a long
and a `decimal(19,0)` give `decimal(20,0)`, a decimal and a double give double, a
date and a timestamp give timestamp). `UnivocityParser` then reads each value the
same way (`datum.toDouble` trims blanks and reads the suffix).

## Fix
`ubunye/adapters/csv_infer.py`. The CSV is read as text; each column is typed over
all files at once. Arrow's own casts decide the common columns (a cast is tried only
when the first value could pass it, since a failing Arrow cast reads the whole
column); Java's forms are checked with RE2 patterns on Arrow arrays. No Python loop
over values except for hex doubles.

Speed (dev box, best of 3): 300 MB, 5,000,000 rows, 6 columns: 2.59 s (1.98 s
before); 1,500 columns by 2,000 rows: 1.23 s (0.92 s). Performance guard
`csv_read_plain_60k`: 48.3 ms against 40.0 ms (+21%, inside its 30%).

## Evidence
Unit: `TestCsvInferSchemaNumbers` (6 failed before, 7 pass after); the existing
`test_pandas_reads_like_spark` inference tests still pass. Integration (CI):
`test_csv_infer_schema_numbers` (8 cases) and `test_csv_infer_schema_over_many_files`.
