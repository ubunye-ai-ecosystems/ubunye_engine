# F-065: JSON records with no fields are silently lost on pandas

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** major (silent row loss)
**Source:** experiment E-09 (awkward data), shape 3 (nested JSON) and 8 (empty inputs)
**Promise:** 5 (nothing is lost silently)

## What happens
A JSON file of records with no fields (`{}`, or only fields Spark drops: an empty
name, an empty object) reads as 0 rows on pandas. The E-08 task
(`nested_empty_records`: three `{}` lines, a transform that adds a column) finished
with status success and wrote 0 rows; the run record said the input had 0 rows.

## Repro
```python
from ubunye.backends.pandas_backend import PandasBackend
open("e.json", "w").write("{}\n{}\n{}\n")
PandasBackend().read_frame("json", "e.json").count()   # 0
```

## Expected
Spark reads one row per record with an empty schema (`JsonInferSchema` gives
`StructType(Nil)`, `JacksonParser` a row with no fields): 3 rows, no columns. A
transform that adds a column then has 3 rows.

## Fix
The reader returns a table with the rows and no columns (`no_columns`, which the
writer already used for the same reason) when the records have no fields left.

## Evidence
Unit: `TestJsonRecordsWithNoFields` (2 failed before, 2 pass after). Integration
(CI): `test_json_records_with_no_fields_are_rows`. E-08 rerun: `nested_empty_records`
writes 3 rows.
