# F-068: an empty file or folder read with a schema is refused on pandas

**Status:** fixed on hardening/awkward-data (2026-09-30) for inputs with a schema;
without a schema left open (see below)
**Severity:** minor (a run Spark finishes with zero rows stops on pandas)
**Source:** experiment E-09 (awkward data), shape 8 (empty inputs)
**Promise:** 1 (same result anywhere)

## What happens
A daily drop that arrives empty (a zero byte `orders.csv`, or a folder with only
`_SUCCESS`), read with an explicit `schema`:

```
SourceReadError: Path does not exist or holds no data files: .../zero.csv
```

The message is also wrong: the path exists.

## Expected
With a user schema Spark infers nothing; the scan of an empty file or of a folder
with no data files yields no rows. So zero rows with the schema's columns and types.
A path that does not exist is still an error on both.

## Fix
When the path exists but holds no data and a schema is given, the pandas backend
returns zero rows of that schema (timestamps as instants, as for any read).

## Not fixed: no schema
Without a schema, Spark's CSV and JSON sources infer from a file list that is not
empty, find no first line, and (by the source: `TextInputCSVDataSource.inferFromDataset`,
`JsonInferSchema.infer`) give an empty schema: zero rows and no columns. The pandas
backend refuses the read instead. Refusing is arguably the better answer for a
pipeline (an empty drop with no schema is usually a mistake), so this is a design
question for the lead, not changed here. `test_empty_file_without_a_schema` is marked
`xfail(strict=False)`: CI shows what Spark really does. An empty parquet file with no
schema is an error on both (it has no footer).

## Evidence
Unit: `TestEmptyInputsWithASchema` (4 failed before, 5 pass after). Integration
(CI): `test_empty_file_with_a_schema` (csv, json, parquet),
`test_empty_folder_with_a_schema`, `test_zero_rows_with_a_schema_and_a_header_only_csv`.
