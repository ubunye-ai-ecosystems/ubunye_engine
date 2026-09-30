# F-069: columns whose names differ only by case run on pandas, fail on Spark

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** major (a task passes on the laptop and fails on the cluster)
**Source:** experiment E-09 (awkward data), shape 10 (column names)
**Promise:** 1 (same result anywhere)

## What happens
A parquet file with columns `Col` and `col`, JSON records with keys `a` and `A`, or a
transform that returns a frame with `Col` and `col`: the E-08 task
(`names_parquet_case_dupes`) succeeded on pandas and wrote both columns.

## Repro
```python
import pyarrow as pa, pyarrow.parquet as pq
from ubunye.backends.pandas_backend import PandasBackend
pq.write_table(pa.table({"Col": [1], "col": [2]}), "c.parquet")
PandasBackend().read_frame("parquet", "c.parquet")   # read, two columns
```

## Expected
Spark's default is `spark.sql.caseSensitive=false`. A file source then refuses a data
schema with names that differ only by case (`DataSource` checks it: "Found duplicate
column(s) in the data schema"), and a write refuses such a frame
(`InsertIntoHadoopFsRelationCommand`: "Found duplicate column(s) when inserting
into"). The task that ran on pandas fails when it moves to Spark.

## Fix
The pandas reader refuses such data (parquet, JSON; CSV headers are renamed by Spark's
rule, F-060) and the writer refuses such a frame, before anything is written, with a
message that says why. Other awkward names (unicode, spaces, dots, reserved words,
tabs) are read and written as before.

## Evidence
Unit: `TestNamesThatDifferOnlyByCase` (3 failed before, 4 pass after). Integration
(CI): `test_names_that_differ_only_by_case_are_refused` (parquet and JSON, both engines
must refuse), `test_a_frame_with_names_that_differ_only_by_case_is_not_written`, and
`test_awkward_column_names_read_the_same` (types, values and rows-v1 digest).
