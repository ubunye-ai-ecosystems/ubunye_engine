# F-066: parquet unsigned integers keep an unsigned type on pandas

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** minor (silent: a different schema and run record hash, same values)
**Source:** experiment E-08 (awkward data), shape 9 (special numbers)
**Promise:** 1 (same result anywhere)

## What happens
A parquet file with unsigned columns (pandas and numpy write them from `uint8` and
friends; so do many IoT and image pipelines) reads on pandas as `uint8`, `uint16`,
`uint32`, `uint64`. The E-08 task wrote them back unsigned, and the run record's
schema hash names `uint64`, a type Spark does not have.

## Repro
```python
import pyarrow as pa, pyarrow.parquet as pq
from ubunye.backends.pandas_backend import PandasBackend
pq.write_table(pa.table({"u": pa.array([2**64 - 1], pa.uint64())}), "u.parquet")
PandasBackend().read_frame("parquet", "u.parquet").native.dtypes   # uint64[pyarrow]
```

## Expected
Spark's `ParquetSchemaConverter` (SPARK-34817, Spark 3.2 onwards): UINT_8 is
`smallint`, UINT_16 `int`, UINT_32 `bigint`, UINT_64 `decimal(20,0)`. The values are
the same; only the type differs.

## Fix
The pandas parquet reader casts unsigned columns (also inside lists, structs and
maps) to those types. Signed and other columns are not touched.

## Evidence
Unit: `TestParquetUnsignedIntegers` (failed before, passes after). Integration (CI):
`test_parquet_unsigned_integers` checks the schema, the values and the rows-v1
fingerprint against Spark.
