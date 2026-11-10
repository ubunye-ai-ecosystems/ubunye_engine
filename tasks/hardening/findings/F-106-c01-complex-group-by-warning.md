# F-106: C01 on pandas warned "Found complex group-by expression"

**Status:** fixed (0.9.1)
**Severity:** minor (a warning on every pandas run of the proving workload, and a slow path)
**Source:** docs audit (0.9.1)
**Promise:** none

## What happened
The C01 transform (`examples/proving/c01_portable_etl`) cast each count and sum inside
`group_by().agg()`. On pandas, Narwhals cannot map an aggregation followed by a cast
to a pandas group-by, so it fell back to a per-group apply and warned
`UserWarning: Found complex group-by expression`. The same shape lost the cast in
F-027.

## Fix
The aggregation is plain; the casts come after it, in one `with_columns`, as
`docs/guides/portable-transforms.md` already advises. No warning is filtered. The
outputs are the same: the C01 golden digest still matches on pandas and Spark.

## Evidence
`tests/unit/proving/test_c01_pandas_warnings.py` fails before (the warning is raised)
and passes after. `tests/integration/test_proving_c01.py` (Spark and pandas, golden
digest) passes.
