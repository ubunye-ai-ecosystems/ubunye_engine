# F-036: expectations on a Spark output needed pyarrow, which the Spark images do not have

**Status:** fixed on feat/prove-r1 (2026-09-29), not yet merged
**Severity:** major
**Source:** proving R1 on GCP Dataproc and Azure Container Apps (ubunye-infra prove-r1,
run 36576881079)
**Promise:** a task that runs on local Spark runs unchanged in the images `ubunye deploy
dockerfile` writes

## What happens
R1's `clean` checks `CONFIG.expectations` (not null, price above 0). The checks count
broken rows with Narwhals, and on a Spark frame Narwhals collects the one row of
counts through Arrow. The Dataproc image (`deploy dockerfile dataproc`) and the
container image (`deploy dockerfile container`) install the engine without pyarrow, so
`clean` read, transformed, then died:

```
narwhals/_spark_like/dataframe.py, in _collect
    from narwhals._arrow.dataframe import ArrowDataFrame
ModuleNotFoundError: No module named 'pyarrow'
```

C01 has no expectations, so it never reached this. Glue and Databricks ship pyarrow, so
R1 passed there.

## Repro
`tests/integration/test_expectations_spark.py::test_rules_are_counted_on_spark_without_pyarrow`
blocks pyarrow after the frames exist and checks the same rules as on pandas. On the
old code: `ModuleNotFoundError: import of narwhals._arrow.dataframe halted`.

## Fix
`expectations._scalars` takes the one row of counts from Spark itself
(`DataFrame.collect()`, a `Row`) when the frame is a Spark frame; other backends are
unchanged. The counts are the same numbers, so no verdict or hash moves. Checked: R1
through the deploy entry script on local Spark 4.2 with pyarrow blocked gives the
golden digests (clean f61e0f0544f5, monitor 021cc19ca2b6).
