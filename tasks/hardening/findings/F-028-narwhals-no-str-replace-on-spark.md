# F-028: `str.replace` does not exist on Spark in Narwhals

**Status:** worked around (the guide says so)
**Severity:** major (code that runs on pandas does not run on Spark)
**Source:** real-world example R1 (examples/real-world/food_prices_africa), WFP data 2025-2026, pandas-local vs spark-local (2026-09-29)
**Promise:** 1

## What happens
`nw.col("u").str.replace(" KG", "")` raises `NotImplementedError: 'replace' is not
implemented for: 'SparkLikeExprStringNamespace'` on Spark; it works on pandas.

## Fix
`str.replace_all(" KG", "", literal=True)` works on both (checked on live Spark 4.2).
