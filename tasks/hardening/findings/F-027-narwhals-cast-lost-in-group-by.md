# F-027: a cast inside a group-by was lost on pandas

**Status:** worked around in the example; upstream (Narwhals) not yet reported
**Severity:** major (the same code gave another schema on pandas than on Spark)
**Source:** real-world example R1 (examples/real-world/food_prices_africa), WFP data 2025-2026, pandas-local vs spark-local (2026-09-29)
**Promise:** 1

## What happens
With Narwhals 2.26 and pandas 3 (and pandas 2.2), a `.cast(nw.Int64)` inside a
`group_by().agg()` that also computes a mean is dropped: counts come out as `float64`.
A group-by with only simple aggregations keeps the cast. Spark gives `bigint`, so the
schemas, and the data hashes, differ.

## Repro
```python
df = nw.from_native(pd.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 2], "x": [1.0, 2.0, 3.0]}))
df.group_by("k").agg(n=nw.len().cast(nw.Int64), m=nw.col("x").mean())   # n: float64
```

## Fix
Cast after the aggregation (`docs/guides/portable-transforms.md`). An upstream issue for
Narwhals is drafted; filing it is the owner's decision (public, under their account).
