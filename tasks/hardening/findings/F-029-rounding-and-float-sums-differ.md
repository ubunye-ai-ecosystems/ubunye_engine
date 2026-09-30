# F-029: rounding and float sums differ between pandas and Spark

**Status:** fixed in the example; documented in the portable transforms guide
**Severity:** major (the same code gave other numbers on each engine)
**Source:** real-world example R1 (examples/real-world/food_prices_africa), WFP data 2025-2026, pandas-local vs spark-local (2026-09-29)
**Promise:** 1

## What happens
1. `.round(4)` rounds a half up on Spark and to even on pandas: a mean of 921.03125 was
   921.0313 on Spark and 921.0312 on pandas; 43 of 6,366 monthly prices and 73 USD
   prices differed by 0.0001.
2. With half-up rounding by hand, 7 prices and 27 USD prices still differed: a mean of
   floats depends on the order the engine adds in (pandas pairwise, Spark by
   partition), and last-bit differences moved values across a rounding boundary.

## Fix
Round half up by hand (`floor(x * 10^4 + 0.5) / 10^4`, needs Narwhals 2.9 for
`Expr.floor`), and make means from exact integer sums (prices in whole millionths,
summed as int64, divided once). After both, the monthly table and alerts are identical:
digest e580f7f51483 on the full 2025-2026 data, 021cc19ca2b6 on the committed sample.

## Evidence
tests/integration/test_real_world_food_prices.py (Spark and pandas, golden digests).
