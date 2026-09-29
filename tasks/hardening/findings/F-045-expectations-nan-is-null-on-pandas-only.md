# F-045: an expectation counts NaN as null on pandas but not on Spark

**Status:** fixed on hardening/real-world (2026-09-29)
**Severity:** major (the same task gives a different verdict on two backends)
**Source:** skeptic review of feat/prove-r1 (F-036), reproduced by the example author
**Promise:** a task gives the same result on pandas and Spark
**Number:** F-045 on purpose. F-033 is on hardening/real-world and F-034 to F-037 on
feat/prove-r1; F-038 onwards may be taken by the E-06 scale work (exp/e06-scale, not
on the remote yet), so this skips to a number clearly free.

## What happens
A float column holding one NaN and one null. `not_null` and `between` count them
differently on the two backends:

| Rule on `qty` = [1.0, NaN, 5.0, 2.0, null] | pandas | Spark |
|---|---|---|
| `not_null: qty` broken by | 2 | 1 |
| `between: {column: qty, min: 1, max: 4}` broken by | 1 | 2 |

pandas stores a missing float as NaN, so Narwhals' `is_null` on pandas sees NaN and
null alike. Spark keeps NaN as a value that is not null, and NaN compares as greater
than every number, so it breaks `between`. A `fail` rule can then stop a run on one
backend and pass it on the other; a `quarantine` rule quarantines different rows.

## Repro
Local Spark 4.2, pandas 2.3, narwhals 2.19, engine feat/prove-r1 at 8a40672:

```python
import pandas as pd
from pyspark.sql import SparkSession
from ubunye.config.schema import ExpectationSet
from ubunye.core import expectations
rows = [(1, 1.0), (2, float("nan")), (3, 5.0), (3, 2.0), (None, None)]
spec = ExpectationSet(rules=[{"not_null": "qty", "severity": "warn"},
    {"between": {"column": "qty", "min": 1, "max": 4}, "severity": "warn"}])
spark = SparkSession.builder.master("local[1]").getOrCreate()
for name, frame in (("pandas", pd.DataFrame(rows, columns=["id", "qty"])),
                    ("spark", spark.createDataFrame(rows, "id INT, qty DOUBLE"))):
    print(name, [(r.rule, r.failed) for r in expectations.check_output("out", frame, spec)[2]])
# pandas [('qty_not_null', 2), ('qty_between', 1)]
# spark  [('qty_not_null', 1), ('qty_between', 2)]
```

## Not fixed here
One rule must be chosen and pinned as a parity case (for example "NaN counts as
missing" on both, checking `is_null | is_nan` for float columns), and the choice
documented next to the rows-v1 rule that already tells null from NaN.

## Decision and fix
NaN counts as missing on every backend. Following Spark (NaN is a value) was not
possible: pandas stores a missing float as NaN, so a missing value would pass
`not_null` on pandas and fail it on Spark. For float columns `not_null` checks
`is_null | is_nan`; `between` and `one_of` rule the missing out. Tests: the unit
tier on numpy and Arrow floats (Arrow failed before), and live Spark 4.2 (failed
before: between 2, one_of 2; now 1 and 1, the same as pandas).
