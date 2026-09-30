# F-053: no portable way to count days between two dates

**Status:** worked around in the Olist example and documented (portable transforms
guide); the causes are in Narwhals and pandas, not in the engine
**Severity:** minor (costs a newcomer time; no wrong answer, a crash)
**Source:** example-author, building R2 (Olist), 2026-09-30
**Promise:** one transform runs on every engine (ADR 005)

## What happens
"Delivery days" (delivered minus purchased) is the first thing a data team computes
from Olist. Every direct way fails on one engine:

| Way | pandas (2.3.3, Arrow backed) | Spark (Narwhals 2.19) |
|---|---|---|
| `(a - b).dt.total_seconds()` (Narwhals has no `total_days`) | works | `total_*` not implemented |
| `dt.timestamp("ms")` difference | works | not implemented |
| `dt.ordinal_day()` with year arithmetic | **TypeError** when the column has a null | works |
| `% 12` on an integer column | **NotImplementedError: mod** (Arrow integers, pandas 2) | works |

A delivery date is null for every order not yet delivered, so `ordinal_day` crashes
the run on pandas.

## Repro
```python
import narwhals as nw, pandas as pd, pyarrow as pa
t = pa.array([pd.Timestamp("2018-03-01", tz="UTC"), None], type=pa.timestamp("us", tz="UTC"))
df = pd.DataFrame({"t": pd.array(t, dtype=pd.ArrowDtype(t.type))})
nw.from_native(df).select(nw.col("t").dt.ordinal_day())
# TypeError: 'float' object cannot be interpreted as an integer
# (the same with a numpy datetime column holding NaT)
```

## Workaround
A day number from year, month and day with `//`, `+`, `-`, `*` and `when` only
(`day_number` in `examples/real-world/olist_ecommerce/.../orders_fact/transformations.py`),
now in [the portable transforms guide](../../../docs/guides/portable-transforms.md).
The Olist example's golden digest checks it on pandas; the integration test checks
Spark gives the same rows (CI).

## Not done
No engine change: a date helper in the core would be a new feature, and the core stays
small. Worth Narwhals issues (pandas-like `ordinal_day` with nulls; `total_seconds` on
Spark); not filed, since that would be public. The Spark column of this table is read
from the Narwhals 2.19 source (`not_implemented()` in `_spark_like/expr_dt.py`), not
run: this work had no Spark on the dev box.
