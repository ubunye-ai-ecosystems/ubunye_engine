# F-052: a quarantined row's reasons end in a stray comma on pandas, not on Spark

**Status:** fixed on example/r2-olist
**Severity:** major (the same task writes different rows on two backends)
**Source:** example-author, building R2 (Olist), 2026-09-30
**Promise:** a task gives the same result on pandas and Spark

## What happens
An output with several `quarantine` rules. A row that breaks the first rule and not
the last gets `_ubunye_failed_rules` = `price_between,` on pandas. Spark writes
`price_between`. The quarantine output differs, and so does the run record's digest.
The docs promise `quantity_between,paymentMethod_one_of`.

The existing tests only had a row that broke every rule, which hides it.

## Cause
`ubunye.core.expectations._split` built the reasons with Narwhals'
`concat_str(..., separator=",", ignore_nulls=True)`. On pandas, Narwhals adds the
separator after each present value except the last one given, whether or not a later
value is present. Spark's `concat_ws` skips nulls properly.

## Repro
pandas 2.3.3, narwhals 2.19.0 (numpy or Arrow backed, the same):

```python
import narwhals as nw, pandas as pd
df = nw.from_native(pd.DataFrame({"a": [1, 5, 9], "b": [5, 1, 9]}))
print(df.select(r=nw.concat_str(
    [nw.when(nw.col("a") > 3).then(nw.lit("a_big")),
     nw.when(nw.col("b") > 3).then(nw.lit("b_big"))],
    separator=",", ignore_nulls=True)).to_native()["r"].tolist())
# ['b_big', 'a_big,', 'a_big,b_big']      Spark: ['b_big', 'a_big', 'a_big,b_big']
```

In the Olist example: `order_items_rejected` (rules on `price` and `freight_value`)
held `price_between,` for the free item.

## Fix
Each broken rule gives `,name`, every other rule `""`, joined with no separator, and
the leading comma is cut (`str.slice(1)`). No nulls reach `concat_str`, so both engines
build the same text. Spark's output is unchanged.

Test: `tests/unit/core/test_expectations.py::test_the_reasons_name_only_the_rules_a_row_broke_with_no_stray_comma`
(fails before: `'a_between,' != 'a_between'`; passes after). Spark parity for CI:
`tests/integration/test_expectations_spark.py::test_a_row_that_broke_only_some_rules_has_the_same_reasons_on_both`
(not run on the dev box: no Spark there for this work).

Upstream: worth a Narwhals issue (pandas-like `concat_str` with `ignore_nulls=True`);
not filed, since that would be public.
