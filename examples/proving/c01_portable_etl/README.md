# C01: portable tabular ETL

The proving ground's first workload. One transform, written once with Narwhals, runs
unchanged on pandas, on local Spark and on the clouds, and the run records say whether
the results were the same.

**It answers:** can the exact same transformation run on pandas and Spark, and give the
same rows?

## What it computes

From `data/orders.csv` (12 orders) and `data/customers.csv` (6 customers):

- `order_lines`: the paid orders, joined to their customer, with `amount_cents` and the
  UTC day of the order (10 rows);
- `country_summary`: per country, orders, distinct customers, revenue and the first and
  last order (5 rows).

## What it protects against

| Case in the data | The divergence it catches |
|---|---|
| an order with no customer, one with an unknown customer, a customer with no country | null join keys and a **null group key**: Spark keeps one null group, plain pandas drops it |
| a missing quantity | arithmetic with a null gives a null, not 0 |
| cancelled and refunded orders | a text filter |
| timestamps at 23:59:59 and 00:00:00 UTC | cutting time into days: found **F-021**, days depended on the machine's zone (ADR 007) |
| names with accents | text survives both engines |
| money as integer cents | on purpose: a float sum depends on the order of addition, which engines differ in |

## Run it and prove it

See [Tutorial 1](../../../docs/tutorials/01-local-parity.md). In short, from this folder:

```bash
ubunye run -d pipelines -u proving -p c01 -t etl --backend pandas --lineage --var out_dir=output/pandas
ubunye prove observe --workload c01-portable-etl --env pandas-local -d pipelines -u proving -p c01 -t etl -o evidence
ubunye run -d pipelines -u proving -p c01 -t etl --backend spark --lineage --var out_dir=output/spark
ubunye prove observe --workload c01-portable-etl --env spark-local -d pipelines -u proving -p c01 -t etl -o evidence
ubunye prove report evidence --workload c01-portable-etl --reference spark-local
```

Tested: `tests/integration/test_proving_c01.py`, golden digest `bb08a7d7a9fd`; CI runs it
on Linux with Spark 3.5 and Spark 4; first measured on Windows with Spark 4.2 and pandas 3.
