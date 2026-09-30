# Tutorial 2: nine messy tables to a fact table, with checks that catch the mess

You will build what a data team builds first on an online shop's data: one row per
order, then monthly numbers per seller. The data is shaped like the Olist Brazilian
e-commerce set: nine CSV files, each with its own problems. On the way you will see
four Ubunye features stop four kinds of bad data, and then break the pipeline on
purpose to watch them work.

About 20 minutes. No cloud, no account, no Java (Spark is an optional last step).

## You need

- Python 3.10 to 3.13.
- The Ubunye repository (the example lives in it):

```bash
git clone https://github.com/ubunye-ai-ecosystems/ubunye_engine
cd ubunye_engine/examples/real-world/olist_ecommerce
python -m venv .venv
. .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -e "../../..[pandas]" "narwhals>=2.9"
```

The last line installs Ubunye from the repository you just cloned (`../../..` is its
top folder). **This example needs a newer Ubunye than 0.7.1, the latest release on
PyPI**: 0.7.1 has no `columns` input contracts and none of the fixes this example
led to (F-052, F-054, F-055), so `pip install ubunye-engine` would fail at step 2.
Once the next release is out, `pip install "ubunye-engine[pandas]" "narwhals>=2.9"`
will do.

Every command below runs from this folder.

## 1. Look at the data

```bash
ls sample
```

Nine files: orders, order items, payments, reviews, customers, sellers, products,
geolocation (points per zip code) and a table that translates category names from
Portuguese to English. They are a small **made up** sample (60 orders) with the same
file names, columns and formats as the real Olist files, so the same pipeline reads
both.

The pipeline has three steps, each a folder under `pipelines/olist/sales/` with a
`config.yaml` (what to read, write and check) and a `transformations.py` (the logic):

| Step | Reads | Writes |
|---|---|---|
| `clean` | the nine raw files | typed tables, and the bad rows set aside |
| `orders_fact` | what `clean` wrote | one row per order |
| `monthly` | what the first two wrote | sales per seller and month, and per category and month |

## 2. Clean the raw files

```bash
ubunye run -d pipelines -u olist -p sales -t clean --backend pandas --lineage
```

Expected: `[OK] Run complete for clean`. A lot was checked on the way. See what was
set aside:

```bash
python -c "import pandas as pd; print(pd.read_parquet('output/order_items_rejected')[['order_id', 'price', '_ubunye_failed_rules']])"
python -c "import pandas as pd; print(pd.read_parquet('output/payments_rejected')[['payment_type', 'payment_value', '_ubunye_failed_rules']])"
python -c "import pandas as pd; print(pd.read_parquet('output/reviews_rejected')[['review_score', '_ubunye_failed_rules']])"
```

Three bad rows, one of each kind: an item priced 0.00, a payment of type `pix` that the
rules do not know, a review scored 7 (scores go from 1 to 5). Each carries the rule it
broke. This is **quarantine**: the bad rows are kept and labelled, not dropped and
not mixed in with the good ones. In `pipelines/olist/sales/clean/config.yaml`:

```yaml
    order_items:
      quarantine: order_items_rejected   # where bad rows go
      max_quarantine_rate: 0.05          # more than 5% bad: stop, the source has changed
      rules:
        - between: {column: price, min: 0.01}
          severity: quarantine
```

Two more features ran before any of this. **Input contracts** checked each raw file's
columns and types right after it was read, before the transform ran:

```yaml
    raw_orders:
      rules:
        - columns:
            order_id: string
            order_purchase_timestamp: timestamp
            ...
```

And a **reconcile** checked that no row went missing between the raw file and the
output. Set aside counts as arrived: it has a reason, it is not lost.

## 3. Read the run record

`--lineage` kept a record of the run: every input and output, how many rows, a hash of
every row, and every check with its result.

```bash
ubunye lineage trace -d pipelines -u olist -p sales -t clean
```

At the end, under `EXPECTATIONS`, every check is listed. For example:

```text
    quarantine order_items.price_between                1/80
    ok         order_items.freight_value_between        0/80
    ok         order_items.rows_from_raw_items          0/80
               80 rows read from raw_items, 80 reached order_items: 0 lost, 0 gained (at most 0 lost, at most 0 gained)
```

## 4. Build the fact table and the monthly numbers

```bash
ubunye run -d pipelines -u olist -p sales -t orders_fact --backend pandas --lineage
ubunye run -d pipelines -u olist -p sales -t monthly     --backend pandas --lineage
```

`orders_fact` warns, and goes on:

```text
expectation warning: orders_fact: payment_gap_between (between) broken by 4 of 60 rows
```

Four orders were paid more or less than their items and freight, by over one real.
Look at them:

```bash
python -c "import pandas as pd; f = pd.read_parquet('output/orders_fact'); print(f[f.payment_gap.abs() > 1][['order_status', 'items_price', 'freight', 'payments_total', 'payment_gap']])"
```

Each has a reason: a canceled order that was paid; instalment interest (paid 3.20
more); an order whose free item was set aside in step 2, so its freight is missing;
and the order whose `pix` payment was set aside, so it looks unpaid. A **warning** is
for things worth knowing but not worth stopping for. The count is in the run record.

One order is not in the table at all:

```bash
python -c "import pandas as pd; print(pd.read_parquet('output/orders_fact_rejected')[['order_id', 'delivery_days', '_ubunye_failed_rules']])"
```

It was delivered 3 days before it was bought: the dates are wrong, so it waits here
for someone to fix them. And the **reconcile** on the fact table still passed:

```bash
ubunye lineage trace -d pipelines -u olist -p sales -t orders_fact
```

```text
    ok         orders_fact.rows_from_orders             0/60
               60 rows read from orders, 60 reached orders_fact: 0 lost, 0 gained (at most 0 lost, at most 0 gained)
    ok         orders_fact.payments_total_sum_from_payments 0/1
               sum of payment_value in payments 7411.88, of payments_total in orders_fact 7411.88: difference 0 (at most 0.01)
```

Every order arrived once (59 in the table, 1 set aside), and every real paid is in
the table. (The item price sum shows a difference like `1.8e-12`: that is how floating
point numbers add up, and why the config allows 0.01.)

Finally, the monthly table:

```bash
python -c "import pandas as pd; print(pd.read_parquet('output/seller_monthly').sort_values(['seller_id', 'purchase_month']))"
```

## 5. Break it: a source file changes

A colleague opens the orders file in a spreadsheet and saves it; one purchase time
comes back as `18/02/2018 10:59`. Make that copy in `data/` (git ignores that folder):

```bash
python -c "import shutil, pathlib; shutil.copytree('sample', 'data', dirs_exist_ok=True); p = pathlib.Path('data/olist_orders_dataset.csv'); p.write_text(p.read_text().replace('2018-02-18 10:59:08', '18/02/2018 10:59', 1))"
ubunye run -d pipelines -u olist -p sales -t clean --backend pandas --var data_dir=data --var out_dir=output-bad
```

Expected:

```text
[ERROR] Run stopped for clean: An input broke its expectations, so the transform did not run and nothing was written:
  raw_orders: columns (columns): order_purchase_timestamp: expected timestamp, found string
```

One bad value turned the whole column into text. Without the contract, the pipeline
would have run on, and every date calculation on that column would have failed later,
or worse, given wrong numbers. The contract stopped it at the door, named the column,
and wrote nothing: `output-bad` does not exist.

## 6. Break it: a join loses orders

The most common silent bug in a fact table: an inner join where a left join was
needed. Open `pipelines/olist/sales/orders_fact/transformations.py` and change this
line:

```python
            .join(per_order_items, on="order_id", how="left")
```

to

```python
            .join(per_order_items, on="order_id", how="inner")
```

Save, and run the step again:

```bash
ubunye run -d pipelines -u olist -p sales -t orders_fact --backend pandas --lineage
```

Expected:

```text
[ERROR] Run stopped for orders_fact: Expectations failed, so nothing was written:
  orders_fact: rows_from_orders (reconcile): 60 rows read from orders, 59 reached orders_fact: 1 lost, 0 gained (at most 0 lost, at most 0 gained)
  orders_fact: payments_total_sum_from_payments (reconcile): sum of payment_value in payments 7411.88, of payments_total in orders_fact 7365.98: difference -45.900000000000546 (at most 0.01)
```

The canceled order has no items, so the inner join dropped it, and its 45.90 payment
with it. Both reconciles saw it; the run wrote nothing, so yesterday's good table is
still there. The run record keeps why:

```bash
ubunye lineage trace -d pipelines -u olist -p sales -t orders_fact
```

```text
Run:     ...  [error]  ...
Error:   ExpectationError: Expectations failed, so nothing was written: ...
```

Change `inner` back to `left` before going on.

## 7. Optional: the same answer on Spark

Every step is written once with Narwhals, so it runs on Spark unchanged. With Java 17
or 21 installed (Windows also needs `HADOOP_HOME` with `winutils.exe`):

```bash
pip install -e "../../..[pandas,spark]"
for t in clean orders_fact monthly; do
  ubunye run -d pipelines -u olist -p sales -t $t --backend pandas --lineage --var out_dir=output/pandas
  ubunye prove observe --workload r2-olist-$t --env pandas-local -d pipelines -u olist -p sales -t $t -o evidence
done
for t in clean orders_fact monthly; do
  ubunye run -d pipelines -u olist -p sales -t $t --backend spark --lineage --var out_dir=output/spark
  ubunye prove observe --workload r2-olist-$t --env spark-local -d pipelines -u olist -p sales -t $t -o evidence
done
ubunye prove report evidence --workload r2-olist-orders_fact --reference spark-local
```

(`prove observe` takes one task at a time, hence the loop.) Every dimension should say
PASS, with the same digest on both engines: `6affe2f5382c` for `clean`,
`6dcf270328d8` for `orders_fact`, `a0bd206029ca` for `monthly`. The repository's
tests check these digests on pandas, and on Spark 3.5 and 4 in CI.

## 8. The real data

The real Olist data (about 100,000 orders) is on Kaggle under a non-commercial
licence (CC BY-NC-SA 4.0), so it is not in the repository. Download it when you run:

```bash
pip install kaggle        # plus a Kaggle API token: https://www.kaggle.com/settings
bash scripts/fetch_data.sh
for t in clean orders_fact monthly; do
  ubunye run -d pipelines -u olist -p sales -t $t --backend pandas --lineage --var data_dir=data --var out_dir=output-real
done
```

Nothing in the pipeline changes: only where the data is.

## What each feature caught

| Feature | Where | What it caught in this tutorial |
|---|---|---|
| Input contract | `clean` | one date saved the wrong way turned a column to text (step 5) |
| Quarantine | `clean`, `orders_fact` | a free item, an unknown payment type, a score of 7, an order delivered before it was bought |
| Reconcile | `clean`, `orders_fact` | an inner join that lost a canceled order and its 45.90 (step 6) |
| Warning | `orders_fact` | four orders paid more or less than they cost |
| Run record | every step | every check, every count, and why a run failed |

## Clean up

```bash
rm -rf output output-bad output-real data evidence pipelines/.ubunye
```

## Common problems

- `ImportError: the example needs narwhals>=2.9 (Expr.floor)`: upgrade Narwhals
  (`pip install -U narwhals`). Every step checks this first.
- `pyarrow` older than 24 on Windows cannot find a time zone database; upgrade it.
- `kaggle: command not found` or `401`: install the `kaggle` package and put your API
  token where `scripts/fetch_data.sh` says.
