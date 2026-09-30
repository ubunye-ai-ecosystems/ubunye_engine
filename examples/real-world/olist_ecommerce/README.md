# Olist sales: nine raw tables to an order fact table and monthly seller numbers

**Every order, once, with what was bought, what was paid, how long delivery took and
what the customer said. Then, per seller and month: sales and the share delivered on
time.** This is the first job a data team builds on an online shop's data. The data is
the Olist Brazilian e-commerce set: about 100,000 orders from 2016 to 2018 in nine CSV
files that must be joined.

The point of this example is what can go wrong on the way, and how Ubunye stops it:

| What goes wrong in real data | What stops it | Where |
|---|---|---|
| A raw file arrives with a column renamed, dropped, or turned into text | **input contract**: the run stops before the transform, naming the column | `clean` |
| A few bad rows (a free item, an unknown payment type, a review score of 7) | **quarantine**: set aside with the rule they broke; the rest is written | `clean` |
| A join quietly drops orders, or money goes missing between tables | **reconcile**: rows in = rows out, and payment totals match, or nothing is written | `clean`, `orders_fact` |
| An order delivered before it was bought | **quarantine** on the fact table | `orders_fact` |
| Paid more than the items and freight (instalment interest, vouchers) | **warning**: counted in the run record, run goes on | `orders_fact` |
| "What did this run read, write and check?" | **the run record** (`--lineage`) | every step |

## Run it (under a minute, no Java, no account)

A small made up sample ships with the example (60 orders), so the first run needs
nothing but Ubunye. **It needs a newer Ubunye than 0.7.1**, the latest release on
PyPI (0.7.1 has no `columns` input contracts), so until the next release install it
from this repository. From the repository's top folder:

```bash
cd examples/real-world/olist_ecommerce
pip install -e "../../..[pandas]" "narwhals>=2.9"
ubunye run -d pipelines -u olist -p sales -t clean       --backend pandas --lineage
ubunye run -d pipelines -u olist -p sales -t orders_fact --backend pandas --lineage
ubunye run -d pipelines -u olist -p sales -t monthly     --backend pandas --lineage
python -c "import pandas as pd; print(pd.read_parquet('output/seller_monthly'))"
```

Expected: the three runs end with `[OK]`, and `orders_fact` warns:

```text
expectation warning: orders_fact: payment_gap_between (between) broken by 4 of 60 rows
```

What landed where:

| Output | Rows | What it is |
|---|---|---|
| `output/orders_fact` | 59 | one row per order |
| `output/orders_fact_rejected` | 1 | the order delivered 3 days before it was bought |
| `output/order_items_rejected` | 1 | an item with price 0.00 |
| `output/payments_rejected` | 1 | a payment of type `pix`, which the rules do not know |
| `output/reviews_rejected` | 1 | a review scored 7 (scores go from 1 to 5) |
| `output/seller_monthly` | 24 | 6 sellers, January to April 2018 |
| `output/category_monthly` | 30 | 9 categories (one untranslated, one `unknown`) |

Each rejected row has a `_ubunye_failed_rules` column naming the rule it broke.
[Tutorial 2](../../../docs/tutorials/02-olist-multi-table.md) walks through all of it,
and breaks the pipeline on purpose to show each check catching its problem.

## The whole data set, from Kaggle

The real data is **not in this repository** and must not be committed (its licence is
below). Download it when you run:

```bash
pip install kaggle             # and a Kaggle API token, see scripts/fetch_data.sh
kaggle datasets download -d olistbr/brazilian-ecommerce -p data --unzip   # data/ is git-ignored
ubunye run -d pipelines -u olist -p sales -t clean       --backend pandas --lineage --var data_dir=data
ubunye run -d pipelines -u olist -p sales -t orders_fact --backend pandas --lineage
ubunye run -d pipelines -u olist -p sales -t monthly     --backend pandas --lineage
```

On a cloud, point the same tasks at object storage, nothing else changes:
`--var data_dir=s3://bucket/kaggle/olistbr/brazilian-ecommerce --var out_dir=s3://bucket/olist-out`.
On the full data (99,441 orders), measured once on one Windows machine with pandas:

| Step | Time | What the checks said |
|---|---|---|
| `clean` | 16 s | every raw file met its contract |
| `orders_fact` | 31 s | 99,441 orders in, 99,441 accounted for (reconcile passed); item and payment totals reconcile; 1,022 orders paid more or less than items and freight by over one real (warning) |
| `monthly` | 3 s | |

The sample is what CI checks; the full data is not run in CI.

## What it does

1. **`clean`** reads the nine files with a contract on each (columns and types), and
   writes them typed: timestamps as timestamps, categories in English (a category the
   translation file lacks keeps its Portuguese name; a product with none is
   `unknown`), and one point per zip code prefix from the geolocation file (the mean
   of its distinct points inside Brazil). Bad items, payments and reviews go to the
   `*_rejected` outputs. A reconcile checks every raw item and payment reached its
   output (set aside counts as reached) and that the price and payment sums match.
2. **`orders_fact`** makes one row per order: items, sellers, items price, freight,
   payments, the gap between paid and owed, delivery days, on time or not, the latest
   review score. It reconciles with its inputs before writing: every order in, exactly
   one row out; item prices and payments add up to the same totals.
3. **`monthly`** makes sales (GMV, item prices without freight) per seller and month,
   with the on-time rate of that seller's delivered orders, and sales per category and
   month. Canceled and unavailable orders sold nothing and are left out.

## The traps in the real files, and what the config does about them

- **The category translation file starts with a byte order mark** (the invisible
  bytes EF BB BF). Both engines drop it; before F-002 the pandas backend kept it in
  the first column's name.
- **Review comments run over several lines and double their quotes** (`""nota 10""`).
  Spark's default escape is a backslash, which splits these rows in the wrong place.
  The reviews input sets `multiLine: "true"` and `escape: '"'` (F-006).
- **Zip code prefixes are written with a leading zero** (`01310`) and read as the
  number 1310. Every file loses the same zero, so the joins still work.
- **Times have no zone.** They are Brazil's clock; Ubunye reads them in UTC on every
  engine, so the clock reading is kept exactly (ADR 007).
- **Money is added up in whole cents**, then turned back into reais, so the sums are
  the same on every engine (F-029).

## The same answer on pandas and Spark

All three steps are written once with [Narwhals](https://narwhals-dev.github.io/narwhals/).
On the sample, the pandas run records have these digests (one hash of every row of
every output of a step):

| Step | Digest |
|---|---|
| `clean` | `6affe2f5382c` |
| `orders_fact` | `6dcf270328d8` |
| `monthly` | `a0bd206029ca` |

`tests/unit/examples/test_olist_example.py` pins them on pandas.
`tests/integration/test_real_world_olist.py` runs the same steps on Spark and checks
the same digests and the same expectation results; CI runs it on Spark 3.5 and 4.

Building this example found two engine bugs, both fixed: a quarantined row's reasons
ended in a stray comma on pandas only (F-052), and a failed run's record did not say
why it failed (F-054). And one gap: there is no portable way to count days between two
dates, so `orders_fact` counts them with whole numbers (F-053, now in the
[portable transforms guide](../../../docs/guides/portable-transforms.md)).

## Data and licence

**The sample is made up.** `scripts/make_sample.py` writes it from a fixed random seed
and a list of planted cases; run it again and you get the same bytes (a test checks
that). It copies the real files' names, columns, types and formats, and the traps
above, and nothing else: no row, id, review text or price comes from Olist. The
places are real cities with rough coordinates; the zip prefixes, sellers, customers,
products and English category names are this script's own.

**The real data** is the Brazilian E-Commerce Public Dataset by Olist, on Kaggle as
[`olistbr/brazilian-ecommerce`](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
licensed [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/):
attribution, non-commercial use, share alike. That is why it is downloaded at run
time and never committed here.
