# A staple food price monitor for African markets

**Where are the prices of staple foods jumping, and by how much?** This pipeline takes
the World Food Programme's market price data (about 4 million prices from markets in
71 countries), keeps the retail prices from African markets, puts every price on the
same footing (local currency per kilogram), and flags every staple whose monthly
price rose by half or more on the same month a year before, where at least three
markets agree.

From the full data for 2025 and 2026, it flags, among others:

| Country | Food | A year before (per kg) | Now (per kg) | Change | Markets |
|---|---|---|---|---|---|
| Mali | Rice (paddy), Feb 2026 | 254 XOF | 620 XOF | +144% | 11 |
| Ethiopia | Groundnuts, Mar 2026 | 184 ETB | 408 ETB | +122% | 14 |
| Chad | Wheat flour, Jan 2026 | 432 XAF | 768 XAF | +78% | 54 |
| Sierra Leone | Beans (fava), Jan 2026 | 26,496 SLL | 50,012 SLL | +89% | 15 |

That is what a food security analyst watches: a price rise in the money a household
earns, backed by many markets, not one.

## Run it (about a minute, no Java, no account)

A small sample ships with the example (Chad and Mozambique, staple cereals, 2025 and
2026), so the first run needs nothing but Ubunye:

```bash
pip install "ubunye-engine[pandas]" "narwhals>=2.9"   # 2.9 added Expr.floor
cd examples/real-world/food_prices_africa
ubunye run -d pipelines -u food -p prices -t clean   --backend pandas --lineage
ubunye run -d pipelines -u food -p prices -t monitor --backend pandas --lineage
python -c "import pandas as pd; print(pd.read_parquet('output/alerts'))"
```

Expected: two alerts in Chad for January 2026, imported rice (+59%) and wheat flour
(+78%), each across 54 markets.

The whole data set, from Kaggle (needs a Kaggle API token; see the script):

```bash
bash scripts/fetch_data.sh
ubunye run -d pipelines -u food -p prices -t clean   --backend pandas --var data_dir=data
ubunye run -d pipelines -u food -p prices -t monitor --backend pandas
```

## What it does

1. **`clean`** keeps African countries, retail prices, and prices WFP saw in a market
   (not its own averages of them, which would count a market twice). A price is
   quoted per kg, per 25 kg, per 100 kg, per litre, per head: it keeps the weights and
   turns each into a price per kg. Data checks stop the run if a price is missing or
   not positive.
2. **`monitor`** makes one price per country, food and month (with how many markets it
   rests on), compares it with the same month a year before, and writes the monthly
   table and the alerts. The threshold (50%) and the minimum number of markets (3) are
   settings in `config.yaml`.

## The same answer on a laptop and on Spark

Both steps are written once with [Narwhals](https://narwhals-dev.github.io/narwhals/),
so they run on pandas and on Spark unchanged. On the 2025 and 2026 data (132,224 African
retail prices per kg), `ubunye prove` compared the run records: every row, every value,
the same on both.

| Step | pandas (laptop) | Spark (local) | Same rows? |
|---|---|---|---|
| `clean` | 2.5 s | 9.7 s | yes, digest `981427b26e1c` |
| `monitor` | 0.5 s | 31.4 s | yes, digest `e580f7f51483` |

Times are the task time from each run record, on one Windows laptop (Spark 4.2 in local
mode, pandas 3), one run each: a rough guide, not a benchmark.

At this size pandas is the right tool; Spark earns its start-up cost only on much more
data. The point is that the choice is yours, and moving costs nothing.

Getting there took four fixes, each a way the same code gave different numbers on two
engines. They are in the [portable transforms guide](../../../docs/guides/portable-transforms.md):
`round()` rounds halves differently, a mean of floats depends on the order it is added
in, a cast inside a group-by was lost on pandas, and `str.replace` does not exist on
Spark.

## Data and licence

World Food Programme, Global Food Prices Database, via the Humanitarian Data Exchange;
republished on Kaggle as `abhishekgupta56447/global-food-prices-database-wfp`. Licence:
Creative Commons Attribution 3.0 IGO (CC BY 3.0 IGO). The sample in `sample/` is a
subset (Chad and Mozambique, five staple cereals, retail, 2025 and 2026), unchanged.

South Africa is not in the WFP data; its neighbours Mozambique, Zimbabwe, Malawi,
Zambia, Lesotho and Eswatini are.
