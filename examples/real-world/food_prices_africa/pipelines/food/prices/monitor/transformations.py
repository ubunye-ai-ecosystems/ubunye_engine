"""Step 2: monthly staple prices per country, the change on a year before, alerts.

- One monthly price per country, food and currency: the mean price per kg over the
  markets seen that month, with how many markets and prices it rests on, so a reader
  can tell a price from one market from a price from forty.
- The change on a year before is a join on (year - 1, same month), not a window: it
  means the same on every engine, and a month with no price a year before simply has
  no change, rather than a change against the wrong month.
- Prices are rounded half up to 4 decimals, by hand (`round4`): `.round()` rounds a
  half up on Spark and to even on pandas, and the proving ground caught 43 monthly
  prices differing by 0.0001 (finding F-029). The mean is an exact sum of whole
  millionths divided once: a mean of floats differs in its last bits between engines,
  which add in different orders, and that still moved 7 prices across a rounding
  boundary.
- An alert needs prices from at least `min_markets` markets that month: one market's
  price doubling is weak evidence, and a monitor that cries wolf is ignored.
- Money stays in the local currency the market quoted: a rise in local money is what
  a household feels. The USD price is kept alongside for comparison across countries.

Written with Narwhals, so the same code runs on pandas and Spark.
"""

import narwhals as nw

from ubunye.core.interfaces import Task

KEYS = ["countryiso3", "commodity", "currency"]


def round4(expr):
    """Round half up to 4 decimals, the same on every engine.

    `.round(4)` is not: Spark rounds a half up and pandas rounds it to even, so a mean
    of 921.03125 became 921.0313 on Spark and 921.0312 on pandas (finding F-029).
    """
    return (expr * 10000 + 0.5).floor() / 10000


def micro(expr):
    """A price in whole millionths: integers add up exactly, in any order."""
    return (expr * 1_000_000 + 0.5).floor().cast(nw.Int64)


class MonitorPrices(Task):
    def transform(self, sources):
        params = self.config["CONFIG"]["transform"]["params"]
        prices = nw.from_native(sources["prices_per_kg"]).with_columns(
            year=nw.col("date").dt.year().cast(nw.Int32),
            month=nw.col("date").dt.month().cast(nw.Int32),
            price_micro=micro(nw.col("price_per_kg")),
            usd_micro=micro(nw.col("usd_per_kg")),
        )

        monthly = (
            prices.group_by(*KEYS, "category", "year", "month")
            .agg(
                # Sums of whole millionths, not a mean of floats: a float sum depends on
                # the order it is added in, which differs between engines; an integer
                # sum does not (finding F-029).
                price_micro_sum=nw.col("price_micro").sum(),
                usd_micro_sum=nw.col("usd_micro").sum(),
                markets=nw.col("market_id").n_unique(),
                prices=nw.len(),
            )
            .with_columns(
                # Cast after the aggregation, not inside it: on pandas, Narwhals 2.26 drops
                # a cast inside a group-by that also averages (the counts came out as
                # float64, and the schema no longer matched Spark's). Finding F-027.
                nw.col("markets", "prices").cast(nw.Int64),
            )
            .with_columns(
                price_per_kg=round4(nw.col("price_micro_sum") / nw.col("prices") / 1_000_000),
                usd_per_kg=round4(nw.col("usd_micro_sum") / nw.col("prices") / 1_000_000),
            )
            .drop("price_micro_sum", "usd_micro_sum")
        )

        before = monthly.select(
            *KEYS,
            "month",
            (nw.col("year") + 1).cast(nw.Int32).alias("year"),
            nw.col("price_per_kg").alias("price_per_kg_year_before"),
        )
        changes = monthly.join(before, on=[*KEYS, "year", "month"], how="left").with_columns(
            change_on_year=round4(nw.col("price_per_kg") / nw.col("price_per_kg_year_before") - 1)
        )

        alerts = changes.filter(
            nw.col("category").is_in(params["staples"]),
            ~nw.col("change_on_year").is_null(),
            nw.col("change_on_year") >= params["rise_threshold"],
            nw.col("markets") >= params["min_markets"],
        ).select(
            *KEYS,
            "category",
            "year",
            "month",
            "price_per_kg",
            "price_per_kg_year_before",
            "change_on_year",
            "markets",
        )

        return {"monthly": changes, "alerts": alerts}
