"""Step 1: WFP market prices for Africa, retail, one price per kilogram.

What the raw data makes you decide, and what this step decides:

- WFP publishes both prices seen in a market (`priceflag` "actual") and its own
  averages of them ("aggregate"). Keeping both would count one market twice: only
  "actual" is kept.
- Retail and wholesale prices differ by design: only retail, what households pay.
- A price is quoted per kg, per 25 kg, per 100 kg, per litre, per head... Only weights
  in kilograms are comparable, so those are turned into a price per kg; the rest (litres,
  units, heads, days) are left out rather than guessed.
- Casting "25 KG" to 25 is done only after keeping the rows that are weights: pandas
  fails on a row it cannot cast, Spark 3.5 quietly makes it null, Spark 4 fails. Filter,
  then cast, is the same on all three.

Written with Narwhals, so the same code runs on pandas (a laptop, no Java) and Spark.
"""

import narwhals as nw

from ubunye.core.interfaces import Task


class CleanPrices(Task):
    def transform(self, sources):
        africa = self.config["CONFIG"]["transform"]["params"]["africa"]
        raw = nw.from_native(sources["raw"])

        kept = raw.filter(
            nw.col("countryiso3").is_in(africa),
            nw.col("pricetype") == "Retail",
            nw.col("priceflag") == "actual",
            ~nw.col("price").is_null(),
        ).with_columns(
            unit_kg=nw.when(nw.col("unit") == "KG").then(nw.lit("1 KG")).otherwise(nw.col("unit"))
        )
        weights = kept.filter(nw.col("unit_kg").str.contains("^[0-9.]+ KG$").fill_null(False))
        per_kg = weights.with_columns(
            # replace_all, literal: Narwhals has no `str.replace` on Spark (finding F-028).
            kg_per_unit=nw.col("unit_kg")
            .str.replace_all(" KG", "", literal=True)
            .cast(nw.Float64)
        ).with_columns(
            price_per_kg=nw.col("price") / nw.col("kg_per_unit"),
            usd_per_kg=nw.col("usdprice") / nw.col("kg_per_unit"),
        )

        return {
            "prices_per_kg": per_kg.select(
                "countryiso3",
                "date",
                "admin1",
                "market",
                "market_id",
                "latitude",
                "longitude",
                "category",
                "commodity",
                "currency",
                "unit",
                "kg_per_unit",
                "price",
                "price_per_kg",
                "usd_per_kg",
            )
        }
