"""Step 1: the nine raw Olist files, typed and cleaned.

What the raw data makes you decide, and what this step decides:

- The raw files are checked first (the input contracts in config.yaml): if a file
  arrives with a column missing, renamed or turned to text, the run stops before this
  code runs, naming the column.
- Times are written without a zone (Brazil's clock). Ubunye reads them in UTC on every
  engine (ADR 007), so the clock reading is kept exactly as written.
- A zip code prefix like 01310 is read as the number 1310 (both engines infer an
  integer). It still joins, since every file loses the same zero.
- Category names are Portuguese; the translation file gives English. A category it
  does not translate keeps its Portuguese name (`category_translated` is false), and a
  product with no category is "unknown". No product is dropped.
- The geolocation file has many points per zip prefix, exact duplicates, and some
  points outside Brazil. This keeps one point per prefix: the mean of its distinct
  points inside Brazil, to 6 decimals.
- Review comments are free text. Only whether a review has a comment is kept.
- Bad items, payments and reviews are not dropped here: the expectations in
  config.yaml set them aside in the `*_rejected` outputs with the rule they broke.

Written with Narwhals, so the same code runs on pandas (a laptop, no Java) and Spark.
"""

import narwhals as nw

from ubunye.core.interfaces import Task

# Expr.floor came in Narwhals 2.9; older ones fail later with a less clear error.
if not hasattr(nw.Expr, "floor"):
    raise ImportError(
        "the example needs narwhals>=2.9 (Expr.floor); upgrade with: pip install -U narwhals"
    )


def micro(expr):
    """A number in whole millionths: integers add up exactly, in any order (F-029)."""
    return (expr * 1_000_000 + 0.5).floor().cast(nw.Int64)


class CleanOlist(Task):
    def transform(self, sources):
        box = self.config["CONFIG"]["transform"]["params"]["brazil"]
        raw = {name: nw.from_native(frame) for name, frame in sources.items()}

        orders = raw["raw_orders"].select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        )

        order_items = raw["raw_items"].with_columns(nw.col("order_item_id").cast(nw.Int32))
        payments = raw["raw_payments"].with_columns(
            nw.col("payment_sequential", "payment_installments").cast(nw.Int32)
        )
        reviews = raw["raw_reviews"].select(
            "review_id",
            "order_id",
            nw.col("review_score").cast(nw.Int32),
            (~nw.col("review_comment_message").is_null()).alias("has_comment"),
            "review_creation_date",
            "review_answer_timestamp",
        )

        # One point per zip prefix: the mean of its distinct points inside Brazil.
        geolocation = (
            raw["raw_geolocation"]
            .select(
                nw.col("geolocation_zip_code_prefix").cast(nw.Int32).alias("zip_code_prefix"),
                nw.col("geolocation_lat").alias("lat"),
                nw.col("geolocation_lng").alias("lng"),
            )
            .filter(
                nw.col("lat").is_between(box["lat_min"], box["lat_max"]),
                nw.col("lng").is_between(box["lng_min"], box["lng_max"]),
            )
            .unique()
            .with_columns(lat_micro=micro(nw.col("lat")), lng_micro=micro(nw.col("lng")))
            .group_by("zip_code_prefix")
            .agg(
                nw.col("lat_micro").sum(),
                nw.col("lng_micro").sum(),
                points=nw.len(),
            )
            .with_columns(nw.col("points").cast(nw.Int64))
            .with_columns(
                lat=(nw.col("lat_micro") / nw.col("points") + 0.5).floor() / 1_000_000,
                lng=(nw.col("lng_micro") / nw.col("points") + 0.5).floor() / 1_000_000,
            )
            .select("zip_code_prefix", "lat", "lng", "points")
        )

        customers = (
            raw["raw_customers"]
            .with_columns(nw.col("customer_zip_code_prefix").cast(nw.Int32))
            .join(
                geolocation.select(
                    nw.col("zip_code_prefix").alias("customer_zip_code_prefix"),
                    nw.col("lat").alias("customer_lat"),
                    nw.col("lng").alias("customer_lng"),
                ),
                on="customer_zip_code_prefix",
                how="left",
            )
        )
        sellers = raw["raw_sellers"].with_columns(nw.col("seller_zip_code_prefix").cast(nw.Int32))

        english = raw["raw_categories"]
        products = (
            raw["raw_products"]
            .select(
                "product_id", "product_category_name", nw.col("product_weight_g").cast(nw.Int64)
            )
            .join(english, on="product_category_name", how="left")
            .with_columns(
                # Narwhals has no when().when(): the fallbacks nest.
                category=nw.when(~nw.col("product_category_name_english").is_null())
                .then(nw.col("product_category_name_english"))
                .otherwise(
                    nw.when(~nw.col("product_category_name").is_null())
                    .then(nw.col("product_category_name"))
                    .otherwise(nw.lit("unknown"))
                ),
                category_translated=~nw.col("product_category_name_english").is_null(),
            )
            .select(
                "product_id",
                "category",
                "category_translated",
                nw.col("product_category_name").alias("category_pt"),
                "product_weight_g",
            )
        )

        return {
            "orders": orders,
            "order_items": order_items,
            "payments": payments,
            "reviews": reviews,
            "customers": customers,
            "sellers": sellers,
            "products": products,
            "geolocation": geolocation,
        }
