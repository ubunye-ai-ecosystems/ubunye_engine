"""Step 2: one row per order.

- Money is added up in whole cents (integers), then turned back into reais: a sum of
  floats depends on the order it is added in, which differs between engines (F-029).
- An order with no items (most are canceled) keeps its row, with zero items. An order
  with no review keeps its row, with no score. The reconcile in config.yaml holds this:
  every order in, exactly one row out.
- An order reviewed more than once takes its latest review (the highest score if two
  share a day), and says how many reviews it had.
- Delivery days are calendar days from purchase to delivery, and on time means
  delivered on or before the estimated day. Narwhals cannot subtract two timestamps
  on Spark, so each date becomes a day number (days since year 1) first.

Written with Narwhals, so the same code runs on pandas and Spark.
"""

import narwhals as nw

from ubunye.core.interfaces import Task


def cents(expr):
    """Reais to whole cents, rounded half up."""
    return (expr * 100 + 0.5).floor().cast(nw.Int64)


def day_number(expr):
    """A timestamp's date as a count of days, so two can be subtracted.

    Only whole number arithmetic on the year, month and day, so every engine gives
    the same number (the civil-from-days method, with years starting in March so the
    leap day comes last). Not `dt.ordinal_day()`: on pandas it fails on a column
    with a null in it (finding F-053), and an order not yet delivered has no delivery date.
    """
    month = expr.dt.month().cast(nw.Int64)
    y = expr.dt.year().cast(nw.Int64) - nw.when(month <= 2).then(1).otherwise(0)
    # March is 0, February is 11. Not `% 12`: pandas 2 has no modulo on Arrow integers.
    march_based = nw.when(month >= 3).then(month - 3).otherwise(month + 9)
    day_of_year = (march_based * 153 + 2) // 5 + expr.dt.day().cast(nw.Int64) - 1
    return (y * 365 + y // 4 - y // 100 + y // 400 + day_of_year).cast(nw.Int64)


class OrdersFact(Task):
    def transform(self, sources):
        orders = nw.from_native(sources["orders"])
        items = nw.from_native(sources["order_items"])
        payments = nw.from_native(sources["payments"])
        reviews = nw.from_native(sources["reviews"])
        customers = nw.from_native(sources["customers"])

        per_order_items = (
            items.with_columns(
                price_cents=cents(nw.col("price")), freight_cents=cents(nw.col("freight_value"))
            )
            .group_by("order_id")
            .agg(
                items=nw.len(),
                sellers=nw.col("seller_id").n_unique(),
                price_cents=nw.col("price_cents").sum(),
                freight_cents=nw.col("freight_cents").sum(),
            )
        )
        per_order_payments = (
            payments.with_columns(value_cents=cents(nw.col("payment_value")))
            .group_by("order_id")
            .agg(payments=nw.len(), payments_cents=nw.col("value_cents").sum())
        )

        # The latest review of each order; the highest score if two share that day.
        latest = reviews.group_by("order_id").agg(
            reviews=nw.len(), review_creation_date=nw.col("review_creation_date").max()
        )
        per_order_review = (
            reviews.join(latest, on=["order_id", "review_creation_date"], how="inner")
            .group_by("order_id", "reviews")
            .agg(review_score=nw.col("review_score").max())
        )

        bought = nw.col("order_purchase_timestamp")
        delivered = nw.col("order_delivered_customer_date")
        fact = (
            orders.join(
                customers.select("customer_id", "customer_unique_id", "customer_state"),
                on="customer_id",
                how="left",
            )
            .join(per_order_items, on="order_id", how="left")
            .join(per_order_payments, on="order_id", how="left")
            .join(per_order_review, on="order_id", how="left")
            .with_columns(
                # Counts and sums cast after the aggregation, not inside it (F-027);
                # an order with no items or payments gets 0, not null.
                nw.col("items", "sellers", "payments", "reviews").fill_null(0).cast(nw.Int64),
                nw.col("price_cents", "freight_cents", "payments_cents")
                .fill_null(0)
                .cast(nw.Int64),
                nw.col("review_score").cast(nw.Int32),
                purchase_year=bought.dt.year().cast(nw.Int32),
                purchase_month=bought.dt.month().cast(nw.Int32),
                delivery_days=(day_number(delivered) - day_number(bought)).cast(nw.Int32),
                delivered_on_time=(
                    day_number(delivered) <= day_number(nw.col("order_estimated_delivery_date"))
                ).cast(nw.Boolean),
            )
            .with_columns(
                items_price=nw.col("price_cents") / 100,
                freight=nw.col("freight_cents") / 100,
                payments_total=nw.col("payments_cents") / 100,
                payment_gap=(
                    nw.col("payments_cents") - nw.col("price_cents") - nw.col("freight_cents")
                )
                / 100,
            )
        )

        return {
            "orders_fact": fact.select(
                "order_id",
                "customer_unique_id",
                "customer_state",
                "order_status",
                "order_purchase_timestamp",
                "purchase_year",
                "purchase_month",
                "items",
                "sellers",
                "items_price",
                "freight",
                "payments",
                "payments_total",
                "payment_gap",
                "delivery_days",
                "delivered_on_time",
                "reviews",
                "review_score",
            )
        }
