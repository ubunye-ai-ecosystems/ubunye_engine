"""Step 3: monthly sales per seller and per product category.

- Sales (GMV) is the price of the items sold, in the month the order was placed,
  without freight. Canceled and unavailable orders sold nothing and are left out.
- An order set aside by step 2 (its dates were wrong) is not in the fact table, so it
  is not counted here either: its rows are in `orders_fact_rejected`, waiting for a fix.
- On-time rate: of a seller's delivered orders that month, the share that arrived on
  or before the estimated day. A month with no delivered order has no rate (null),
  not 0%: nothing was late.
- An order with items from two sellers counts once for each seller.
- Money is added up in whole cents, then turned back into reais (F-029).

Written with Narwhals, so the same code runs on pandas and Spark.
"""

import narwhals as nw

from ubunye.core.interfaces import Task

MONTH = ["purchase_year", "purchase_month"]


def cents(expr):
    """Reais to whole cents, rounded half up."""
    return (expr * 100 + 0.5).floor().cast(nw.Int64)


class MonthlySales(Task):
    def transform(self, sources):
        not_sold = self.config["CONFIG"]["transform"]["params"]["not_sold"]
        fact = nw.from_native(sources["orders_fact"])
        items = nw.from_native(sources["order_items"])
        sellers = nw.from_native(sources["sellers"])
        products = nw.from_native(sources["products"])

        sold = (
            fact.filter(~nw.col("order_status").is_in(not_sold))
            .select(
                "order_id",
                *MONTH,
                # 1 or 0, so they add up; a null on-time (not delivered yet) is 0 and 0.
                delivered=nw.when(~nw.col("delivered_on_time").is_null())
                .then(1)
                .otherwise(0)
                .cast(nw.Int64),
                on_time=nw.when(nw.col("delivered_on_time")).then(1).otherwise(0).cast(nw.Int64),
            )
            .join(
                items.select(
                    "order_id", "seller_id", "product_id", price_cents=cents(nw.col("price"))
                ),
                on="order_id",
                how="inner",
            )
        )

        # One row per seller and order first, so an order counts once per seller.
        seller_orders = sold.group_by("seller_id", "order_id", *MONTH).agg(
            items=nw.len(),
            price_cents=nw.col("price_cents").sum(),
            delivered=nw.col("delivered").max(),
            on_time=nw.col("on_time").max(),
        )
        seller_monthly = (
            seller_orders.group_by("seller_id", *MONTH)
            .agg(
                orders=nw.len(),
                items=nw.col("items").sum(),
                gmv_cents=nw.col("price_cents").sum(),
                delivered_orders=nw.col("delivered").sum(),
                on_time_orders=nw.col("on_time").sum(),
            )
            .with_columns(
                nw.col("orders", "items", "gmv_cents", "delivered_orders", "on_time_orders").cast(
                    nw.Int64
                )
            )
            .with_columns(
                gmv=nw.col("gmv_cents") / 100,
                on_time_rate=nw.col("on_time_orders")
                / nw.when(nw.col("delivered_orders") > 0).then(nw.col("delivered_orders")),
            )
            .join(sellers.select("seller_id", "seller_state"), on="seller_id", how="left")
            .select(
                "seller_id",
                "seller_state",
                *MONTH,
                "orders",
                "items",
                "gmv",
                "delivered_orders",
                "on_time_orders",
                "on_time_rate",
            )
        )

        category_monthly = (
            sold.join(products.select("product_id", "category"), on="product_id", how="left")
            .group_by("category", *MONTH)
            .agg(
                orders=nw.col("order_id").n_unique(),
                items=nw.len(),
                gmv_cents=nw.col("price_cents").sum(),
            )
            .with_columns(nw.col("orders", "items", "gmv_cents").cast(nw.Int64))
            .with_columns(gmv=nw.col("gmv_cents") / 100)
            .select("category", *MONTH, "orders", "items", "gmv")
        )

        return {"seller_monthly": seller_monthly, "category_monthly": category_monthly}
