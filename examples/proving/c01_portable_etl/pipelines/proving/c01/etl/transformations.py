"""C01: paid order lines with their customer, and a summary per country.

Written once with Narwhals, so the same code runs on pandas and on Spark. What it
exercises, on purpose:

- a left join where some orders have no customer, or an unknown one (null keys);
- a filter on text;
- arithmetic with a null (a missing quantity gives a missing amount);
- a group-by whose key is sometimes null (Spark keeps one null group; plain pandas
  drops it by default, which is the divergence this workload is here to catch);
- counts and sums cast to one integer type, since engines type them differently;
- timestamps kept as instants (UTC) and truncated to a day.

Money is in integer cents: a floating-point sum depends on the order of addition,
which differs between engines. That is a separate question, for its own workload.
"""

import narwhals as nw

from ubunye.core.interfaces import Task


class PaidOrders(Task):
    def transform(self, sources):
        orders = nw.from_native(sources["orders"])
        customers = nw.from_native(sources["customers"]).select("customer_id", "name", "country")

        lines = (
            orders.filter(nw.col("status") == "paid")
            .join(customers, on="customer_id", how="left")
            .with_columns(
                amount_cents=(nw.col("qty") * nw.col("unit_price_cents")).cast(nw.Int64),
                order_day=nw.col("ordered_at").dt.truncate("1d"),
            )
            .select(
                "order_id",
                "customer_id",
                "name",
                "country",
                "qty",
                "unit_price_cents",
                "amount_cents",
                "ordered_at",
                "order_day",
            )
        )

        summary = lines.group_by("country", drop_null_keys=False).agg(
            orders=nw.len().cast(nw.Int64),
            customers=nw.col("customer_id").n_unique().cast(nw.Int64),
            revenue_cents=nw.col("amount_cents").sum().cast(nw.Int64),
            first_order=nw.col("ordered_at").min(),
            last_order=nw.col("ordered_at").max(),
        )

        return {"order_lines": lines, "country_summary": summary}
