"""Write the small synthetic sample in sample/: nine CSV files shaped like Olist's.

Every value here is made up by this script. No row comes from the Olist data: the
file names, column names, types and formats match the Kaggle files, so the pipeline
reads the sample and the real data the same way, and nothing else is shared.

It also copies the three traps the real files hold:

- the category translation file starts with a UTF-8 byte order mark;
- review comments span lines and double their quotes (""), as pandas and Excel
  write CSV, so the reviews input needs escape '"' and multiLine;
- a zip code prefix is written with its leading zero (01310), which inferSchema
  reads as the number 1310 on both engines.

And it plants the bad rows the pipeline must catch (see README.md, "What it
catches"). Run it again and it writes the same bytes:

    python scripts/make_sample.py
"""

from __future__ import annotations

import csv
import hashlib
import io
import random
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
OUT = HERE / "sample"
rng = random.Random(20260930)
OUT.mkdir(exist_ok=True)


def hex_id(kind: str, n: object) -> str:
    """A 32 character hex id, like Olist's, from a made up name."""
    return hashlib.md5(f"ubunye-sample-{kind}-{n}".encode()).hexdigest()


def ts(t: datetime | None) -> str:
    return "" if t is None else t.strftime("%Y-%m-%d %H:%M:%S")


def day(t: datetime) -> datetime:
    return t.replace(hour=0, minute=0, second=0)


def money(x: float) -> str:
    return repr(round(x, 2))


def write(name: str, header: list[str], rows: list[list[object]], *, bom: bool = False) -> None:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, doublequote=True)
    w.writerow(header)
    w.writerows(rows)
    data = buf.getvalue().encode("utf-8")
    (OUT / name).write_bytes((b"\xef\xbb\xbf" if bom else b"") + data)


# --------------------------------------------------------------------------- places

# (zip prefix, city, state, lat, lng): real places, rough centres, made up prefixes.
PLACES = [
    ("01310", "sao paulo", "SP", -23.561, -46.656),
    ("04538", "sao paulo", "SP", -23.586, -46.682),
    ("13015", "campinas", "SP", -22.905, -47.061),
    ("20040", "rio de janeiro", "RJ", -22.903, -43.176),
    ("24020", "niteroi", "RJ", -22.894, -43.123),
    ("30130", "belo horizonte", "MG", -19.920, -43.938),
    ("80010", "curitiba", "PR", -25.429, -49.271),
    ("90010", "porto alegre", "RS", -30.027, -51.228),
    ("40010", "salvador", "BA", -12.971, -38.501),
    ("69005", "manaus", "AM", -3.119, -60.021),
    ("88010", "florianopolis", "SC", -27.595, -48.548),
]
# A prefix whose only point lies outside Brazil: a customer there gets no location.
LOST_ZIP = ("59000", "natal", "RN")

geo_rows: list[list[object]] = []
for z, city, state, lat, lng in PLACES:
    for k in range(rng.randint(2, 4)):
        p = [z, round(lat + rng.uniform(-0.02, 0.02), 6), round(lng + rng.uniform(-0.02, 0.02), 6)]
        geo_rows.append([p[0], p[1], p[2], city, state])
    geo_rows.append(list(geo_rows[-1]))  # an exact duplicate, as the real file has many
# A point far outside Brazil for a Sao Paulo prefix (the real file has some in Europe).
geo_rows.append(["01310", 38.722252, -9.139337, "sao paulo", "SP"])
geo_rows.append([LOST_ZIP[0], 41.1579, -8.6291, LOST_ZIP[1], LOST_ZIP[2]])
rng.shuffle(geo_rows)
write(
    "olist_geolocation_dataset.csv",
    [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ],
    geo_rows,
)

# --------------------------------------------------------------------------- sellers

SELLERS = []
for i, place in enumerate([PLACES[0], PLACES[2], PLACES[3], PLACES[5], PLACES[6], PLACES[8]]):
    SELLERS.append((hex_id("seller", i), place[0], place[1], place[2]))
write(
    "olist_sellers_dataset.csv",
    ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    [list(s) for s in SELLERS],
)

# --------------------------------------------------------------------------- products

# Portuguese category names and this script's own English for them.
TRANSLATION = [
    ("brinquedos", "toys"),
    ("livros", "books"),
    ("utilidades_cozinha", "kitchen_utilities"),
    ("esporte", "sport"),
    ("papelaria", "stationery"),
    ("jardim", "garden"),
    ("relogios", "watches"),
]
UNTRANSLATED = "jogos_pc"  # in products, missing from the translation file
write(
    "product_category_name_translation.csv",
    ["product_category_name", "product_category_name_english"],
    [list(t) for t in TRANSLATION],
    bom=True,
)

PRODUCTS = []  # (id, category or None, base price)
cats = [t[0] for t in TRANSLATION]
for i in range(14):
    if i == 12:
        cat = UNTRANSLATED
    elif i == 13:
        cat = None  # the real file has products with no category
    else:
        cat = cats[i % len(cats)]
    PRODUCTS.append(
        (hex_id("product", i), cat, rng.choice([19.9, 34.5, 59.0, 89.9, 129.99, 249.0]))
    )
prod_rows = []
for pid, cat, _ in PRODUCTS:
    if cat is None:
        prod_rows.append([pid, "", "", "", "", 500, 20, 10, 15])
    else:
        prod_rows.append(
            [
                pid,
                cat,
                rng.randint(20, 60),
                rng.randint(100, 2000),
                rng.randint(1, 6),
                rng.randint(100, 5000),
                rng.randint(10, 60),
                rng.randint(2, 40),
                rng.randint(10, 50),
            ]
        )
write(
    "olist_products_dataset.csv",
    [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
    prod_rows,
)

# --------------------------------------------------------------------------- orders

customers: list[list[object]] = []
orders: list[list[object]] = []
items: list[list[object]] = []
payments: list[list[object]] = []
reviews: list[list[object]] = []


def add_order(
    n: int,
    *,
    status: str = "delivered",
    lines: int = 1,
    days_to_deliver: int | None = 8,
    days_estimated: int = 15,
    pay: list[tuple[str, float, int]] | None = None,
    extra_pay: float = 0.0,
    review: list[tuple[int, int, str, str]] | None = None,
    place=None,
    person: int | None = None,
    sellers: list[int] | None = None,
    prices: list[float] | None = None,
    start: datetime | None = None,
) -> str:
    """One order with its customer, items, payments and reviews."""
    oid, cid = hex_id("order", n), hex_id("customer", n)
    place = place or rng.choice(PLACES)
    customers.append([cid, hex_id("person", person if person is not None else n), *place[:3]])
    bought = start or datetime(2018, 1, 3) + timedelta(
        days=rng.randint(0, 115), hours=rng.randint(7, 22), minutes=rng.randint(0, 59)
    )
    bought = bought.replace(second=rng.randint(0, 59))
    approved = bought + timedelta(minutes=rng.randint(10, 600)) if status != "created" else None
    carrier = bought + timedelta(days=2, hours=3) if status in ("delivered", "shipped") else None
    delivered = None
    if status == "delivered" and days_to_deliver is not None:
        delivered = bought + timedelta(days=days_to_deliver, hours=rng.randint(1, 9))
    estimated = day(bought + timedelta(days=days_estimated))
    orders.append(
        [oid, cid, status, ts(bought), ts(approved), ts(carrier), ts(delivered), ts(estimated)]
    )
    total = 0.0
    for k in range(lines):
        pid, _, base = rng.choice(PRODUCTS[:13] if k else PRODUCTS)
        price = prices[k] if prices else base
        freight = round(rng.choice([0.0, 7.39, 12.79, 15.1, 18.23, 24.84]), 2)
        seller = SELLERS[sellers[k] if sellers else rng.randrange(len(SELLERS))][0]
        items.append(
            [oid, k + 1, pid, seller, ts(bought + timedelta(days=4)), money(price), money(freight)]
        )
        total += price + freight
    if pay is None:
        pay = [(rng.choice(["credit_card", "credit_card", "boleto", "debit_card"]), 1.0, 0)]
    for seq, (kind, share, inst) in enumerate(pay, start=1):
        value = total * share + (extra_pay if seq == 1 else 0.0)
        payments.append(
            [
                oid,
                seq,
                kind,
                inst or (rng.randint(1, 10) if kind == "credit_card" else 1),
                money(value),
            ]
        )
    for k, (rid, score, title, text) in enumerate(review or []):
        # A second review of one order comes later: the latest one counts.
        made = day((delivered or bought) + timedelta(days=rid % 3 + 1 + 5 * k))
        reviews.append(
            [
                hex_id("review", rid),
                oid,
                score,
                title,
                text,
                ts(made),
                ts(made + timedelta(days=1, hours=rng.randint(0, 20), minutes=rng.randint(0, 59))),
            ]
        )
    return oid


r = 1000  # review numbers


def next_review() -> int:
    global r
    r += 1
    return r


# The named cases: each is asserted in the tests.
add_order(1, days_to_deliver=6, days_estimated=14, review=[(next_review(), 5, "", "")])
add_order(
    2,
    days_to_deliver=21,
    days_estimated=12,
    review=[(next_review(), 1, "Atrasou", "Chegou muito depois do prazo.")],
)
add_order(
    3,
    lines=2,
    sellers=[0, 3],
    pay=[("credit_card", 0.8, 3), ("voucher", 0.2, 1)],
    review=[(next_review(), 4, "", "")],
)
add_order(
    4, status="canceled", lines=0, days_to_deliver=None, pay=[("boleto", 0.0, 1)], extra_pay=45.9
)
add_order(5, status="shipped", days_to_deliver=None, review=[])
add_order(
    6,
    review=[
        (next_review(), 2, "", "Produto errado"),
        (next_review(), 4, "Resolvido", 'Trocaram o produto, "nota 10" pelo atendimento.'),
    ],
)
add_order(7, review=[])
add_order(8, pay=[("credit_card", 1.0, 10)], extra_pay=3.2, review=[(next_review(), 5, "", "")])
add_order(
    9, days_to_deliver=-3, review=[(next_review(), 3, "", "")]
)  # delivered before it was bought
add_order(
    10, lines=2, prices=[0.0, 89.9], review=[(next_review(), 4, "", "")]
)  # a free item: price 0
add_order(
    11,
    review=[
        (
            next_review(),
            7,
            "Otimo",
            'Recomendo.\nChegou antes do prazo e bem embalado:\n"perfeito".',
        )
    ],
)
add_order(
    12, pay=[("pix", 1.0, 1)], review=[(next_review(), 5, "", "")]
)  # a payment type the rules do not know
add_order(13, place=(LOST_ZIP[0], LOST_ZIP[1], LOST_ZIP[2]), review=[(next_review(), 5, "", "")])
add_order(
    14, person=1, review=[(next_review(), 5, "", "")]
)  # a repeat customer: the person of order 1
# Two orders sharing one review id: the real file has this too.
shared = next_review()
add_order(15, review=[(shared, 4, "", "")])
add_order(16, review=[(shared, 4, "", "")])

# The rest: ordinary orders, so the monthly tables have something to add up.
for n in range(17, 61):
    lines = rng.choice([1, 1, 1, 2, 3])
    add_order(
        n,
        lines=lines,
        days_to_deliver=rng.randint(3, 25),
        days_estimated=rng.randint(10, 30),
        review=(
            [(next_review(), rng.choice([1, 3, 4, 5, 5, 5]), "", "")] if rng.random() < 0.85 else []
        ),
    )

write(
    "olist_customers_dataset.csv",
    [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],
    customers,
)
write(
    "olist_orders_dataset.csv",
    [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    orders,
)
write(
    "olist_order_items_dataset.csv",
    [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ],
    items,
)
write(
    "olist_order_payments_dataset.csv",
    ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    payments,
)
write(
    "olist_order_reviews_dataset.csv",
    [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ],
    reviews,
)

if __name__ == "__main__":
    for f in sorted(OUT.glob("*.csv")):
        print(f"{f.name}: {sum(1 for _ in f.open(encoding='utf-8-sig')) - 1} lines")
