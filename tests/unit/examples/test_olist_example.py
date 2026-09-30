"""The real-world example `examples/real-world/olist_ecommerce` runs, and catches what it says.

Runs the three steps (clean, orders_fact, monthly) on the committed synthetic sample
on the pandas backend, as the README and Tutorial 2 tell a newcomer to, and checks:

- the golden data digests of every step (the same on Spark: see
  tests/integration/test_real_world_olist.py);
- each planted bad row lands where the README says (quarantine, warning);
- an input contract stops the run when a raw file changes type, before the transform;
- a reconcile stops the run when a join loses orders, and nothing is written.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
nw = pytest.importorskip("narwhals")
if not hasattr(nw.col("x"), "floor"):
    pytest.skip("the example needs narwhals>=2.9 (Expr.floor)", allow_module_level=True)

import pandas as pd  # noqa: E402

import ubunye  # noqa: E402
from ubunye.core.errors import ExpectationError  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

EXAMPLE = Path(__file__).resolve().parents[3] / "examples" / "real-world" / "olist_ecommerce"
STEPS = ("clean", "orders_fact", "monthly")
GOLDEN = {"clean": "6affe2f5382c", "orders_fact": "6dcf270328d8", "monthly": "a0bd206029ca"}


def order_id(n: int) -> str:
    """The id scripts/make_sample.py gives its order number n."""
    return hashlib.md5(f"ubunye-sample-order-{n}".encode()).hexdigest()


def _digest(record) -> str:
    joined = ";".join(
        f"{o.name}={o.data_hash}" for o in sorted(record.outputs, key=lambda o: o.name)
    )
    return hashlib.sha256(joined.encode()).hexdigest()[:12]


def _task(root: Path, step: str) -> str:
    return str(root / "pipelines" / "olist" / "sales" / step)


def _results(record, output: str) -> dict:
    return {e["rule"]: e for e in record.expectations if e["output"] == output}


@pytest.fixture
def example(tmp_path):
    root = tmp_path / "olist"
    shutil.copytree(
        EXAMPLE, root, ignore=shutil.ignore_patterns("output*", "data", "evidence", ".ubunye")
    )
    return root


def test_the_sample_is_what_the_script_writes(tmp_path):
    """The committed sample is the generator's output, byte for byte."""
    root = tmp_path / "olist"
    shutil.copytree(EXAMPLE / "scripts", root / "scripts")
    import runpy

    runpy.run_path(str(root / "scripts" / "make_sample.py"))
    written = sorted(p.name for p in (root / "sample").glob("*.csv"))
    assert len(written) == 9
    for name in written:
        assert (root / "sample" / name).read_bytes() == (EXAMPLE / "sample" / name).read_bytes()


def test_the_readme_run_gives_the_golden_rows_and_catches_the_bad_ones(example):
    for step in STEPS:
        ubunye.run_task(_task(example, step), backend="pandas", lineage=True)

    out = example / "output"
    store = FileSystemLineageStore(str(example / "pipelines" / ".ubunye" / "lineage"))
    records = {}
    for step, golden in GOLDEN.items():
        (record,) = store.list_runs(f"olist/sales/{step}")
        assert record.status == "success"
        assert _digest(record) == golden, step
        records[step] = record

    # clean: every raw file met its contract.
    contracts = [e for e in records["clean"].expectations if e.get("side") == "input"]
    assert len(contracts) == 9 and all(e["passed"] for e in contracts)

    # clean: one bad row of each kind set aside, with the rule it broke.
    items = pd.read_parquet(out / "order_items_rejected")
    assert list(items["price"]) == [0.0]
    assert list(items["_ubunye_failed_rules"]) == ["price_between"]
    payments = pd.read_parquet(out / "payments_rejected")
    assert list(payments["payment_type"]) == ["pix"]
    assert list(payments["_ubunye_failed_rules"]) == ["payment_type_one_of"]
    reviews = pd.read_parquet(out / "reviews_rejected")
    assert list(reviews["review_score"]) == [7]

    # clean: set aside is not lost; the reconcile counts it as carried over.
    rows = _results(records["clean"], "order_items")["rows_from_raw_items"]
    assert rows["passed"] and "80 rows read from raw_items, 80 reached" in rows["detail"]

    # clean: a category with no English keeps its Portuguese name; none is lost.
    products = pd.read_parquet(out / "products").set_index("category_pt", drop=False)
    assert products.loc["jogos_pc", "category"] == "jogos_pc"
    assert not products.loc["jogos_pc", "category_translated"]
    assert list(products[products["category_pt"].isna()]["category"]) == ["unknown"]
    assert set(products["category"]) >= {"toys", "books", "kitchen_utilities"}

    # clean: one point per zip prefix, inside Brazil. 59000's only point is abroad.
    geo = pd.read_parquet(out / "geolocation").set_index("zip_code_prefix")
    assert len(geo) == 11 and 59000 not in geo.index
    assert -24 < geo.loc[1310, "lat"] < -23  # its point in Europe was left out
    customers = pd.read_parquet(out / "customers").set_index("customer_zip_code_prefix")
    assert customers.loc[[59000], "customer_lat"].isna().all()

    # orders_fact: 60 orders in, 59 in the table, 1 set aside (delivered before bought).
    fact = pd.read_parquet(out / "orders_fact").set_index("order_id")
    rejected = pd.read_parquet(out / "orders_fact_rejected")
    assert len(fact) == 59
    assert list(rejected["order_id"]) == [order_id(9)]
    assert list(rejected["_ubunye_failed_rules"]) == ["delivery_days_between"]
    checks = _results(records["orders_fact"], "orders_fact")
    assert checks["rows_from_orders"]["passed"]
    assert (
        "60 rows read from orders, 60 reached orders_fact" in checks["rows_from_orders"]["detail"]
    )
    assert checks["items_price_sum_from_order_items"]["passed"]
    assert checks["payments_total_sum_from_payments"]["passed"]

    # orders_fact: the named cases.
    assert fact.loc[order_id(2), "delivered_on_time"] == False  # noqa: E712
    assert fact.loc[order_id(4), "items"] == 0  # canceled, no items, still a row
    assert pd.isna(fact.loc[order_id(5), "delivery_days"])  # shipped, not delivered
    assert fact.loc[order_id(6), "reviews"] == 2
    assert fact.loc[order_id(6), "review_score"] == 4  # the later review
    assert pd.isna(fact.loc[order_id(7), "review_score"])  # no review
    assert fact.loc[order_id(3), "sellers"] == 2 and fact.loc[order_id(3), "payments"] == 2

    # orders_fact: paid more or less than items and freight by over a real: a warning.
    gap = checks["payment_gap_between"]
    assert gap["severity"] == "warn" and gap["failed"] == 4
    gaps = fact[fact["payment_gap"].abs() > 1]["payment_gap"].round(2).to_dict()
    assert gaps == {
        order_id(4): 45.9,  # canceled after it was paid
        order_id(8): 3.2,  # instalment interest
        order_id(10): 24.84,  # the free item's freight: that item was set aside
        order_id(12): -267.23,  # its payment (pix) was set aside
    }

    # monthly: one row per seller and month, and a rate between 0 and 1.
    sellers = pd.read_parquet(out / "seller_monthly")
    assert not sellers.duplicated(["seller_id", "purchase_year", "purchase_month"]).any()
    assert sellers["on_time_rate"].between(0, 1).all()
    categories = pd.read_parquet(out / "category_monthly")
    assert "unknown" in set(categories["category"])


@pytest.mark.parametrize("step", STEPS)
def test_an_old_narwhals_gets_the_message_the_tutorial_promises(example, monkeypatch, step):
    """Narwhals before 2.9 has no Expr.floor; say so, not "'Expr' has no attribute"."""
    monkeypatch.delattr(nw.Expr, "floor")
    with pytest.raises(ImportError, match=r"the example needs narwhals>=2\.9 \(Expr\.floor\)"):
        ubunye.run_task(_task(example, step), backend="pandas")


def test_a_raw_file_that_changes_type_stops_the_run_before_the_transform(example):
    """One purchase time written the day-first way turns the column to text."""
    orders = example / "sample" / "olist_orders_dataset.csv"
    lines = orders.read_text(encoding="utf-8").splitlines(keepends=True)
    first = lines[1].split(",")
    first[3] = "18/02/2018 10:59"  # order_purchase_timestamp, as a spreadsheet may save it
    lines[1] = ",".join(first)
    orders.write_text("".join(lines), encoding="utf-8")

    with pytest.raises(ExpectationError) as err:
        ubunye.run_task(_task(example, "clean"), backend="pandas", lineage=True)

    assert "order_purchase_timestamp: expected timestamp, found string" in str(err.value)
    assert "transform did not run" in str(err.value)
    assert not (example / "output").exists()


def test_a_join_that_loses_orders_stops_the_run_and_writes_nothing(example):
    """The mistake the reconcile is for: an inner join drops orders with no items."""
    ubunye.run_task(_task(example, "clean"), backend="pandas")
    code = Path(_task(example, "orders_fact")) / "transformations.py"
    text = code.read_text(encoding="utf-8")
    broken = text.replace(
        '.join(per_order_items, on="order_id", how="left")',
        '.join(per_order_items, on="order_id", how="inner")',
    )
    assert broken != text
    code.write_text(broken, encoding="utf-8")

    with pytest.raises(ExpectationError) as err:
        ubunye.run_task(_task(example, "orders_fact"), backend="pandas")

    assert "60 rows read from orders, 59 reached orders_fact: 1 lost" in str(err.value)
    assert not (example / "output" / "orders_fact").exists()
