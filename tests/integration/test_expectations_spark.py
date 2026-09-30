"""Expectations give the same verdicts on Spark as on pandas.

The rules are evaluated with Narwhals on whatever frame the transform returned,
so the same config must count the same broken rows, quarantine the same rows
with the same reasons, and refuse the same runs on both engines.
"""

from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

from pyspark.sql import SparkSession  # noqa: E402

from ubunye.config.schema import ExpectationSet  # noqa: E402
from ubunye.core import expectations  # noqa: E402
from ubunye.core.errors import ExpectationError  # noqa: E402

pytestmark = pytest.mark.integration

ROWS = [
    (1, 1, "visa", "4111"),
    (2, 0, "cash", "41"),
    (3, 5, None, "4111"),
    (3, 2, "visa", "4111"),
    (None, 1, "amex", None),
]
COLUMNS = ["id", "qty", "method", "card"]

RULES = [
    {"not_null": "id", "severity": "warn"},
    {"between": {"column": "qty", "min": 1}, "severity": "quarantine"},
    {"one_of": {"column": "method", "values": ["visa", "amex"]}, "severity": "quarantine"},
    {"matches": {"column": "card", "pattern": r"^\d{4}$"}, "severity": "quarantine"},
    {"unique": "id", "severity": "warn"},
    {"row_count": {"min": 1, "max": 10}},
]


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def _both(spark):
    pandas_frame = pd.DataFrame(ROWS, columns=COLUMNS)
    spark_frame = spark.createDataFrame(
        pandas_frame.astype(object).where(pandas_frame.notna(), None)
    )
    return pandas_frame, spark_frame


def test_the_same_rules_count_the_same_rows_on_spark_and_pandas(spark):
    spec = ExpectationSet(rules=RULES, quarantine="bad")
    pandas_frame, spark_frame = _both(spark)
    _, _, on_pandas = expectations.check_output("out", pandas_frame, spec)
    _, _, on_spark = expectations.check_output("out", spark_frame, spec)
    assert [r.as_dict() for r in on_spark] == [r.as_dict() for r in on_pandas]
    assert {r.rule: r.failed for r in on_spark} == {
        "id_not_null": 1,
        "qty_between": 1,
        "method_one_of": 1,
        "card_matches": 1,
        "id_unique": 2,
        "row_count": 0,
    }


def test_quarantine_splits_the_same_rows_with_the_same_reasons(spark):
    spec = ExpectationSet(rules=RULES, quarantine="bad")
    pandas_frame, spark_frame = _both(spark)
    out_pandas, _ = expectations.apply({"out": pandas_frame}, {"out": spec})
    out_spark, _ = expectations.apply({"out": spark_frame}, {"out": spec})

    def rows(frame):
        native = frame.toPandas() if hasattr(frame, "toPandas") else frame
        return sorted(
            tuple(None if pd.isna(v) else v for v in row)
            for row in native.astype(object).itertuples(index=False)
        )

    assert out_spark["out"].count() == len(out_pandas["out"]) == 4
    assert rows(out_spark["bad"]) == rows(out_pandas["bad"])
    assert rows(out_spark["bad"])[0][-1] == "qty_between,method_one_of,card_matches"


def test_a_fail_rule_refuses_the_run_on_spark_too(spark):
    _, spark_frame = _both(spark)
    spec = ExpectationSet(rules=[{"not_null": "id"}])
    with pytest.raises(ExpectationError, match="broken by 1 of 5 rows"):
        expectations.apply({"out": spark_frame}, {"out": spec})


def test_an_empty_quarantine_output_on_spark_has_the_reason_column(spark):
    _, spark_frame = _both(spark)
    clean = spark_frame.filter("qty > 0")
    spec = ExpectationSet(
        rules=[{"between": {"column": "qty", "min": 1}, "severity": "quarantine"}],
        quarantine="bad",
    )
    out, _ = expectations.apply({"out": clean}, {"out": spec})
    assert out["bad"].count() == 0
    assert expectations.FAILED_RULES_COLUMN in out["bad"].columns


def test_rules_are_counted_on_spark_without_pyarrow(spark, monkeypatch):
    """A Spark image need not have pyarrow (F-036).

    `ubunye deploy dockerfile dataproc` and `container` install the engine without it,
    and R1's expectations failed on Dataproc, Kubernetes and Container Apps with
    "No module named 'pyarrow'": Narwhals collects a Spark result through Arrow unless
    told otherwise. The counts must come from Spark itself.
    """
    spec = ExpectationSet(rules=RULES, quarantine="bad")
    pandas_frame, spark_frame = _both(spark)
    _, _, on_pandas = expectations.check_output("out", pandas_frame, spec)
    for name in ("pyarrow", "narwhals._arrow.dataframe", "narwhals._arrow.series"):
        monkeypatch.setitem(__import__("sys").modules, name, None)
    _, _, on_spark = expectations.check_output("out", spark_frame, spec)
    assert [r.as_dict() for r in on_spark] == [r.as_dict() for r in on_pandas]


def test_nan_counts_as_missing_on_spark_as_on_pandas(spark):
    # F-045: Spark keeps NaN apart from null and orders it above every number, so
    # without the rule not_null counted 1 (pandas 2) and between 2 (pandas 1).
    rows = [(1, 1.0), (2, float("nan")), (3, 5.0), (4, 2.0), (5, None)]
    spec = ExpectationSet(
        rules=[
            {"not_null": "qty", "severity": "warn"},
            {"between": {"column": "qty", "min": 1, "max": 4}, "severity": "warn"},
            {"one_of": {"column": "qty", "values": [1.0, 2.0]}, "severity": "warn"},
        ]
    )
    frames = {
        "pandas": pd.DataFrame(rows, columns=["id", "qty"]),
        "spark": spark.createDataFrame(rows, "id INT, qty DOUBLE"),
    }
    counts = {
        name: {r.rule: r.failed for r in expectations.check_output("out", f, spec)[2]}
        for name, f in frames.items()
    }
    assert (
        counts["spark"]
        == counts["pandas"]
        == {
            "qty_not_null": 2,
            "qty_between": 1,
            "qty_one_of": 1,
        }
    )


# --- reconcile (F-017): rows and totals carried from an input to an output ----------

ORDERS = [
    (0, 1, 10.0),
    (1, 2, float("nan")),
    (2, 3, None),
    (3, 99, 40.0),  # an unknown customer: the inner join drops it
    (4, 99, 50.0),
    (5, 1, 60.0),
]
CUSTOMERS = [(1, "jhb"), (2, "cpt"), (3, "dbn")]
RECONCILE = [
    {"input": "orders", "rows": {"max_lost": 0}, "severity": "warn"},
    {"input": "orders", "sum": {"column": "amount", "tolerance": 1}, "severity": "warn"},
]


def _orders_and_join(spark, engine):
    if engine == "pandas":
        orders = pd.DataFrame(ORDERS, columns=["order_id", "customer_id", "amount"])
        customers = pd.DataFrame(CUSTOMERS, columns=["customer_id", "city"])
        return orders, orders.merge(customers, on="customer_id", how="inner")
    orders = spark.createDataFrame(ORDERS, "order_id INT, customer_id INT, amount DOUBLE")
    customers = spark.createDataFrame(CUSTOMERS, "customer_id INT, city STRING")
    return orders, orders.join(customers, "customer_id", "inner")


def test_reconcile_gives_the_same_results_on_spark_and_pandas(spark):
    spec = ExpectationSet(reconcile=RECONCILE)
    found = {}
    for engine in ("pandas", "spark"):
        orders, joined = _orders_and_join(spark, engine)
        _, results = expectations.apply(
            {"enriched": joined}, {"enriched": spec}, {"orders": orders}
        )
        found[engine] = [r.as_dict() for r in results]
    assert found["spark"] == found["pandas"]
    rows, total = found["spark"]
    assert (rows["failed"], rows["total"], rows["passed"]) == (2, 6, False)
    # NaN and null are left out of both sums: 160 read, 70 reached.
    assert "difference -90" in total["detail"] and total["passed"] is False


def test_a_reconcile_refuses_the_run_on_spark_too(spark):
    orders, joined = _orders_and_join(spark, "spark")
    spec = ExpectationSet(reconcile=[{"input": "orders", "rows": {"max_lost": 0}}])
    with pytest.raises(ExpectationError, match="6 rows read from orders, 4 reached"):
        expectations.apply({"enriched": joined}, {"enriched": spec}, {"orders": orders})


def test_a_task_that_drops_orders_writes_nothing_on_spark(spark, tmp_path):
    import textwrap

    import ubunye

    root = tmp_path.as_posix()
    _orders_and_join(spark, "spark")[0].write.parquet(f"{root}/orders")
    spark.createDataFrame(CUSTOMERS, "customer_id INT, city STRING").write.parquet(
        f"{root}/customers"
    )
    task = tmp_path / "uc" / "pkg" / "enrich"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(textwrap.dedent(f"""\
        CONFIG:
          inputs:
            orders: {{format: s3, path: "{root}/orders", file_format: parquet}}
            customers: {{format: s3, path: "{root}/customers", file_format: parquet}}
          outputs:
            enriched: {{format: s3, path: "{root}/out", file_format: parquet, mode: overwrite}}
          expectations:
            enriched:
              reconcile:
                - input: orders
                  rows: {{max_lost: 0}}
        """))
    (task / "transformations.py").write_text(textwrap.dedent("""\
        from ubunye.core.interfaces import Task


        class Enrich(Task):
            def transform(self, sources):
                joined = sources["orders"].join(sources["customers"], "customer_id", "inner")
                return {"enriched": joined}
        """))
    with pytest.raises(ExpectationError, match="2 lost"):
        ubunye.run_task(str(task), spark=spark)
    assert not (tmp_path / "out").exists()


# --- input contracts (F-018): column types by the run record's names ---------------

TYPED = (
    "i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, f32 FLOAT, f64 DOUBLE, b BOOLEAN, "
    "s STRING, bin BINARY, d DATE, t TIMESTAMP, tntz TIMESTAMP_NTZ, dec DECIMAL(10,2), "
    "l ARRAY<BIGINT>, lntz ARRAY<TIMESTAMP_NTZ>, "
    "st STRUCT<a: INT, b: STRING>"
)


def test_spark_and_pandas_name_the_same_parquet_columns_the_same_way(spark, tmp_path):
    import datetime as dt
    from decimal import Decimal

    from ubunye.adapters import pandas_io
    from ubunye.lineage.content_hash import frame_kinds

    row = (
        1,
        2,
        3,
        4,
        1.5,
        2.5,
        True,
        "x",
        bytearray(b"a"),
        dt.date(2024, 1, 2),
        dt.datetime(2024, 1, 2, 10, 15),
        dt.datetime(2024, 1, 2, 10, 15),
        Decimal("1.25"),
        [1],
        [dt.datetime(2024, 1, 2, 10, 15)],
        (1, "a"),
    )
    path = (tmp_path / "typed").as_posix()
    spark.createDataFrame([row], TYPED).write.parquet(path)
    on_spark = frame_kinds(spark.read.parquet(path))
    on_pandas = frame_kinds(pandas_io.read_frame("parquet", path).native)
    assert on_spark == on_pandas
    assert on_spark["i32"] == "int32" and on_spark["dec"] == "decimal(10,2)"
    assert on_spark["st"] == "struct<a:int32,b:string>"
    assert (on_spark["t"], on_spark["tntz"]) == ("timestamp", "timestamp_ntz")
    assert on_spark["lntz"] == "list<timestamp_ntz>"


def test_a_parquet_file_with_naive_and_zoned_timestamps_is_named_alike(spark, tmp_path):
    # Skeptic review 4: a naive timestamp is timestamp_ntz and a zoned one timestamp,
    # on Spark (3.4 and later read a naive parquet timestamp as TIMESTAMP_NTZ) and on
    # pandas, at the top level and inside a list.
    import datetime as dt

    import pyarrow as pa
    import pyarrow.parquet as pq

    from ubunye.adapters import pandas_io
    from ubunye.lineage.content_hash import frame_kinds

    when = dt.datetime(2024, 1, 2, 10, 15)
    table = pa.table(
        {
            "naive": pa.array([when], pa.timestamp("us")),
            "zoned": pa.array([when], pa.timestamp("us", tz="UTC")),
            "naive_list": pa.array([[when]], pa.list_(pa.timestamp("us"))),
        }
    )
    (tmp_path / "ts").mkdir()
    pq.write_table(table, tmp_path / "ts" / "part-0.parquet")
    path = (tmp_path / "ts").as_posix()
    on_spark = frame_kinds(spark.read.parquet(path))
    on_pandas = frame_kinds(pandas_io.read_frame("parquet", path).native)
    assert (
        on_spark
        == on_pandas
        == {
            "naive": "timestamp_ntz",
            "zoned": "timestamp",
            "naive_list": "list<timestamp_ntz>",
        }
    )


def test_reconcile_sums_are_exact_on_spark_as_on_pandas(spark):
    # Skeptic review 2 and 7: integer totals past 2**63 and decimal totals of 20 digits.
    from decimal import Decimal

    rows = [(2**62, Decimal("0.10")), (2**62, Decimal("12345678901234567890.01")), (2**62, None)]
    orders = spark.createDataFrame(rows, "n BIGINT, amount DECIMAL(38,2)")
    kept = orders.limit(2)
    spec = ExpectationSet(
        reconcile=[
            {"input": "orders", "sum": {"column": "n"}, "severity": "warn"},
            {"input": "orders", "sum": {"column": "amount"}, "severity": "warn", "name": "amt"},
        ]
    )
    _, on_spark = expectations.apply({"out": kept}, {"out": spec}, {"orders": orders})
    frame = pd.DataFrame(rows, columns=["n", "amount"])
    frame["amount"] = frame["amount"].astype(pd.ArrowDtype(pa_decimal(38, 2)))
    _, on_pandas = expectations.apply({"out": frame.head(2)}, {"out": spec}, {"orders": frame})
    assert [r.as_dict() for r in on_spark] == [r.as_dict() for r in on_pandas]
    assert f"orders {3 * 2**62}, of n in out {2 * 2**62}" in on_spark[0].detail
    assert "12345678901234567890.11" in on_spark[1].detail and on_spark[1].passed


def pa_decimal(precision, scale):
    import pyarrow as pa

    return pa.decimal128(precision, scale)


def test_a_columns_contract_gives_the_same_verdict_on_spark_and_pandas(spark):
    spec = ExpectationSet(
        rules=[{"columns": {"id": "int64", "qty": "float64", "extra_col": "string"}}]
    )
    rows = [(1, "1.5"), (2, None)]
    frames = {
        "pandas": pd.DataFrame(rows, columns=["id", "qty"]),
        "spark": spark.createDataFrame(rows, "id BIGINT, qty STRING"),
    }
    found = {}
    for engine, frame in frames.items():
        with pytest.raises(ExpectationError) as err:
            expectations.check_inputs({"orders": frame}, {"orders": spec})
        found[engine] = err.value.results
    assert found["spark"] == found["pandas"]
    assert found["spark"][0]["detail"] == (
        "qty: expected float64, found string; extra_col: expected string, missing"
    )


def test_a_retyped_source_stops_a_spark_task_before_the_transform(spark, tmp_path):
    import textwrap

    import ubunye

    root = tmp_path.as_posix()
    spark.createDataFrame(
        [(1, 2, "11.0")], "order_id BIGINT, qty BIGINT, price STRING"
    ).write.parquet(f"{root}/orders")
    task = tmp_path / "uc" / "pkg" / "enrich"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(textwrap.dedent(f"""\
        CONFIG:
          inputs:
            orders: {{format: s3, path: "{root}/orders", file_format: parquet}}
          outputs:
            enriched: {{format: s3, path: "{root}/out", file_format: parquet, mode: overwrite}}
          expectations:
            orders:
              rules:
                - columns: {{order_id: int64, qty: int64, price: float64}}
        """))
    (task / "transformations.py").write_text(textwrap.dedent("""\
        from pathlib import Path

        from ubunye.core.interfaces import Task


        class Enrich(Task):
            def transform(self, sources):
                Path(__file__).with_name("transform_ran").write_text("yes")
                orders = sources["orders"]
                return {"enriched": orders.withColumn("total", orders.qty * orders.price)}
        """))
    with pytest.raises(ExpectationError, match="price: expected float64, found string"):
        ubunye.run_task(str(task), spark=spark)
    assert not (task / "transform_ran").exists()
    assert not (tmp_path / "out").exists()
