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
