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
