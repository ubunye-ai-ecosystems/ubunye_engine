"""``reconcile``: rows and totals must carry over from an input to an output (F-017).

An inner join that drops the orders whose customer is unknown used to succeed; the
record showed 1,000 in and 900 out, and nothing could stop the run. A reconcile is
checked with the other expectations, before anything is written. The unit tier runs
it on pandas; tests/integration/test_expectations_spark.py runs the same checks on
Spark and must agree.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

import ubunye  # noqa: E402
from ubunye.config.schema import ExpectationSet, UbunyeConfig  # noqa: E402
from ubunye.core import expectations  # noqa: E402
from ubunye.core.errors import ExpectationError  # noqa: E402

ORDERS = pd.DataFrame(
    {
        "order_id": range(10),
        "customer_id": [1, 2, 3, 4, 5, 6, 7, 8, 99, 99],  # 99 is unknown
        "amount": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
    }
)
CUSTOMERS = pd.DataFrame({"customer_id": range(1, 9), "city": ["jhb", "cpt"] * 4})
JOINED = ORDERS.merge(CUSTOMERS, on="customer_id", how="inner")  # 8 of 10 orders


def spec(**check) -> ExpectationSet:
    return ExpectationSet(reconcile=[{"input": "orders", **check}])


def run(out, s, inputs=None):
    return expectations.apply({"enriched": out}, {"enriched": s}, inputs or {"orders": ORDERS})


def by_rule(results):
    return {r.rule: r for r in results}


# --- rows ---------------------------------------------------------------------------


def test_an_inner_join_that_drops_orders_stops_the_run():
    with pytest.raises(ExpectationError) as err:
        run(JOINED, spec(rows={"max_lost": 0}))
    message = str(err.value)
    assert "nothing was written" in message
    assert "rows_from_orders" in message
    assert "10 rows read from orders, 8 reached enriched: 2 lost" in message
    found = err.value.results[0]
    assert found["kind"] == "reconcile" and found["failed"] == 2 and found["total"] == 10
    assert found["passed"] is False


def test_every_row_carried_over_passes_and_is_still_reported():
    _, results = run(ORDERS, spec(rows={"max_lost": 0}))
    r = by_rule(results)["rows_from_orders"]
    assert r.passed and r.failed == 0 and r.total == 10


@pytest.mark.parametrize("bound, passed", [("20%", True), ("19.9%", False), (2, True), (1, False)])
def test_max_lost_takes_rows_or_a_share_of_the_input(bound, passed):
    s = spec(rows={"max_lost": bound}, severity="warn")
    _, results = run(JOINED, s)
    assert by_rule(results)["rows_from_orders"].passed is passed


def test_max_gained_catches_a_join_that_fans_out():
    doubled = pd.concat([ORDERS, ORDERS.head(3)])
    with pytest.raises(ExpectationError, match="3 gained"):
        run(doubled, spec(rows={"max_gained": 0}))
    # max_lost alone does not look at gains.
    _, results = run(doubled, spec(rows={"max_lost": 0}))
    assert by_rule(results)["rows_from_orders"].passed


def test_quarantined_rows_count_as_carried_over():
    s = ExpectationSet(
        quarantine="bad",
        rules=[{"between": {"column": "amount", "max": 80}, "severity": "quarantine"}],
        reconcile=[{"input": "orders", "rows": {"max_lost": 0}}],
    )
    out, results = run(ORDERS, s)
    assert len(out["enriched"]) == 8 and len(out["bad"]) == 2
    assert by_rule(results)["rows_from_orders"].passed


def test_warn_reports_and_writes(caplog):
    _, results = run(JOINED, spec(rows={"max_lost": 0}, severity="warn"))
    r = by_rule(results)["rows_from_orders"]
    assert not r.passed and r.severity == "warn"
    assert "2 lost" in caplog.text


# --- sums ---------------------------------------------------------------------------


def test_a_sum_that_does_not_carry_over_fails_and_says_by_how_much():
    with pytest.raises(ExpectationError) as err:
        run(JOINED, spec(sum={"column": "amount"}))
    assert "sum of amount in orders 550, of amount in enriched 360: difference -190" in str(
        err.value
    )


@pytest.mark.parametrize("tolerance, passed", [(190, True), (189.9, False), ("35%", True)])
def test_sum_tolerance_is_absolute_or_a_share_of_the_input_sum(tolerance, passed):
    _, results = run(
        JOINED, spec(sum={"column": "amount", "tolerance": tolerance}, severity="warn")
    )
    assert by_rule(results)["amount_sum_from_orders"].passed is passed


def test_the_input_column_may_have_another_name():
    renamed = ORDERS.rename(columns={"amount": "value"})
    _, results = run(renamed, spec(sum={"column": "value", "input_column": "amount"}))
    r = by_rule(results)["value_sum_from_orders"]
    assert r.passed and r.column == "value"


def test_nan_and_null_are_left_out_of_a_sum_on_both_sides():
    inp = ORDERS.assign(amount=[1.0, None, float("nan")] + [1.0] * 7)
    out = inp.assign(amount=[1.0, 1.0] + [1.0] * 6 + [None, None])
    _, results = run(out, spec(sum={"column": "amount"}), {"orders": inp})
    assert by_rule(results)["amount_sum_from_orders"].passed


def test_integer_sums_compare_exactly():
    inp = pd.DataFrame({"n": [2**53, 1]})
    out = pd.DataFrame({"n": [2**53]})
    with pytest.raises(ExpectationError, match="difference -1"):
        expectations.apply({"o": out}, {"o": spec(sum={"column": "n"})}, {"orders": inp})


def test_a_sum_of_text_or_a_missing_column_is_one_clear_error():
    with pytest.raises(ExpectationError, match="sums column 'city'.*not a number"):
        run(JOINED, spec(sum={"column": "city", "input_column": "amount"}))
    with pytest.raises(ExpectationError, match="input orders: reconcile names column 'nope'"):
        run(JOINED, spec(sum={"column": "amount", "input_column": "nope"}))


def test_each_input_is_counted_once_however_many_outputs_reconcile_with_it(monkeypatch):
    calls = []
    real = expectations._scalars

    def counting(frame, exprs):
        calls.append(len(exprs))
        return real(frame, exprs)

    monkeypatch.setattr(expectations, "_scalars", counting)
    s = spec(rows={"max_lost": 10}, sum={"column": "amount", "tolerance": "100%"})
    expectations.apply({"a": JOINED, "b": ORDERS}, {"a": s, "b": s}, {"orders": ORDERS})
    assert len(calls) == 3  # one pass over the input, one over each output


def test_without_the_input_frames_a_reconcile_says_so():
    with pytest.raises(ExpectationError, match="reconcile needs the input frames"):
        expectations.apply({"enriched": JOINED}, {"enriched": spec(rows={"max_lost": 0})})


# --- the config ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "check, message",
    [
        ({}, "needs 'rows', 'sum' or both"),
        ({"rows": {}}, "needs 'max_lost', 'max_gained'"),
        ({"rows": {"max_lost": -1}}, "cannot be negative"),
        ({"rows": {"max_lost": "ten"}}, "percentage like '1%'"),
        ({"rows": {"max_lost": 0.5}}, "number of rows"),
        ({"sum": {"column": "a", "tolerance": "0.1"}}, "percentage like '1%'"),
        ({"rows": {"max_lost": 0}, "severity": "quarantine"}, "severity"),
        ({"rows": {"max_lost": 0}, "colour": "red"}, "colour"),
    ],
)
def test_a_badly_written_reconcile_is_refused(check, message):
    with pytest.raises(ValidationError, match=message):
        spec(**check)


def _config(block):
    return {
        "CONFIG": {
            "inputs": {"orders": {"format": "s3", "path": "in.csv", "file_format": "csv"}},
            "outputs": {"enriched": {"format": "s3", "path": "o", "file_format": "parquet"}},
            "expectations": block,
        }
    }


def test_a_reconcile_must_name_a_real_input():
    block = {"enriched": {"reconcile": [{"input": "order", "rows": {"max_lost": 0}}]}}
    with pytest.raises(ValidationError, match="there is no input named 'order'"):
        UbunyeConfig(**_config(block))


def test_a_config_with_a_reconcile_dumps_and_reads_back():
    block = {
        "enriched": {
            "reconcile": [
                {
                    "input": "orders",
                    "rows": {"max_lost": "1%"},
                    "sum": {"column": "amount", "tolerance": 0.01},
                }
            ]
        }
    }
    cfg = UbunyeConfig(**_config(block))
    again = UbunyeConfig(**cfg.model_dump(mode="json"))
    check = again.CONFIG.expectations["enriched"].reconcile[0]
    assert check.rows.max_lost == "1%" and check.sum.tolerance == 0.01


@pytest.mark.parametrize(
    "check, code, says",
    [
        ({"input": "orders", "rows": {"max_lost": "1%"}}, 0, "[OK]"),
        ({"input": "order", "rows": {"max_lost": 0}}, 1, "there is no input named 'order'"),
        ({"input": "orders", "rows": {"max_lost": "lots"}}, 1, "percentage like '1%'"),
    ],
)
def test_ubunye_validate_accepts_a_reconcile_and_refuses_a_bad_one(tmp_path, check, code, says):
    import re

    import yaml
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    cfg = {"MODEL": "etl", "VERSION": "0.1.0", **_config({"enriched": {"reconcile": [check]}})}
    (task / "config.yaml").write_text(yaml.dump(cfg), encoding="utf-8")
    result = CliRunner().invoke(
        app, ["validate", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t"]
    )
    assert result.exit_code == code
    assert says in re.sub(r"\x1b\[[0-9;]*m", "", result.output)


def test_reconcile_names_must_not_clash_with_rule_names():
    with pytest.raises(ValidationError, match="repeated: rows_from_orders"):
        ExpectationSet(
            rules=[{"row_count": {"min": 1}, "name": "rows_from_orders"}],
            reconcile=[{"input": "orders", "rows": {"max_lost": 0}}],
        )


# --- end to end, through the engine, on the pandas backend -------------------------

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Enrich(Task):
    def transform(self, sources):
        joined = sources["orders"].merge(sources["customers"], on="customer_id", how="inner")
        return {"enriched": joined}
"""


def _task(root: Path, reconcile_yaml: str) -> Path:
    task = root / "uc" / "pkg" / "enrich"
    task.mkdir(parents=True)
    ORDERS.to_parquet(root / "orders.parquet")
    CUSTOMERS.to_parquet(root / "customers.parquet")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    orders: {{format: s3, path: "{(root / 'orders.parquet').as_posix()}", file_format: parquet}}
    customers:
      format: s3
      path: "{(root / 'customers.parquet').as_posix()}"
      file_format: parquet
  outputs:
    enriched:
      format: s3
      path: "{(root / 'out').as_posix()}"
      file_format: parquet
      mode: overwrite
  expectations:
    enriched:
      reconcile:
{reconcile_yaml}
""",
        encoding="utf-8",
    )
    return task


def test_a_run_that_loses_orders_writes_nothing_and_the_record_says_why(tmp_path):
    task = _task(
        tmp_path,
        """\
        - input: orders
          rows: {max_lost: 0}
          sum: {column: amount, tolerance: 0.01}
""",
    )
    lineage = tmp_path / "lineage"
    with pytest.raises(ExpectationError, match="2 lost"):
        ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    assert not (tmp_path / "out").exists()

    records = [json.loads(p.read_text(encoding="utf-8")) for p in lineage.rglob("*.json")]
    found = [e for r in records for e in r.get("expectations") or []]
    assert {e["rule"]: e["passed"] for e in found} == {
        "rows_from_orders": False,
        "amount_sum_from_orders": False,
    }
    assert any("2 lost" in (e.get("detail") or "") for e in found)


def test_a_run_within_tolerance_writes(tmp_path):
    task = _task(
        tmp_path,
        """\
        - input: orders
          rows: {max_lost: 20%}
""",
    )
    ubunye.run_task(str(task), backend="pandas")
    assert len(pd.read_parquet(tmp_path / "out")) == 8


def test_the_notebook_path_reconciles_with_the_frames_it_read(tmp_path):
    from ubunye.notebook import notebook

    task = _task(
        tmp_path,
        """\
        - input: orders
          rows: {max_lost: 0}
""",
    )
    session = notebook(str(task), backend="pandas")
    try:
        with pytest.raises(ExpectationError, match="2 lost"):
            session.run()
    finally:
        session.close()
    assert not (tmp_path / "out").exists()
