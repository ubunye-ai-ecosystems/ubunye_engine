"""``reconcile``: rows and totals must carry over from an input to an output (F-017).

An inner join that drops the orders whose customer is unknown used to succeed; the
record showed 1,000 in and 900 out, and nothing could stop the run. A reconcile is
checked with the other expectations, before anything is written. The unit tier runs
it on pandas; tests/integration/test_expectations_spark.py runs the same checks on
Spark and must agree.
"""

from __future__ import annotations

import decimal
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
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
    real = expectations._exact_sums

    def counting(frame, columns):
        calls.append(len(frame))
        return real(frame, columns)

    monkeypatch.setattr(expectations, "_exact_sums", counting)
    s = spec(rows={"max_lost": 10}, sum={"column": "amount", "tolerance": "100%"})
    expectations.apply({"a": JOINED, "b": ORDERS}, {"a": s, "b": s}, {"orders": ORDERS})
    assert sorted(calls) == [8, 10, 10]  # the input once, then each output's sum


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


def _task(root: Path, reconcile_yaml: str, transform: str = TRANSFORM) -> Path:
    task = root / "uc" / "pkg" / "enrich"
    task.mkdir(parents=True)
    ORDERS.to_parquet(root / "orders.parquet")
    CUSTOMERS.to_parquet(root / "customers.parquet")
    (task / "transformations.py").write_text(transform, encoding="utf-8")
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


# --- skeptic review (fix/f017-f018-contracts) ----------------------------------------

IN_PLACE_DROP = """\
from ubunye.core.interfaces import Task


class Enrich(Task):
    def transform(self, sources):
        orders = sources["orders"]
        orders.drop(orders[orders.customer_id == 99].index, inplace=True)
        return {"enriched": orders}
"""

IN_PLACE_ZERO = """\
from ubunye.core.interfaces import Task


class Enrich(Task):
    def transform(self, sources):
        orders = sources["orders"]
        orders["amount"] = orders["amount"] * 0
        return {"enriched": orders}
"""


@pytest.mark.parametrize(
    "transform, says",
    [
        (IN_PLACE_DROP, "10 rows read from orders, 8 reached enriched: 2 lost"),
        (IN_PLACE_ZERO, "difference -550"),
    ],
)
def test_a_transform_that_changes_its_input_in_place_cannot_hide_the_loss(
    tmp_path, transform, says
):
    # 1: the input was counted after the transform, so this said "8 read, 0 lost".
    task = _task(
        tmp_path,
        """\
        - input: orders
          rows: {max_lost: 0}
          sum: {column: amount}
""",
        transform,
    )
    with pytest.raises(ExpectationError, match=says):
        ubunye.run_task(str(task), backend="pandas")
    assert not (tmp_path / "out").exists()


def test_the_notebook_counts_the_input_before_an_in_place_transform(tmp_path):
    from ubunye.notebook import notebook

    task = _task(
        tmp_path, "        - input: orders\n          rows: {max_lost: 0}\n", IN_PLACE_DROP
    )
    session = notebook(str(task), backend="pandas")
    try:
        session.read()
        with pytest.raises(ExpectationError, match="10 rows read from orders, 8 reached"):
            session.write(session.transform())
    finally:
        session.close()


def test_decimal_sums_are_exact():
    # 2: compared as floats, 0.01 on a 1.2e19 total vanished at tolerance 0.
    dec = pd.ArrowDtype(pa.decimal128(38, 2))
    values = [decimal.Decimal("0.10")] * 3 + [decimal.Decimal("12345678901234567890.01")]
    before = pd.DataFrame({"amount": pd.array(values, dtype=dec)})
    after = before.copy()
    after.loc[0, "amount"] = decimal.Decimal("0.11")
    with pytest.raises(ExpectationError) as err:
        run(after, spec(sum={"column": "amount"}), {"orders": before})
    assert (
        "sum of amount in orders 12345678901234567890.31, of amount in enriched "
        "12345678901234567890.32: difference 0.01 (at most 0)" in str(err.value)
    )
    _, results = run(after, spec(sum={"column": "amount", "tolerance": 0.01}), {"orders": before})
    assert results[0].passed


@pytest.mark.parametrize("dtype", ["int64", pd.ArrowDtype(pa.int64())])
def test_integer_sums_do_not_wrap_past_2_to_the_63(dtype):
    # 7: 3 x 2**62 wrapped to a negative number on both sides.
    big = pd.DataFrame({"amount": pd.array([2**62] * 3, dtype=dtype)})
    with pytest.raises(ExpectationError) as err:
        run(big.head(2), spec(sum={"column": "amount"}), {"orders": big})
    assert f"orders {3 * 2**62}, of amount in enriched {2 * 2**62}: difference {-(2**62)}" in str(
        err.value
    )
    _, results = run(big, spec(sum={"column": "amount"}), {"orders": big})
    assert results[0].passed


def test_equal_infinite_totals_match():
    # 8: inf - inf is NaN, which failed a sum that carried over exactly.
    frame = pd.DataFrame({"amount": [1.0, float("inf")]})
    _, results = run(frame, spec(sum={"column": "amount"}), {"orders": frame})
    assert results[0].passed
    _, results = run(
        frame.head(1), spec(sum={"column": "amount"}, severity="warn"), {"orders": frame}
    )
    assert not results[0].passed


def test_two_named_checks_against_one_input_can_coexist():
    # 8: a warn at 1% and a fail at 25% on the same input clashed by name.
    s = ExpectationSet(
        reconcile=[
            {"input": "orders", "name": "early", "rows": {"max_lost": "1%"}, "severity": "warn"},
            {"input": "orders", "name": "stop", "rows": {"max_lost": "25%"}},
        ]
    )
    _, results = run(JOINED, s)
    assert {r.rule: r.passed for r in results} == {"early_rows": False, "stop_rows": True}


@pytest.mark.parametrize(
    "check, message",
    [
        ({"sum": {"column": "a", "tolerance": None}}, "tolerance is empty"),
        ({"sum": {"column": "a", "tolerance": float("nan")}}, "finite number"),
        ({"sum": {"column": "a", "tolerance": float("inf")}}, "finite number"),
        ({"sum": {"column": "a", "tolerance": [1]}}, r"percentage like '1%', not \[1\]"),
        ({"rows": {"max_lost": [1]}}, r"percentage like '1%', not \[1\]"),
        ({"rows": {"max_lost": {"a": 1}}}, "percentage like '1%'"),
    ],
)
def test_a_bad_bound_is_a_clear_config_error_not_a_type_error(check, message):
    # 3: these escaped `ubunye validate` as a bare TypeError.
    with pytest.raises(ValidationError, match=message):
        spec(**check)


def test_ubunye_validate_names_a_blank_tolerance(tmp_path):
    import yaml
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    block = {
        "enriched": {"reconcile": [{"input": "orders", "sum": {"column": "a", "tolerance": None}}]}
    }
    cfg = {"MODEL": "etl", "VERSION": "0.1.0", **_config(block)}
    (task / "config.yaml").write_text(yaml.dump(cfg), encoding="utf-8")
    result = CliRunner().invoke(
        app, ["validate", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t"]
    )
    assert result.exit_code == 1
    assert not isinstance(result.exception, TypeError)
    assert "tolerance is empty" in result.output


def test_the_notebook_reconciles_against_the_frames_the_transform_got(tmp_path):
    # 6: a sample passed to transform() was compared with the full read.
    from ubunye.notebook import notebook

    task = _task(tmp_path, "        - input: orders\n          rows: {max_lost: 0}\n", COPY)
    session = notebook(str(task), backend="pandas")
    try:
        session.read()
        sample = {"orders": ORDERS.head(4).copy(), "customers": CUSTOMERS.copy()}
        session.write(session.transform(sample))
    finally:
        session.close()
    assert len(pd.read_parquet(tmp_path / "out")) == 4


def test_the_notebook_says_what_to_call_when_it_has_no_inputs(tmp_path):
    from ubunye.config.loader import load_config
    from ubunye.core import backends
    from ubunye.core.runtime import Engine

    task = _task(tmp_path, "        - input: orders\n          rows: {max_lost: 0}\n")
    cfg = load_config(str(task / "config.yaml")).model_dump(mode="json")
    engine = Engine(backend=backends.resolve("pandas"))
    with pytest.raises(ExpectationError, match=r"call read\(\) or transform\(\) before write"):
        engine.write_outputs({"enriched": ORDERS}, cfg)


COPY = """\
from ubunye.core.interfaces import Task


class Enrich(Task):
    def transform(self, sources):
        return {"enriched": sources["orders"].copy()}
"""
