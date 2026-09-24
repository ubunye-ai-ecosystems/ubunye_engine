"""``CONFIG.expectations``: declared checks on outputs, run before anything is written.

The unit tier runs them on pandas (no Java); the Spark tier runs the same rules on
Spark in tests/integration/test_expectations_spark.py and must agree.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from pydantic import ValidationError

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

import ubunye  # noqa: E402
from ubunye.config.schema import ExpectationRule, ExpectationSet, UbunyeConfig  # noqa: E402
from ubunye.core import expectations  # noqa: E402
from ubunye.core.errors import ExpectationError, TransformOutputError  # noqa: E402

ROWS = pd.DataFrame(
    {
        "id": [1, 2, 3, 3, None],
        "qty": [1, 0, 5, 2, 1],
        "method": ["visa", "cash", None, "visa", "amex"],
        "card": ["4111", "41", "4111", "4111", None],
    }
)


def rule(**kw) -> ExpectationRule:
    return ExpectationRule(**kw)


def results_by_rule(results):
    return {r.rule: r for r in results}


# --- the rules, one kind at a time -----------------------------------------------


def test_each_kind_counts_the_rows_that_break_it():
    spec = ExpectationSet(
        rules=[
            rule(not_null="id"),
            rule(between={"column": "qty", "min": 1}),
            rule(one_of={"column": "method", "values": ["visa", "amex"]}),
            rule(matches={"column": "card", "pattern": r"^\d{4}$"}),
            rule(unique="id"),
            rule(row_count={"min": 1, "max": 10}),
        ]
    )
    _, _, results = expectations.check_output("out", ROWS, spec)
    found = {r.rule: r.failed for r in results}
    assert found == {
        "id_not_null": 1,
        "qty_between": 1,  # qty 0
        "method_one_of": 1,  # cash; the null passes
        "card_matches": 1,  # "41"; the null passes
        "id_unique": 2,  # two rows share id 3
        "row_count": 0,
    }
    assert all(r.total == 5 for r in results)


def test_a_null_passes_every_kind_but_not_null():
    frame = pd.DataFrame({"x": [None, None]}, dtype="float64")
    spec = ExpectationSet(
        rules=[
            rule(between={"column": "x", "min": 0, "max": 1}, severity="warn"),
            rule(one_of={"column": "x", "values": [1.0]}, severity="warn"),
            rule(not_null="x", severity="warn"),
        ]
    )
    _, _, results = expectations.check_output("out", frame, spec)
    assert {r.rule: r.failed for r in results} == {
        "x_between": 0,
        "x_one_of": 0,
        "x_not_null": 2,
    }


def test_row_count_outside_its_range_fails_once():
    spec = ExpectationSet(rules=[rule(row_count={"min": 10})])
    _, _, results = expectations.check_output("out", ROWS, spec)
    assert results[0].failed == 1 and not results[0].passed


# --- what a breach does -------------------------------------------------------------


def test_a_fail_rule_raises_with_every_result_and_the_counts():
    spec = ExpectationSet(rules=[rule(not_null="id"), rule(between={"column": "qty", "max": 9})])
    with pytest.raises(ExpectationError) as err:
        expectations.apply({"out": ROWS}, {"out": spec})
    assert "nothing was written" in str(err.value)
    assert "out: id_not_null (not_null) broken by 1 of 5 rows" in str(err.value)
    assert [r["rule"] for r in err.value.results] == ["id_not_null", "qty_between"]
    assert err.value.results[1]["passed"] is True


def test_quarantine_moves_breaking_rows_and_names_every_rule_they_broke():
    spec = ExpectationSet(
        quarantine="bad",
        rules=[
            rule(between={"column": "qty", "min": 1}, severity="quarantine"),
            rule(matches={"column": "card", "pattern": r"^\d{4}$"}, severity="quarantine"),
            rule(one_of={"column": "method", "values": ["visa", "amex"]}, severity="quarantine"),
        ],
    )
    out, results = expectations.apply({"out": ROWS}, {"out": spec})
    assert len(out["out"]) == 4
    bad = out["bad"]
    assert list(bad["id"]) == [2]
    # Row 2 broke three rules at once: one row, one bug report, three reasons.
    assert bad[expectations.FAILED_RULES_COLUMN].iloc[0] == "qty_between,card_matches,method_one_of"
    assert all(not r.passed for r in results)


def test_no_quarantined_row_still_writes_an_empty_quarantine_output():
    clean = ROWS[ROWS["qty"] > 0]
    spec = ExpectationSet(
        quarantine="bad", rules=[rule(between={"column": "qty", "min": 1}, severity="quarantine")]
    )
    out, _ = expectations.apply({"out": clean}, {"out": spec})
    assert len(out["bad"]) == 0
    assert expectations.FAILED_RULES_COLUMN in out["bad"].columns


def test_too_much_quarantined_is_a_failure():
    spec = ExpectationSet(
        quarantine="bad",
        max_quarantine_rate=0.1,
        rules=[rule(between={"column": "qty", "min": 1}, severity="quarantine")],
    )
    with pytest.raises(ExpectationError, match=r"1 of 5 rows quarantined \(20.0%\)"):
        expectations.apply({"out": ROWS}, {"out": spec})


def test_warn_reports_and_keeps_every_row(caplog):
    spec = ExpectationSet(rules=[rule(not_null="id", severity="warn")])
    with caplog.at_level(logging.WARNING):
        out, results = expectations.apply({"out": ROWS}, {"out": spec})
    assert len(out["out"]) == 5
    assert not results[0].passed
    assert "id_not_null" in caplog.text


def test_the_transform_cannot_fill_the_quarantine_output_itself():
    spec = ExpectationSet(quarantine="bad", rules=[rule(not_null="id", severity="quarantine")])
    with pytest.raises(TransformOutputError, match="quarantine output"):
        expectations.apply({"out": ROWS, "bad": ROWS}, {"out": spec})


# --- config validation --------------------------------------------------------------


@pytest.mark.parametrize(
    "kw, message",
    [
        ({}, "exactly one of"),
        ({"not_null": "a", "unique": ["a"]}, "exactly one of"),
        ({"unique": "a", "severity": "quarantine"}, "cannot quarantine"),
        ({"row_count": {"min": 1}, "severity": "quarantine"}, "cannot quarantine"),
        ({"between": {"column": "a"}}, "needs 'min', 'max'"),
        ({"matches": {"column": "a", "pattern": "("}}, "not a valid regular expression"),
        ({"not_null": "a", "severity": "loud"}, "severity"),
    ],
)
def test_a_badly_written_rule_is_refused(kw, message):
    with pytest.raises(ValidationError, match=message):
        ExpectationRule(**kw)


def test_a_rule_is_named_after_its_column_and_kind_unless_named():
    assert rule(not_null="id").name == "id_not_null"
    assert rule(unique=["a", "b"]).name == "a_b_unique"
    assert rule(row_count={"min": 1}).name == "row_count"
    assert rule(not_null="id", name="has_id").name == "has_id"


def test_a_quarantine_rule_needs_a_quarantine_output():
    with pytest.raises(ValidationError, match="needs 'quarantine"):
        ExpectationSet(rules=[rule(not_null="id", severity="quarantine")])


def _config(expectations_block, outputs=("out", "bad")):
    return {
        "CONFIG": {
            "inputs": {"src": {"format": "s3", "path": "in.csv", "file_format": "csv"}},
            "outputs": {o: {"format": "s3", "path": o, "file_format": "parquet"} for o in outputs},
            "expectations": expectations_block,
        }
    }


@pytest.mark.parametrize(
    "block, message",
    [
        ({"nope": {"rules": [{"not_null": "id"}]}}, "no output named 'nope'"),
        (
            {
                "out": {
                    "quarantine": "gone",
                    "rules": [{"not_null": "id", "severity": "quarantine"}],
                }
            },
            "no output named 'gone'",
        ),
        (
            {"out": {"quarantine": "out", "rules": [{"not_null": "id", "severity": "quarantine"}]}},
            "cannot quarantine into itself",
        ),
    ],
)
def test_expectations_must_name_real_outputs(block, message):
    with pytest.raises(ValidationError, match=message):
        UbunyeConfig(**_config(block))


def test_a_config_with_expectations_dumps_and_reads_back():
    block = {"out": {"quarantine": "bad", "rules": [{"not_null": "id", "severity": "quarantine"}]}}
    cfg = UbunyeConfig(**_config(block))
    UbunyeConfig(**cfg.model_dump(mode="json"))


# --- end to end, through the engine, on the pandas backend ---------------------------

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"orders": sources["raw"]}
"""


def _task(root: Path, expectations_yaml: str) -> Path:
    task = root / "uc" / "pkg" / "orders"
    task.mkdir(parents=True)
    (root / "raw.csv").write_text("id,qty\n1,2\n2,0\n3,5\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    raw:
      format: s3
      path: "{(root / 'raw.csv').as_posix()}"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  outputs:
    orders:
      format: s3
      path: "{(root / 'out' / 'orders').as_posix()}"
      file_format: parquet
      mode: overwrite
    orders_quarantine:
      format: s3
      path: "{(root / 'out' / 'orders_quarantine').as_posix()}"
      file_format: parquet
      mode: overwrite
  expectations:
{expectations_yaml}
""",
        encoding="utf-8",
    )
    return task


def test_a_failed_run_writes_nothing(tmp_path):
    task = _task(
        tmp_path,
        """\
    orders:
      rules:
        - between: {column: qty, min: 1}
""",
    )
    with pytest.raises(ExpectationError):
        ubunye.run_task(str(task), backend="pandas")
    assert not (tmp_path / "out").exists()


def test_a_quarantining_run_writes_clean_and_quarantined_rows(tmp_path):
    task = _task(
        tmp_path,
        """\
    orders:
      quarantine: orders_quarantine
      rules:
        - between: {column: qty, min: 1}
          severity: quarantine
        - not_null: id
""",
    )
    ubunye.run_task(str(task), backend="pandas")
    clean = pd.read_parquet(tmp_path / "out" / "orders")
    bad = pd.read_parquet(tmp_path / "out" / "orders_quarantine")
    assert sorted(clean["id"]) == [1, 3]
    assert list(bad["id"]) == [2]
    assert list(bad[expectations.FAILED_RULES_COLUMN]) == ["qty_between"]
