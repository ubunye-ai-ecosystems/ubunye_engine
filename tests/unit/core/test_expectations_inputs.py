"""Input contracts: expectations on an input, checked before the transform (F-018).

A source that wrote ``price`` as text made pandas compute ``qty * price`` as
"11.011.0", and the run succeeded. Expectations may now name an input; its rules
run right after it is read, and a ``columns`` rule checks each column's type by the
run record's names. The unit tier runs them on pandas; the Spark tier
(tests/integration/test_expectations_spark.py) checks the same names on Spark.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

import ubunye  # noqa: E402
from ubunye.adapters import pandas_io  # noqa: E402
from ubunye.config.schema import ExpectationRule, ExpectationSet, UbunyeConfig  # noqa: E402
from ubunye.core import expectations  # noqa: E402
from ubunye.core.errors import ExpectationError  # noqa: E402
from ubunye.lineage import content_hash  # noqa: E402

ORDERS = pd.DataFrame(
    {
        "order_id": pd.array([1, 2, 3], dtype="int64"),
        "qty": pd.array([1, 2, 3], dtype="int64"),
        "price": [10.0, 11.0, 12.0],
    }
)


def contract(**rule) -> ExpectationSet:
    return ExpectationSet(rules=[rule])


def check(frame, spec):
    return expectations.check_inputs({"orders": frame}, {"orders": spec})


# --- the type names: the run record's, timestamps by what they are -----------------


@pytest.mark.parametrize(
    "frame",
    [
        ORDERS,
        pd.DataFrame({"s": ["a", None], "b": [True, False]}),
        pd.DataFrame({"c": pd.Categorical(["x", "y"]), "i8": pd.array([1, 2], dtype="int8")}),
        pandas_io.to_pandas(
            pa.table(
                {
                    "i": pa.array([1, None], pa.int32()),
                    "d": pa.array([None, None], pa.decimal128(10, 2)),
                    "l": pa.array([[1], None], pa.list_(pa.int64())),
                    "day": pa.array([None, None], pa.date32()),
                }
            )
        ),
    ],
)
def test_the_type_names_are_the_ones_the_run_record_writes(frame):
    written = pandas_io.to_arrow(frame, "UTC")
    kinds = {f.name: content_hash.arrow_kind(f.type) for f in written.schema}
    assert content_hash.frame_kinds(frame) == kinds


def test_a_column_of_mixed_python_values_is_named_not_guessed():
    assert content_hash.frame_kinds(pd.DataFrame({"m": [1, "a"]})) == {"m": "mixed"}


def test_timestamps_are_named_by_what_they_are_at_every_depth():
    # Skeptic review 4: pandas called naive and zoned timestamps both "timestamp",
    # so a source switching between them passed; Spark 3.4+ tells them apart.
    table = pa.table(
        {
            "naive": pa.array([None], pa.timestamp("us")),
            "zoned": pa.array([None], pa.timestamp("us", tz="UTC")),
            "naive_list": pa.array([None], pa.list_(pa.timestamp("us"))),
            "zoned_list": pa.array([None], pa.list_(pa.timestamp("us", tz="UTC"))),
        }
    )
    expected = {
        "naive": "timestamp_ntz",
        "zoned": "timestamp",
        "naive_list": "list<timestamp_ntz>",
        "zoned_list": "list<timestamp>",
    }
    assert content_hash.frame_kinds(table) == expected
    assert content_hash.frame_kinds(pandas_io.to_pandas(table)) == expected
    numpy_frame = pd.DataFrame({"t": pd.to_datetime(["2024-01-01"])})
    assert content_hash.frame_kinds(numpy_frame) == {"t": "timestamp_ntz"}
    with pytest.raises(ExpectationError, match="t: expected timestamp, found timestamp_ntz"):
        check(numpy_frame, contract(columns={"t": "timestamp"}))


def test_the_run_record_still_names_every_pandas_timestamp_an_instant():
    # The contract's names changed; the record's (and so its schema hash) did not.
    frame = pd.DataFrame({"t": pd.to_datetime(["2024-01-01"])})
    written = pandas_io.to_arrow(frame, "UTC")
    assert content_hash.arrow_kind(written.schema.field("t").type) == "timestamp"


def test_every_kind_a_frame_reports_can_be_declared():
    # Skeptic review 5: with extra: forbid no column may be undeclarable.
    table = pa.table(
        {
            "u8": pa.array([1], pa.uint8()),
            "tm": pa.array([None], pa.time64("us")),
            "du": pa.array([None], pa.duration("ms")),
            "n": pa.array([None], pa.null()),
            "fb": pa.array([b"ab"], pa.binary(2)),
        }
    )
    kinds = content_hash.frame_kinds(table)
    rule = ExpectationRule(columns=kinds, extra="forbid")
    assert check(table.to_pandas(types_mapper=pd.ArrowDtype), contract(**rule.model_dump()))[
        0
    ].passed
    assert ExpectationRule(columns={"m": "mixed"}).columns == {"m": ["mixed"]}


# --- the E-05 case: price retyped to text ------------------------------------------


def test_a_retyped_column_is_named_with_expected_and_found_type():
    spec = contract(columns={"order_id": "int64", "qty": "int64", "price": "float64"})
    retyped = ORDERS.assign(price=ORDERS["price"].astype(str))
    with pytest.raises(ExpectationError) as err:
        check(retyped, spec)
    message = str(err.value)
    assert "the transform did not run and nothing was written" in message
    assert "orders: columns (columns): price: expected float64, found string" in message
    [found] = err.value.results
    assert (found["side"], found["kind"], found["failed"], found["total"]) == (
        "input",
        "columns",
        1,
        3,
    )


def test_the_right_shape_passes_and_is_reported():
    [found] = check(ORDERS, contract(columns={"qty": "int64", "price": "float64"}))
    assert found.passed and found.failed == 0 and found.side == "input"


def test_a_missing_column_is_named():
    with pytest.raises(ExpectationError, match="price: expected float64, missing"):
        check(ORDERS.drop(columns=["price"]), contract(columns={"price": "float64"}))


def test_types_are_strict_and_a_list_accepts_more_than_one():
    narrow = ORDERS.astype({"qty": "int32"})
    with pytest.raises(ExpectationError, match="qty: expected int64, found int32"):
        check(narrow, contract(columns={"qty": "int64"}))
    assert check(narrow, contract(columns={"qty": ["int32", "int64"]}))[0].passed


def test_an_int_column_that_became_float_is_caught():
    # E-05's "qty int to float": pandas turns an int column with a gap into floats.
    with pytest.raises(ExpectationError, match="qty: expected int64, found float64"):
        check(ORDERS.astype({"qty": "float64"}), contract(columns={"qty": "int64"}))


def test_nulls_are_not_part_of_the_type():
    arrow = pandas_io.to_pandas(pa.table({"qty": pa.array([1, None], pa.int64())}))
    assert check(arrow, contract(columns={"qty": "int64"}))[0].passed


def test_extra_columns_are_allowed_unless_forbidden():
    wider = ORDERS.assign(channel="web")
    assert check(wider, contract(columns={"qty": "int64"}))[0].passed
    with pytest.raises(ExpectationError, match="channel: not expected .*found string"):
        check(wider, contract(columns={"qty": "int64"}, extra="forbid"))


def test_a_columns_only_contract_reads_no_rows(monkeypatch):
    def no_rows(*a, **k):
        raise AssertionError("a columns rule must not read rows")

    monkeypatch.setattr(expectations, "_scalars", no_rows)
    check(ORDERS, contract(columns={"qty": "int64"}))


def test_a_wrong_shape_stops_before_the_other_rules_run():
    spec = ExpectationSet(
        rules=[
            {"columns": {"price": "float64"}},
            {"between": {"column": "price", "min": 0}},  # would fail on text
        ]
    )
    with pytest.raises(ExpectationError) as err:
        check(ORDERS.assign(price=["a", "b", "c"]), spec)
    assert "price: expected float64, found string" in str(err.value)
    assert [r["rule"] for r in err.value.results] == ["columns"]


def test_other_rules_run_on_an_input_too():
    spec = ExpectationSet(
        rules=[{"columns": {"qty": "int64"}}, {"between": {"column": "qty", "min": 2}}]
    )
    with pytest.raises(ExpectationError, match="orders: qty_between .* broken by 1 of 3 rows"):
        check(ORDERS, spec)


def test_a_warn_contract_reports_and_goes_on(caplog):
    [found] = check(
        ORDERS.astype({"qty": "int32"}), contract(columns={"qty": "int64"}, severity="warn")
    )
    assert not found.passed
    assert "qty: expected int64, found int32" in caplog.text


# --- the config ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "rule, message",
    [
        ({"columns": {"price": "double"}}, "write float64"),
        ({"columns": {"qty": "long"}}, "write int64"),
        ({"columns": {"qty": "int63"}}, "did you mean int64"),
        # Skeptic review 5: nested names are read part by part.
        ({"columns": {"qty": "list<banana>"}}, "'banana' is not a type name"),
        ({"columns": {"qty": "map<string,long>"}}, "write int64"),
        ({"columns": {"qty": "struct<a>"}}, "name:type"),
        # Skeptic review 3: a bare TypeError used to escape to `ubunye validate`.
        ({"columns": {"qty": 5}}, "give a type name like int64"),
        ({"columns": {"qty": [5]}}, "a type is a name like int64"),
        ({"columns": {}}, "at least one column"),
        ({"columns": {"qty": []}}, "give 'qty' a type"),
        ({"not_null": "qty", "extra": "forbid"}, "'extra' goes with a 'columns' rule"),
        ({"columns": {"qty": "int64"}, "extra": "maybe"}, "extra"),
        ({"columns": {"qty": "int64"}, "severity": "quarantine"}, "cannot quarantine"),
    ],
)
def test_a_badly_written_columns_rule_is_refused(rule, message):
    with pytest.raises(ValidationError, match=message):
        ExpectationRule(**rule)


def test_type_names_are_read_leniently_and_stored_canonically():
    r = ExpectationRule(
        columns={
            "a": " Int64 ",
            "b": "Decimal(10, 2)",
            "c": ["List<INT64>"],
            "d": "struct<Name: String, when: list<Timestamp_NTZ>>",
            "e": "map<string, decimal(38, 2)>",
        }
    )
    assert r.columns == {
        "a": ["int64"],
        "b": ["decimal(10,2)"],
        "c": ["list<int64>"],
        "d": ["struct<Name:string,when:list<timestamp_ntz>>"],
        "e": ["map<string,decimal(38,2)>"],
    }
    assert r.name == "columns" and r.kind == "columns"


def _config(block, inputs=("orders",), outputs=("enriched",)):
    return {
        "CONFIG": {
            "inputs": {
                i: {"format": "s3", "path": f"{i}.csv", "file_format": "csv"} for i in inputs
            },
            "outputs": {o: {"format": "s3", "path": o, "file_format": "parquet"} for o in outputs},
            "expectations": block,
        }
    }


COLUMNS = {"rules": [{"columns": {"qty": "int64"}}]}


@pytest.mark.parametrize(
    "block, inputs, outputs, message",
    [
        ({"same": COLUMNS}, ("same",), ("same",), "'same' is both an input and an output"),
        (
            {"orders": {**COLUMNS, "quarantine": "enriched"}},
            ("orders",),
            ("enriched",),
            "an input cannot quarantine rows",
        ),
        (
            {"orders": {"reconcile": [{"input": "orders", "rows": {"max_lost": 0}}]}},
            ("orders",),
            ("enriched",),
            "reconcile goes on an output",
        ),
        ({"order": COLUMNS}, ("orders",), ("enriched",), "no input or output named 'order'"),
    ],
)
def test_the_config_says_what_an_input_contract_cannot_be(block, inputs, outputs, message):
    with pytest.raises(ValidationError, match=message):
        UbunyeConfig(**_config(block, inputs, outputs))


def test_a_config_with_an_input_contract_dumps_and_reads_back():
    block = {
        "orders": {
            "rules": [{"columns": {"qty": "int64", "price": ["float64"]}, "extra": "forbid"}]
        }
    }
    cfg = UbunyeConfig(**_config(block))
    UbunyeConfig(**cfg.model_dump(mode="json"))


@pytest.mark.parametrize(
    "columns, code, says",
    [
        ({"qty": "int64"}, 0, "[OK]"),
        ({"qty": "double"}, 1, "write float64"),
    ],
)
def test_ubunye_validate_accepts_an_input_contract_and_refuses_a_bad_one(
    tmp_path, columns, code, says
):
    import yaml
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    cfg = {
        "MODEL": "etl",
        "VERSION": "0.1.0",
        **_config({"orders": {"rules": [{"columns": columns}]}}),
    }
    (task / "config.yaml").write_text(yaml.dump(cfg), encoding="utf-8")
    result = CliRunner().invoke(
        app, ["validate", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t"]
    )
    assert result.exit_code == code
    assert says in re.sub(r"\x1b\[[0-9;]*m", "", result.output)


# --- end to end, through the engine, on the pandas backend -------------------------

TRANSFORM = """\
from pathlib import Path

from ubunye.core.interfaces import Task


class Enrich(Task):
    def transform(self, sources):
        Path(__file__).with_name("transform_ran").write_text("yes")
        orders = sources["orders"]
        orders["total"] = orders["qty"] * orders["price"]
        return {"enriched": orders}
"""


def _task(root: Path, orders: "pd.DataFrame") -> Path:
    task = root / "uc" / "pkg" / "enrich"
    task.mkdir(parents=True)
    orders.to_parquet(root / "orders.parquet")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    orders: {{format: s3, path: "{(root / 'orders.parquet').as_posix()}", file_format: parquet}}
  outputs:
    enriched:
      format: s3
      path: "{(root / 'out').as_posix()}"
      file_format: parquet
      mode: overwrite
  expectations:
    orders:
      rules:
        - columns: {{order_id: int64, qty: int64, price: float64}}
""",
        encoding="utf-8",
    )
    return task


def test_a_retyped_source_stops_the_run_before_the_transform(tmp_path):
    task = _task(tmp_path, ORDERS.assign(price=ORDERS["price"].astype(str)))
    lineage = tmp_path / "lineage"
    with pytest.raises(ExpectationError, match="price: expected float64, found string"):
        ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    assert not (task / "transform_ran").exists()
    assert not (tmp_path / "out").exists()

    [record] = [json.loads(p.read_text(encoding="utf-8")) for p in lineage.rglob("*.json")]
    assert record["status"] == "error"
    [found] = record["expectations"]
    assert (found["output"], found["side"], found["passed"]) == ("orders", "input", False)
    assert [t["step"].split(":")[0] for t in record["timings"]] == ["Reader"]


def test_a_source_of_the_right_shape_runs_and_the_record_keeps_the_check(tmp_path):
    task = _task(tmp_path, ORDERS)
    lineage = tmp_path / "lineage"
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    assert list(pd.read_parquet(tmp_path / "out")["total"]) == [10.0, 22.0, 36.0]
    [record] = [json.loads(p.read_text(encoding="utf-8")) for p in lineage.rglob("*.json")]
    assert [(e["output"], e.get("side"), e["passed"]) for e in record["expectations"]] == [
        ("orders", "input", True)
    ]


def test_the_notebook_checks_the_contract_when_it_reads(tmp_path):
    from ubunye.notebook import notebook

    task = _task(tmp_path, ORDERS.assign(price=ORDERS["price"].astype(str)))
    session = notebook(str(task), backend="pandas")
    try:
        with pytest.raises(ExpectationError, match="price: expected float64, found string"):
            session.read()
    finally:
        session.close()
    assert not (task / "transform_ran").exists()


def test_the_notebook_record_keeps_the_input_checks(tmp_path):
    from ubunye.notebook import notebook

    task = _task(tmp_path, ORDERS)
    lineage = tmp_path / "lineage"
    session = notebook(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    try:
        session.run()
    finally:
        session.close()
    [record] = [json.loads(p.read_text(encoding="utf-8")) for p in lineage.rglob("*.json")]
    assert [e.get("side") for e in record["expectations"]] == ["input"]
