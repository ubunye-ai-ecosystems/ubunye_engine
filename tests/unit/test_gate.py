"""``ubunye gate``: a run's receipt against a baseline, rule by rule."""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.core.gate import FAIL, OK, WARN, Policy, evaluate, markdown, passed
from ubunye.lineage.context import RunContext, StepRecord

runner = CliRunner()
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _run(**over):
    ctx = RunContext(
        run_id="aaaaaaaa-0000-4000-8000-000000000001",
        task_path="shop/orders/clean",
        usecase="shop",
        package="orders",
        task_name="clean",
        profile="default",
        model="etl",
        version="1.0.0",
        config_hash="sha256:cfg",
        started_at="2026-09-24T10:00:00Z",
        duration_sec=10.0,
        status="success",
        code_hash="sha256:code",
        environment_hash="sha256:env",
    )
    ctx.inputs = [StepRecord("raw", "input", "s3", "in.csv", 3, "sha256:s", "sha256:in", "rows-v1")]
    ctx.outputs = [
        StepRecord("clean", "output", "s3", "out", 3, "sha256:s", "sha256:out", "rows-v1")
    ]
    for key, value in over.items():
        setattr(ctx, key, value)
    return ctx


def _changed_output(ctx, data_hash="sha256:other", schema_hash=None, rows=None):
    ctx = copy.deepcopy(ctx)
    ctx.run_id = "bbbbbbbb-0000-4000-8000-000000000002"
    out = ctx.outputs[0]
    out.data_hash = data_hash
    if schema_hash:
        out.schema_hash = schema_hash
    if rows is not None:
        out.row_count = rows
    return ctx


def _by_rule(findings):
    return {(f.rule, f.output): f for f in findings}


# --- the rules ------------------------------------------------------------------------


def test_the_same_receipt_passes():
    findings = evaluate(_run(), _run())
    assert passed(findings)
    assert _by_rule(findings)[("data", "clean")].detail == "identical"


def test_changed_data_without_a_version_bump_fails_and_says_it_is_nondeterministic():
    findings = evaluate(_run(), _changed_output(_run()))
    data = _by_rule(findings)[("data", "clean")]
    assert data.status == FAIL
    assert "without a VERSION bump" in data.detail and "not deterministic" in data.detail
    assert not passed(findings)


def test_the_gate_names_what_else_changed():
    cand = _changed_output(_run(code_hash="sha256:new-code"))
    cand.inputs[0].data_hash = "sha256:new-input"
    detail = _by_rule(evaluate(_run(), cand))[("data", "clean")].detail
    assert "changed: code, input raw" in detail


def test_a_version_bump_makes_a_data_change_deliberate():
    cand = _changed_output(_run(version="1.1.0"), schema_hash="sha256:new-schema")
    findings = evaluate(_run(), cand)
    assert passed(findings)
    assert _by_rule(findings)[("data", "clean")].detail == "changed with VERSION 1.0.0 -> 1.1.0"
    assert _by_rule(findings)[("schema", "clean")].status == OK


def test_allow_data_change_warns_instead():
    findings = evaluate(_run(), _changed_output(_run()), Policy(allow_data_change=True))
    assert passed(findings) and _by_rule(findings)[("data", "clean")].status == WARN


def test_a_schema_change_without_a_bump_fails():
    cand = _changed_output(_run(), data_hash="sha256:out", schema_hash="sha256:new-schema")
    assert _by_rule(evaluate(_run(), cand))[("schema", "clean")].status == FAIL


def test_a_missing_output_fails_and_a_new_one_warns():
    cand = copy.deepcopy(_run())
    cand.outputs = [StepRecord("extra", "output", "s3", "x", 1, "s", "d", "rows-v1")]
    found = _by_rule(evaluate(_run(), cand))
    assert found[("output", "clean")].status == FAIL
    assert found[("output", "extra")].status == WARN


def test_a_failed_run_or_a_fail_expectation_fails():
    broken = {
        "output": "clean",
        "rule": "id_not_null",
        "severity": "fail",
        "failed": 1,
        "total": 3,
        "passed": False,
    }
    assert not passed(evaluate(_run(), _run(status="error", error="boom")))
    assert not passed(evaluate(_run(), _run(expectations=[broken])))
    warned = dict(broken, severity="warn")
    assert passed(evaluate(_run(), _run(expectations=[warned])))


def test_hashes_made_differently_cannot_be_compared():
    cand = copy.deepcopy(_run())
    cand.outputs[0].hash_method = None
    finding = _by_rule(evaluate(_run(), cand))[("data", "clean")]
    assert finding.status == WARN and "cannot compare" in finding.detail


@pytest.mark.parametrize(
    "base_s, cand_s, policy, fails",
    [
        (10.0, 16.0, Policy(max_slowdown=0.5), True),
        (10.0, 14.0, Policy(max_slowdown=0.5), False),
        (0.2, 0.9, Policy(max_slowdown=0.5), False),  # under the noise floor
        (10.0, 31.0, Policy(max_seconds=30), True),
    ],
)
def test_time_limits(base_s, cand_s, policy, fails):
    findings = evaluate(_run(duration_sec=base_s), _run(duration_sec=cand_s), policy)
    assert passed(findings) is not fails


def test_row_change_limit():
    cand = _changed_output(_run(version="1.1.0"), rows=4)
    assert not passed(evaluate(_run(), cand, Policy(max_row_change=0.1)))
    assert passed(evaluate(_run(), cand, Policy(max_row_change=0.5)))


def test_the_markdown_summary_has_a_row_per_finding():
    findings = evaluate(_run(), _changed_output(_run()))
    table = markdown(findings, _run(), _changed_output(_run()))
    assert table.startswith("### Ubunye gate fails")
    assert table.count("\n|") == len(findings) + 2


# --- the command, on real runs ---------------------------------------------------------

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id\n1\n2\n3\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
VERSION: "1.0.0"
CONFIG:
  inputs:
    src: {{format: s3, path: "{(root / 'in.csv').as_posix()}", file_format: csv, options: {{header: "true"}}}}
  outputs:
    out: {{format: s3, path: "{(root / 'out').as_posix()}", file_format: parquet, mode: overwrite}}
""",
        encoding="utf-8",
    )
    return task


def _gate(root: Path, *extra):
    args = ["gate", "-d", str(root), "-u", "uc", "-p", "pkg", "-t", "t", *extra]
    return runner.invoke(app, args)


def test_two_identical_runs_pass(tmp_path):
    task = _task(tmp_path)
    for _ in range(2):
        ubunye.run_task(str(task), backend="pandas", lineage=True)
    result = _gate(tmp_path)
    assert result.exit_code == 0, result.output
    assert "[OK] data out: identical" in ANSI.sub("", result.output)


def test_a_changed_input_fails_the_gate_and_says_why(tmp_path):
    task = _task(tmp_path)
    ubunye.run_task(str(task), backend="pandas", lineage=True)
    (tmp_path / "in.csv").write_text("id\n1\n2\n3\n4\n", encoding="utf-8")
    ubunye.run_task(str(task), backend="pandas", lineage=True)
    summary = tmp_path / "summary.md"
    result = _gate(tmp_path, "--json", "--summary", str(summary))
    assert result.exit_code == 1
    doc = json.loads(result.output)
    [data] = [f for f in doc["findings"] if f["rule"] == "data"]
    assert data["status"] == FAIL and "changed: input src" in data["detail"]
    assert "### Ubunye gate fails" in summary.read_text(encoding="utf-8")


def test_run_records_as_files(tmp_path):
    task = _task(tmp_path)
    ubunye.run_task(str(task), backend="pandas", lineage=True)
    shown = runner.invoke(
        app, ["lineage", "show", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t", "--json"]
    )
    record = tmp_path / "base.json"
    record.write_text(shown.output, encoding="utf-8")
    result = runner.invoke(app, ["gate", "--baseline", str(record), "--candidate", str(record)])
    assert result.exit_code == 0, result.output


def test_a_run_id_needs_the_task(tmp_path):
    result = runner.invoke(app, ["gate", "--baseline", "abc", "--candidate", "def", "--json"])
    assert result.exit_code == 1
    assert "give -d -u -p -t" in json.loads(result.output)["error"]
