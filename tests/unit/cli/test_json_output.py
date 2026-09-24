"""``--json``: machine output a script or an agent can trust.

With ``--json`` the whole of stdout is one JSON document, errors included, and
the exit code still says whether the command succeeded. Each test parses the
entire stdout, so one stray print breaks it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.cli.main import app  # noqa: E402

runner = CliRunner()

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path, name: str = "copy", src: str = "") -> Path:
    task = root / "uc" / "pkg" / name
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id\n1\n2\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{src or (root / "in.csv").as_posix()}"
      file_format: csv
      options:
        header: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / "out").as_posix()}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


def _json(result):
    return json.loads(result.stdout)  # all of stdout, not a line of it


def _where(root: Path, *extra: str):
    return ["-d", str(root), "-u", "uc", "-p", "pkg", *extra]


class TestPlan:
    def test_a_clean_plan(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(app, ["plan", *_where(tmp_path, "-t", "copy", "--json")])
        doc = _json(result)
        assert result.exit_code == 0 and doc["ok"] is True
        (task,) = doc["tasks"]
        assert task["task"] == "uc/pkg/copy" and task["transform"]["class"] == "Copy"
        assert task["config_hash"].startswith("sha256:")

    def test_a_problem_is_json_and_exit_1(self, tmp_path):
        _task(tmp_path, src=(tmp_path / "missing.csv").as_posix())
        result = runner.invoke(app, ["plan", *_where(tmp_path, "-t", "copy", "--json")])
        doc = _json(result)
        assert result.exit_code == 1 and doc["ok"] is False
        assert "nothing at" in doc["tasks"][0]["problems"][0]

    def test_a_config_error_is_json_too(self, tmp_path):
        task = _task(tmp_path)
        (task / "config.yaml").write_text("MODEL: etl\nVERSION: [\n", encoding="utf-8")
        result = runner.invoke(app, ["plan", *_where(tmp_path, "-t", "copy", "--json")])
        doc = _json(result)
        assert result.exit_code == 1 and doc["ok"] is False and doc["error"]


class TestValidate:
    def test_ok(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(app, ["validate", *_where(tmp_path, "-t", "copy", "--json")])
        doc = _json(result)
        assert result.exit_code == 0
        assert doc == {"ok": True, "tasks": [{"task": "copy", "ok": True, "problems": []}]}

    def test_a_backend_problem(self, tmp_path):
        task = _task(tmp_path)
        text = (task / "config.yaml").read_text(encoding="utf-8")
        (task / "config.yaml").write_text(
            text.replace("mode: overwrite", "mode: merge\n      merge_keys: [id]"), encoding="utf-8"
        )
        result = runner.invoke(
            app, ["validate", *_where(tmp_path, "-t", "copy", "--backend", "pandas", "--json")]
        )
        doc = _json(result)
        assert result.exit_code == 1 and doc["ok"] is False
        assert "'merge'" in doc["tasks"][0]["problems"][0]


@pytest.fixture
def two_runs(tmp_path):
    task = _task(tmp_path)
    ubunye.run_task(str(task), backend="pandas", lineage=True)
    ubunye.run_task(str(task), backend="pandas", lineage=True)
    return tmp_path


class TestLineage:
    def test_list(self, two_runs):
        result = runner.invoke(app, ["lineage", "list", *_where(two_runs, "-t", "copy", "--json")])
        runs = _json(result)
        assert result.exit_code == 0 and len(runs) == 2
        assert {"run_id", "status", "started_at", "outputs"} <= set(runs[0])

    def test_show(self, two_runs):
        result = runner.invoke(app, ["lineage", "show", *_where(two_runs, "-t", "copy", "--json")])
        assert _json(result)["backend"] == "pandas"

    def test_compare(self, two_runs):
        a, b = _json(
            runner.invoke(app, ["lineage", "list", *_where(two_runs, "-t", "copy", "--json")])
        )
        result = runner.invoke(
            app,
            [
                "lineage",
                "compare",
                *_where(two_runs, "-t", "copy"),
                "--run-id1",
                a["run_id"],
                "--run-id2",
                b["run_id"],
                "--json",
            ],
        )
        doc = _json(result)
        assert doc["outputs"]["out"]["data_hash"]["state"] == "unchanged"
        assert doc["outputs"]["out"]["row_count"] == {"a": 2, "b": 2, "changed": False}

    def test_list_text_counts_inputs_and_outputs(self, two_runs):
        # Since run record v2 inputs are hashed and counted like outputs.
        result = runner.invoke(app, ["lineage", "list", *_where(two_runs, "-t", "copy")])
        rows = [line for line in result.output.splitlines() if "success" in line]
        assert rows and all("in:2" in r and "out:2" in r for r in rows), rows

    def test_list_text_does_not_turn_an_unknown_count_into_zero(self):
        # A record with no count (hash_inputs=False, or an older record) must not
        # claim an empty input: unknown is shown as unknown.
        from ubunye.cli.lineage import _rows
        from ubunye.lineage.context import StepRecord

        uncounted = [StepRecord(name="src", direction="input", format="s3", location="x")]
        assert _rows(uncounted) == "-"

    def test_compare_text_lines_up_every_output_field(self, two_runs):
        a, b = _json(
            runner.invoke(app, ["lineage", "list", *_where(two_runs, "-t", "copy", "--json")])
        )
        args = ["lineage", "compare", *_where(two_runs, "-t", "copy")]
        result = runner.invoke(app, [*args, "--run-id1", a["run_id"], "--run-id2", b["run_id"]])
        fields = [
            line
            for line in result.output.splitlines()
            if line.lstrip().startswith(("row_count:", "schema_hash:", "data_hash:"))
        ]
        # One input and one output since run record v2, each with the same three fields.
        assert len(fields) == 6
        assert {len(line) - len(line.lstrip()) for line in fields} == {6}, fields

    def test_search(self, two_runs):
        result = runner.invoke(app, ["lineage", "search", "-d", str(two_runs), "--json"])
        assert len(_json(result)) == 2

    def test_trace(self, two_runs):
        result = runner.invoke(app, ["lineage", "trace", *_where(two_runs, "-t", "copy", "--json")])
        doc = _json(result)
        assert doc["outputs"][0]["hash_method"] == "rows-v1"

    def test_no_records_is_an_empty_list_not_text(self, tmp_path):
        result = runner.invoke(
            app, ["lineage", "list", *_where(tmp_path, "-t", "nothing", "--json")]
        )
        assert _json(result) == []

    def test_a_missing_run_is_json(self, two_runs):
        result = runner.invoke(
            app,
            [
                "lineage",
                "compare",
                *_where(two_runs, "-t", "copy"),
                "--run-id1",
                "nope",
                "--run-id2",
                "nope2",
                "--json",
            ],
        )
        doc = _json(result)
        assert result.exit_code == 1 and doc["ok"] is False


class TestModels:
    @pytest.fixture
    def store(self, tmp_path):
        from ubunye.models.registry import ModelRegistry
        from unit.models.test_registry import DummyModel

        registry = ModelRegistry(str(tmp_path / "store"))
        registry.register("uc", "M", "1.0.0", DummyModel(), {"auc": 0.8})
        registry.register("uc", "M", "1.1.0", DummyModel(), {"auc": 0.9})
        return str(tmp_path / "store")

    def test_list(self, store):
        result = runner.invoke(
            app, ["models", "list", "-u", "uc", "-m", "M", "-s", store, "--json"]
        )
        versions = _json(result)
        assert [v["version"] for v in versions] == ["1.1.0", "1.0.0"]
        assert versions[0]["stage"] == "development"

    def test_compare(self, store):
        result = runner.invoke(
            app,
            [
                "models",
                "compare",
                "-u",
                "uc",
                "-m",
                "M",
                "-s",
                store,
                "--versions",
                "1.0.0",
                "--versions",
                "1.1.0",
                "--json",
            ],
        )
        doc = _json(result)
        assert doc["auc"]["delta"] == pytest.approx(0.1)
