"""Choosing a backend: ``--backend`` on run / test run / validate, ``ubunye backends``,
and ``backend=`` in the Python API.

The end to end runs use the pandas backend, so they need no Spark and no Java:
exactly the "run a folder on your laptop" promise, tested where it is made.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.api import _detect_backend  # noqa: E402
from ubunye.cli.main import app  # noqa: E402
from ubunye.core import backends as registry  # noqa: E402
from ubunye.core.errors import BackendCapabilityError  # noqa: E402

runner = CliRunner()

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path, *, input_format: str = "s3", name: str = "copy") -> Path:
    """<root>/uc/pkg/<name>: read a csv, write parquet."""
    task = root / "uc" / "pkg" / name
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id,city\n1,jhb\n2,cpt\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    source = (
        f'format: s3\n      path: "{(root / "in.csv").as_posix()}"\n      file_format: csv\n'
        '      options:\n        header: "true"'
        if input_format == "s3"
        else "format: hive\n      db_name: d\n      tbl_name: t"
    )
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      {source}
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


def _args(root: Path, *extra: str):
    return ["-d", str(root), "-u", "uc", "-p", "pkg", "-t", "copy", *extra]


class TestBackendsCommand:
    def test_lists_every_backend_and_the_default(self):
        result = runner.invoke(app, ["backends"])
        assert result.exit_code == 0, result.output
        assert "spark (default)" in result.output
        assert "pandas" in result.output and "databricks" in result.output
        assert "needs Java: no" in result.output  # pandas

    def test_json_is_machine_readable(self):
        result = runner.invoke(app, ["backends", "--json"])
        rows = {row["name"]: row for row in json.loads(result.output)}
        assert rows["spark"]["default"] is True
        assert rows["pandas"]["capabilities"]["file_formats"] == ["csv", "json", "parquet"]
        assert rows["pandas"]["capabilities"]["needs_jvm"] is False


class TestRun:
    def test_runs_on_pandas_with_no_spark(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(app, ["run", *_args(tmp_path, "--backend", "pandas")])
        assert result.exit_code == 0, result.output
        out = ubunye.core.backends.create("pandas").read_frame("parquet", str(tmp_path / "out"))
        assert out.count() == 2

    def test_names_are_case_blind(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(app, ["run", *_args(tmp_path, "--backend", "Pandas")])
        assert result.exit_code == 0, result.output

    def test_an_unknown_backend_lists_what_is_installed(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(app, ["run", *_args(tmp_path, "--backend", "duckdb")])
        assert result.exit_code == 1
        assert "duckdb" in result.output and "pandas" in result.output

    def test_a_task_the_backend_cannot_run_stops_before_it_starts(self, tmp_path):
        _task(tmp_path, input_format="hive")
        result = runner.invoke(app, ["run", *_args(tmp_path, "--backend", "pandas")])
        assert result.exit_code != 0
        assert isinstance(result.exception, BackendCapabilityError)
        assert "'hive' connector, which needs spark" in str(result.exception)
        assert not (tmp_path / "out").exists()


class TestValidate:
    def test_passes_a_task_pandas_can_run(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(app, ["validate", *_args(tmp_path, "--backend", "pandas")])
        assert result.exit_code == 0, result.output

    def test_names_every_reason_it_cannot(self, tmp_path):
        _task(tmp_path, input_format="hive")
        result = runner.invoke(app, ["validate", *_args(tmp_path, "--backend", "pandas")])
        assert result.exit_code == 1
        assert "input 'src' uses the 'hive' connector" in result.output

    def test_without_backend_nothing_changes(self, tmp_path):
        _task(tmp_path, input_format="hive")
        result = runner.invoke(app, ["validate", *_args(tmp_path)])
        assert result.exit_code == 0, result.output


class TestTestRun:
    def test_runs_task_tests_on_pandas(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(
            app, ["test", "run", *_args(tmp_path, "--backend", "pandas", "--no-lineage")]
        )
        assert result.exit_code == 0, result.output
        assert "[PASS] copy" in result.output


class TestApi:
    def test_run_task_by_name(self, tmp_path):
        task = _task(tmp_path)
        outputs = ubunye.run_task(str(task), backend="pandas")
        assert len(outputs["out"]) == 2  # a native pandas frame (ADR 004)

    def test_run_pipeline_by_name(self, tmp_path):
        _task(tmp_path)
        results = ubunye.run_pipeline(str(tmp_path), "uc", "pkg", ["copy"], backend="pandas")
        assert len(results["copy"]["out"]) == 2

    def test_notebook_step_by_step_by_name(self, tmp_path):
        task = _task(tmp_path)
        nb = ubunye.notebook(str(task), backend="pandas")
        try:
            assert nb.backend.name == "pandas"
            sources = nb.read()
            nb.write(nb.transform(sources))
        finally:
            nb.close()
        assert (tmp_path / "out" / "_SUCCESS").exists()

    def test_an_instance_is_used_as_given(self):
        backend = registry.create("pandas")
        assert _detect_backend(backend=backend) is backend

    def test_backend_and_spark_together_is_refused(self):
        with pytest.raises(ValueError, match="not both"):
            _detect_backend(spark=object(), backend="pandas")

    def test_anything_else_is_refused(self):
        with pytest.raises(TypeError, match="registered name"):
            _detect_backend(backend=42)  # type: ignore[arg-type]

    def test_the_default_is_still_a_new_spark_session(self, monkeypatch):
        monkeypatch.setattr(registry, "_platform_backend", lambda **kw: None)
        backend = _detect_backend(spark_conf={"a": "b"}, app_name="ubunye:x")
        assert type(backend).__name__ == "SparkBackend" and backend.app_name == "ubunye:x"
