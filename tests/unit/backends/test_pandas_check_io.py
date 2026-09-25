"""The pandas backend checks its IO options before a run (plan, validate, run).

Capabilities say what a backend can do in general; ``check_io`` catches the
details that used to fail only when the file was opened: an option the backend
cannot honour, a schema it cannot use.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.cli.main import app  # noqa: E402
from ubunye.core.errors import BackendCapabilityError  # noqa: E402


def test_reader_options_are_checked():
    problems = PandasBackend.check_io(
        "input", {"format": "s3", "file_format": "csv", "options": {"dateFormat": "d"}}
    )
    assert len(problems) == 1 and "dateFormat" in problems[0]


def test_spark_parse_mode_is_fine():
    assert (
        PandasBackend.check_io(
            "input", {"format": "s3", "file_format": "csv", "options": {"mode": "FAILFAST"}}
        )
        == []
    )


def test_a_bad_schema_is_caught():
    (problem,) = PandasBackend.check_io(
        "input", {"format": "s3", "file_format": "json", "schema": "a MAP<STRING,INT>"}
    )
    assert "MAP<STRING,INT>" in problem


def test_writer_options_are_checked():
    (problem,) = PandasBackend.check_io(
        "output", {"format": "s3", "file_format": "csv", "options": {"quoteAll": "true"}}
    )
    assert "quoteAll" in problem


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("a\n1\n", encoding="utf-8")
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\nclass T(Task):\n"
        "    def transform(self, sources):\n        return {'out': sources['src']}\n",
        encoding="utf-8",
    )
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{(root / "in.csv").as_posix()}"
      file_format: csv
      options:
        header: "true"
        dateFormat: "dd/MM/yyyy"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / "out").as_posix()}"
      file_format: parquet
""",
        encoding="utf-8",
    )
    return task


def test_plan_reports_it(tmp_path):
    _task(tmp_path)
    result = CliRunner().invoke(
        app,
        ["plan", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t", "--backend", "pandas"],
    )
    assert result.exit_code == 1 and "dateFormat" in result.output


def test_a_run_stops_before_reading(tmp_path):
    with pytest.raises(BackendCapabilityError, match="dateFormat"):
        ubunye.run_task(str(_task(tmp_path)), backend="pandas")
    assert not (tmp_path / "out").exists()
