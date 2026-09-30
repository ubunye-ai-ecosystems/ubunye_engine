"""`ubunye run` stops on a broken expectation with its message, not a traceback (F-055).

A broken expectation is the engine doing its job: the data is wrong, nothing was
written, and the message says which rule and how many rows. The CLI printed that
message and then about 60 lines of traceback through the engine's own code, which
reads as if Ubunye had crashed.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

from ubunye.cli.main import app  # noqa: E402

CONFIG = """\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    rows: {format: s3, path: "{{ task_dir }}/rows.csv", file_format: csv,
           options: {header: "true", inferSchema: "true"}}
  transform: {}
  outputs:
    out: {format: s3, path: "{{ task_dir }}/out", file_format: parquet, mode: overwrite}
  expectations:
    out:
      rules:
        - between: {column: qty, min: 1}
"""

CODE = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["rows"]}
"""


def _task(tmp_path):
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(CONFIG, encoding="utf-8")
    (task / "transformations.py").write_text(CODE, encoding="utf-8")
    (task / "rows.csv").write_text("id,qty\n1,2\n2,0\n", encoding="utf-8")
    return task


def test_a_broken_expectation_exits_1_with_the_message_and_no_traceback(tmp_path):
    task = _task(tmp_path)
    where = ["run", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t", "--backend", "pandas"]
    result = CliRunner().invoke(app, where)

    assert result.exit_code == 1
    # A clean exit, not the ExpectationError propagating (which prints a traceback).
    assert isinstance(result.exception, SystemExit)
    assert "qty_between (between) broken by 1 of 2 rows" in result.output
    assert "nothing was written" in result.output
    assert not (task / "out").exists()


def test_any_other_error_still_shows_its_traceback(tmp_path):
    task = _task(tmp_path)
    (task / "transformations.py").write_text(
        CODE.replace('return {"out": sources["rows"]}', 'raise RuntimeError("bug in my code")'),
        encoding="utf-8",
    )
    where = ["run", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t", "--backend", "pandas"]
    result = CliRunner().invoke(app, where)

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
