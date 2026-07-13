"""`ubunye plan` must not assume `format` is an enum.

Making `format` an open string broke `plan`, which still called `.format.value`. Every
unit test passed. The thing that caught it was an example actually RUNNING —
`AttributeError: 'str' object has no attribute 'value'` — which is the entire argument
of this codebase landing on the codebase itself.

So: a test that runs the command.
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ubunye.cli.main import app

CONFIG = """
MODEL: "etl"
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "file:///tmp/in"
  transform: {}
  outputs:
    dst:
      format: s3
      path: "file:///tmp/out"
      mode: append
"""


def test_plan_prints_the_formats_without_exploding(tmp_path: Path):
    task = tmp_path / "uc" / "pkg" / "task"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(CONFIG, encoding="utf-8")
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        return {'dst': sources['src']}\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        ["plan", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "task", "-dt", "2026-07-13"],
    )

    assert result.exit_code == 0, result.stdout
    assert "s3" in result.stdout
