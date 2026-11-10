"""Expected refusals in `ubunye run` print one [ERROR] line, never a traceback."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ubunye.cli import doctor
from ubunye.cli.main import app

try:  # Click before 8.2 mixes stderr into stdout unless asked not to
    runner = CliRunner(mix_stderr=False)
except TypeError:  # Click 8.2+ always keeps them apart
    runner = CliRunner()


def _text(result) -> str:
    try:
        err = result.stderr
    except ValueError:
        err = ""
    return result.output + err


def test_a_task_with_no_task_class_is_refused_without_a_traceback(tmp_path: Path):
    runner.invoke(app, ["init", "-d", str(tmp_path), "-u", "demo", "-p", "sales", "-t", "clean"])
    (tmp_path / "demo" / "sales" / "clean" / "transformations.py").write_text("X = 1\n")
    result = runner.invoke(
        app,
        [
            "run",
            "-d",
            str(tmp_path),
            "-u",
            "demo",
            "-p",
            "sales",
            "-t",
            "clean",
            "--backend",
            "pandas",
        ],
    )
    assert result.exit_code == 1
    assert "[ERROR] Run refused for clean" in _text(result)
    assert "Traceback" not in _text(result)
    assert result.exception is None or isinstance(result.exception, SystemExit)


def test_doctor_never_suggests_databricks_off_a_platform():
    assert doctor._suggest(["databricks", "pandas"]) == "pandas"
    assert doctor._suggest(["databricks", "polars"]) == "polars"
    assert doctor._suggest(["databricks"]) == "databricks"
