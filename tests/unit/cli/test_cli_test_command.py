"""Tests for the ``ubunye test run`` CLI command."""
from __future__ import annotations

import yaml
from typer.testing import CliRunner

from ubunye.cli.main import app

runner = CliRunner()

_BASE_ARGS = [
    "test", "run",
    "-u", "fraud_detection",
    "-p", "ingestion",
    "-t", "claim_etl",
]


def _invoke(usecase_dir, extra_args=None):
    args = [*_BASE_ARGS[:1], *_BASE_ARGS[1:], "-d", str(usecase_dir)]
    if extra_args:
        args.extend(extra_args)
    return runner.invoke(app, args)


# ---------------------------------------------------------------------------
# Config validation phase
# ---------------------------------------------------------------------------

class TestUbunyeTestConfigValidation:

    def test_valid_config_reports_config_ok(self, valid_task_dir):
        result = _invoke(valid_task_dir)
        assert "[CONFIG OK]" in result.output

    def test_invalid_config_exits_nonzero(self, invalid_task_dir):
        result = runner.invoke(app, [
            "test", "run",
            "-d", str(invalid_task_dir),
            "-u", "fraud_detection",
            "-p", "ingestion",
            "-t", "bad_etl",
        ])
        assert result.exit_code != 0

    def test_invalid_config_reports_config_fail(self, invalid_task_dir):
        result = runner.invoke(app, [
            "test", "run",
            "-d", str(invalid_task_dir),
            "-u", "fraud_detection",
            "-p", "ingestion",
            "-t", "bad_etl",
        ])
        assert "[CONFIG FAIL]" in result.output

    def test_missing_task_dir_exits_nonzero(self, tmp_path):
        result = runner.invoke(app, [
            "test", "run",
            "-d", str(tmp_path),
            "-u", "no_uc",
            "-p", "no_pkg",
            "-t", "no_task",
        ])
        assert result.exit_code != 0

    def test_missing_task_dir_reports_config_fail(self, tmp_path):
        result = runner.invoke(app, [
            "test", "run",
            "-d", str(tmp_path),
            "-u", "no_uc",
            "-p", "no_pkg",
            "-t", "no_task",
        ])
        assert "[CONFIG FAIL]" in result.output

    def test_output_includes_task_name(self, valid_task_dir):
        result = _invoke(valid_task_dir)
        assert "claim_etl" in result.output


# ---------------------------------------------------------------------------
# Multiple tasks
# ---------------------------------------------------------------------------

class TestUbunyeTestMultipleTasks:

    def test_all_valid_tasks_report_config_ok(self, multi_task_dir):
        result = runner.invoke(app, [
            "test", "run",
            "-d", str(multi_task_dir),
            "-u", "fraud_detection",
            "-p", "ingestion",
            "-t", "claim_etl",
            "-t", "policy_etl",
        ])
        assert result.output.count("[CONFIG OK]") == 2

    def test_one_invalid_task_exits_nonzero(self, tmp_path):
        """One valid + one missing task → exit non-zero."""
        # Create only one of the two tasks
        task = tmp_path / "proj" / "uc" / "pkg" / "good"
        task.mkdir(parents=True)
        cfg = {
            "MODEL": "etl", "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                "outputs": {"s": {"format": "s3", "path": "s3a://b/p"}},
            }
        }
        (task / "config.yaml").write_text(yaml.dump(cfg))
        (task / "transformations.py").write_text("from ubunye.core.interfaces import Task\nclass T(Task):\n    def transform(self,s): return s")

        result = runner.invoke(app, [
            "test", "run",
            "-d", str(tmp_path / "proj"),
            "-u", "uc", "-p", "pkg",
            "-t", "good", "-t", "missing",
        ])
        assert result.exit_code != 0
