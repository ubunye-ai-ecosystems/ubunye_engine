"""Tests for the ``ubunye deploy databricks`` CLI command."""

import re

import yaml
from typer.testing import CliRunner

from ubunye.cli.main import app

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _scaffold_task(base_dir, usecase, pipeline, task_name):
    """Create a minimal task directory with config.yaml and transformations.py."""
    task_dir = base_dir / usecase / pipeline / task_name
    task_dir.mkdir(parents=True)
    config = {
        "CONFIG": {
            "inputs": {
                "source": {"format": "hive", "db_name": "default", "tbl_name": "sample"},
            },
            "outputs": {
                "sink": {"format": "delta", "path": "/tmp/output", "mode": "overwrite"},
            },
        },
    }
    (task_dir / "config.yaml").write_text(yaml.dump(config), encoding="utf-8")
    (task_dir / "transformations.py").write_text("pass\n", encoding="utf-8")

    targets = {
        "targets": {
            "dev": {"host": "https://adb-123.azuredatabricks.net"},
            "prod": {"host": "https://adb-456.azuredatabricks.net", "num_workers": 4},
        }
    }
    (base_dir / usecase / "targets.yaml").write_text(yaml.dump(targets), encoding="utf-8")
    return task_dir


class TestDeployDatabricksCLI:
    def test_dry_run_prints_spec(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "claim_etl")

        result = runner.invoke(
            app,
            [
                "deploy",
                "databricks",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "claim_etl",
                "--target",
                "dev",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DRY RUN" in result.output
        assert "ubunye-fraud-ingestion-claim_etl-dev" in result.output

    def test_dry_run_includes_job_spec_json(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "claim_etl")

        result = runner.invoke(
            app,
            [
                "deploy",
                "databricks",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "claim_etl",
                "--target",
                "dev",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0
        assert '"run_ubunye_task"' in result.output
        assert '"notebook_task"' in result.output

    def test_missing_target_shows_error(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "claim_etl")

        result = runner.invoke(
            app,
            [
                "deploy",
                "databricks",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "claim_etl",
                "--target",
                "staging",
                "--dry-run",
            ],
        )
        assert result.exit_code != 0

    def test_help_shows_flags(self):
        result = runner.invoke(app, ["deploy", "databricks", "--help"])
        assert result.exit_code == 0
        clean = _ANSI_RE.sub("", result.output)
        assert "--target" in clean
        assert "--dry-run" in clean
        assert "-d" in clean
        assert "-u" in clean
        assert "-p" in clean
        assert "-t" in clean

    def test_deploy_help(self):
        result = runner.invoke(app, ["deploy", "--help"])
        assert result.exit_code == 0
        assert "databricks" in result.output
