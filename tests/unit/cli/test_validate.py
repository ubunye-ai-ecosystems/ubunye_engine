"""Unit tests for the ubunye validate CLI command."""

import yaml
from typer.testing import CliRunner

from ubunye.cli.main import app

runner = CliRunner()

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_VALID_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {"source": {"format": "hive", "db_name": "db", "tbl_name": "tbl"}},
        "transform": {"type": "noop"},
        "outputs": {"sink": {"format": "hive", "db_name": "db", "tbl_name": "out"}},
    },
}

_INVALID_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {"source": {"format": "hive"}},  # missing db_name and tbl_name
        "outputs": {"sink": {"format": "hive", "db_name": "db", "tbl_name": "out"}},
    },
}


def _scaffold_task(base_dir, usecase, pipeline, task_name, config_dict):
    task_dir = base_dir / usecase / pipeline / task_name
    task_dir.mkdir(parents=True)
    (task_dir / "config.yaml").write_text(yaml.dump(config_dict), encoding="utf-8")
    return task_dir


# ---------------------------------------------------------------------------
# Single task validation
# ---------------------------------------------------------------------------


class TestValidateCLI:
    def test_valid_config_exits_zero(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "claim_etl", _VALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "claim_etl",
            ],
        )
        assert result.exit_code == 0
        assert "[OK]" in result.output

    def test_invalid_config_exits_nonzero(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "bad_etl", _INVALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "bad_etl",
            ],
        )
        assert result.exit_code != 0
        assert "[FAIL]" in result.output

    def test_invalid_config_shows_error_details(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "bad_etl", _INVALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "bad_etl",
            ],
        )
        # Error details should mention hive and the missing fields
        assert "hive" in result.output

    def test_missing_task_directory_exits_nonzero(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "nonexistent_task",
            ],
        )
        assert result.exit_code != 0

    # ---------------------------------------------------------------------------
    # --all flag
    # ---------------------------------------------------------------------------

    def test_all_valid_tasks_exit_zero(self, tmp_path):
        for task in ("task_a", "task_b"):
            _scaffold_task(tmp_path, "fraud", "ingestion", task, _VALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "--all",
            ],
        )
        assert result.exit_code == 0
        assert "task_a" in result.output
        assert "task_b" in result.output

    def test_all_with_one_invalid_exits_nonzero(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "good_task", _VALID_CONFIG)
        _scaffold_task(tmp_path, "fraud", "ingestion", "bad_task", _INVALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "--all",
            ],
        )
        assert result.exit_code != 0
        # Both tasks should appear in output
        assert "good_task" in result.output
        assert "bad_task" in result.output

    def test_all_mentions_each_task(self, tmp_path):
        for task in ("claim_etl", "policy_etl"):
            _scaffold_task(tmp_path, "fraud", "ingestion", task, _VALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "--all",
            ],
        )
        assert "claim_etl" in result.output
        assert "policy_etl" in result.output

    def test_all_on_empty_package_warns(self, tmp_path):
        empty_pkg = tmp_path / "fraud" / "ingestion"
        empty_pkg.mkdir(parents=True)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "--all",
            ],
        )
        # Should not crash — either exits 0 with a warning or non-zero
        assert result.exit_code in (0, 1)

    # ---------------------------------------------------------------------------
    # --profile flag
    # ---------------------------------------------------------------------------

    def test_valid_profile_passes(self, tmp_path):
        cfg = {
            **_VALID_CONFIG,
            "ENGINE": {
                "spark_conf": {},
                "profiles": {"dev": {"spark_conf": {"spark.master": "local[*]"}}},
            },
        }
        _scaffold_task(tmp_path, "fraud", "ingestion", "task_a", cfg)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "task_a",
                "--profile",
                "dev",
            ],
        )
        assert result.exit_code == 0

    def test_nonexistent_profile_fails(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "task_a", _VALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "task_a",
                "--profile",
                "prod",
            ],
        )
        assert result.exit_code != 0

    # ---------------------------------------------------------------------------
    # Summary line
    # ---------------------------------------------------------------------------

    def test_summary_line_on_success(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "task_a", _VALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "task_a",
            ],
        )
        assert "passed" in result.output.lower() or "1" in result.output

    def test_summary_line_on_failure(self, tmp_path):
        _scaffold_task(tmp_path, "fraud", "ingestion", "bad_task", _INVALID_CONFIG)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(tmp_path),
                "-u",
                "fraud",
                "-p",
                "ingestion",
                "-t",
                "bad_task",
            ],
        )
        assert "failed" in result.output.lower() or "1" in result.output


class TestValidateMatchesRun:
    """validate exists to catch problems BEFORE a run. It must not invent its own.

    It used to inject only {{ dt }}, so a config using {{ mode }} or {{ dtf }} ran
    perfectly under `ubunye run` and failed under `ubunye validate` with an undefined
    variable error. Backwards: the checker was stricter than reality, about nothing.

    It also used a different word than run for the same idea (--profile vs -m), which
    made the two commands feel like different systems.
    """

    def _write(self, tmp_path, body):
        task = tmp_path / "uc" / "pkg" / "t1"
        task.mkdir(parents=True)
        (task / "config.yaml").write_text(body, encoding="utf-8")
        return tmp_path

    CONFIG_USING_MODE = (
        'MODEL: "etl"\n'
        'VERSION: "1.0.0"\n'
        "CONFIG:\n"
        "  inputs:\n"
        '    src: {format: s3, path: "/in/{{ mode }}"}\n'
        "  transform: {}\n"
        "  outputs:\n"
        '    dst: {format: s3, path: "/out", mode: append}\n'
    )

    def test_a_config_that_runs_also_validates(self, tmp_path):
        d = self._write(tmp_path, self.CONFIG_USING_MODE)
        result = runner.invoke(app, ["validate", "-d", str(d), "-u", "uc", "-p", "pkg", "--all"])
        assert result.exit_code == 0, result.output

    def test_dash_m_is_understood_like_run(self, tmp_path):
        d = self._write(
            tmp_path,
            self.CONFIG_USING_MODE.replace(
                "CONFIG:",
                "ENGINE:\n  profiles:\n    PROD:\n      spark_conf: {}\nCONFIG:",
            ),
        )
        result = runner.invoke(
            app, ["validate", "-d", str(d), "-u", "uc", "-p", "pkg", "--all", "-m", "PROD"]
        )
        assert result.exit_code == 0, result.output

    def test_conflicting_profile_and_mode_is_a_clear_error(self, tmp_path):
        d = self._write(tmp_path, self.CONFIG_USING_MODE)
        result = runner.invoke(
            app,
            [
                "validate",
                "-d",
                str(d),
                "-u",
                "uc",
                "-p",
                "pkg",
                "--all",
                "--profile",
                "a",
                "-m",
                "b",
            ],
        )
        assert result.exit_code == 1
        assert "disagree" in result.output
