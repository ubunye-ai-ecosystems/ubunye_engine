"""Unit tests for the ubunye init CLI sub-app (pipeline + github-actions)."""

from __future__ import annotations

import yaml
from typer.testing import CliRunner

from ubunye.cli.main import app

runner = CliRunner()


# ── init pipeline ────────────────────────────────────────────────────


class TestInitPipeline:
    def test_scaffolds_config_and_transformations(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "init", "pipeline",
                "-d", str(tmp_path),
                "-u", "demo",
                "-p", "etl",
                "-t", "clean",
            ],
        )
        assert result.exit_code == 0
        assert "created" in result.output
        assert (tmp_path / "demo" / "etl" / "clean" / "config.yaml").exists()
        assert (tmp_path / "demo" / "etl" / "clean" / "transformations.py").exists()
        assert (tmp_path / "demo" / "etl" / "clean" / "notebooks" / "clean_dev.ipynb").exists()

    def test_skip_existing_without_overwrite(self, tmp_path):
        for _ in range(2):
            runner.invoke(
                app,
                ["init", "pipeline", "-d", str(tmp_path), "-u", "d", "-p", "e", "-t", "t"],
            )
        result = runner.invoke(
            app,
            ["init", "pipeline", "-d", str(tmp_path), "-u", "d", "-p", "e", "-t", "t"],
        )
        assert result.exit_code == 0
        assert "exists" in result.output

    def test_multiple_tasks(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "init", "pipeline",
                "-d", str(tmp_path),
                "-u", "demo",
                "-p", "etl",
                "-t", "task_a",
                "-t", "task_b",
            ],
        )
        assert result.exit_code == 0
        assert (tmp_path / "demo" / "etl" / "task_a" / "config.yaml").exists()
        assert (tmp_path / "demo" / "etl" / "task_b" / "config.yaml").exists()

    def test_generated_config_is_valid_yaml(self, tmp_path):
        runner.invoke(
            app,
            ["init", "pipeline", "-d", str(tmp_path), "-u", "d", "-p", "e", "-t", "t"],
        )
        cfg = yaml.safe_load((tmp_path / "d" / "e" / "t" / "config.yaml").read_text())
        assert cfg["MODEL"] == "etl"
        assert "CONFIG" in cfg


# ── init github-actions ──────────────────────────────────────────────


class TestInitGithubActions:
    def test_generates_workflow_file(self, tmp_path):
        out = tmp_path / "workflow.yml"
        result = runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
                "-o", str(out),
            ],
        )
        assert result.exit_code == 0
        assert out.exists()
        content = out.read_text()
        assert "name: fraud/ingestion" in content

    def test_generated_yaml_is_valid(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
                "-o", str(out),
            ],
        )
        parsed = yaml.safe_load(out.read_text())
        assert parsed["name"] == "fraud/ingestion"
        triggers = parsed[True]  # YAML parses `on:` as boolean True
        assert "push" in triggers
        assert "pull_request" in triggers

    def test_includes_databricks_deploy_steps(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
                "-o", str(out),
            ],
        )
        content = out.read_text()
        assert "ubunye deploy databricks" in content
        assert "DATABRICKS_HOST" in content
        assert "DATABRICKS_TOKEN" in content
        assert "check_secrets" in content

    def test_no_deploy_omits_databricks_steps(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
                "--no-deploy",
                "-o", str(out),
            ],
        )
        content = out.read_text()
        assert "ubunye deploy databricks" not in content
        assert "check_secrets" not in content
        assert "workflow_dispatch" not in content
        assert "ubunye validate" in content

    def test_spark_extras_include_java(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "demo",
                "-p", "etl",
                "-t", "clean",
                "--extras", "spark,dev",
                "-o", str(out),
            ],
        )
        content = out.read_text()
        assert "Set up Java 17" in content
        assert "PySpark" in content

    def test_ml_extras_skip_java(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "demo",
                "-p", "ml",
                "-t", "train",
                "--extras", "ml,dev",
                "-o", str(out),
            ],
        )
        content = out.read_text()
        assert "Java" not in content
        assert 'pip install -e ".[ml,dev]"' in content

    def test_multiple_tasks_get_separate_deploy_steps(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
                "-t", "policy_etl",
                "-o", str(out),
            ],
        )
        content = out.read_text()
        assert "Deploy claim_etl" in content
        assert "Deploy policy_etl" in content

    def test_single_task_uses_generic_deploy_label(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
                "-o", str(out),
            ],
        )
        content = out.read_text()
        assert "Deploy to Databricks" in content

    def test_refuses_overwrite_without_flag(self, tmp_path):
        out = tmp_path / "workflow.yml"
        out.write_text("existing", encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "demo",
                "-p", "etl",
                "-t", "clean",
                "-o", str(out),
            ],
        )
        assert result.exit_code != 0
        assert "already exists" in result.output
        assert out.read_text() == "existing"

    def test_overwrite_flag_replaces_file(self, tmp_path):
        out = tmp_path / "workflow.yml"
        out.write_text("old content", encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "demo",
                "-p", "etl",
                "-t", "clean",
                "--overwrite",
                "-o", str(out),
            ],
        )
        assert result.exit_code == 0
        assert out.read_text() != "old content"

    def test_custom_target(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "demo",
                "-p", "etl",
                "-t", "clean",
                "--target", "prod",
                "-o", str(out),
            ],
        )
        content = out.read_text()
        assert "--target prod" in content

    def test_default_output_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
            ],
        )
        assert result.exit_code == 0
        expected = tmp_path / ".github" / "workflows" / "fraud_ingestion.yml"
        assert expected.exists()

    def test_path_triggers_match_pipeline_dir(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
                "-o", str(out),
            ],
        )
        parsed = yaml.safe_load(out.read_text())
        push_paths = parsed[True]["push"]["paths"]  # YAML parses `on:` as boolean True
        assert "pipelines/fraud/ingestion/**" in push_paths

    def test_validate_step_uses_ubunye_cli(self, tmp_path):
        out = tmp_path / "workflow.yml"
        runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "fraud",
                "-p", "ingestion",
                "-t", "claim_etl",
                "-o", str(out),
            ],
        )
        content = out.read_text()
        assert "ubunye validate -d pipelines -u fraud -p ingestion --all" in content

    def test_shows_next_steps_with_deploy(self, tmp_path):
        out = tmp_path / "workflow.yml"
        result = runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "demo",
                "-p", "etl",
                "-t", "clean",
                "-o", str(out),
            ],
        )
        assert "Next steps" in result.output
        assert "DATABRICKS_HOST" in result.output

    def test_no_next_steps_without_deploy(self, tmp_path):
        out = tmp_path / "workflow.yml"
        result = runner.invoke(
            app,
            [
                "init", "github-actions",
                "-d", "pipelines",
                "-u", "demo",
                "-p", "etl",
                "-t", "clean",
                "--no-deploy",
                "-o", str(out),
            ],
        )
        assert "Next steps" not in result.output
