"""Tests for Databricks notebook generation."""

from ubunye.deploy.databricks.notebook import generate_notebook


class TestGenerateNotebook:
    def test_basic_notebook(self):
        source = generate_notebook(
            workspace_task_path="/Workspace/ubunye/fraud/ingestion/claim_etl",
            mode="PROD",
        )
        assert "ubunye.run_task(" in source
        assert '"/Workspace/ubunye/fraud/ingestion/claim_etl"' in source
        assert '"PROD"' in source
        assert "# Databricks notebook source" in source
        assert "%pip install ubunye-engine" in source

    def test_restarts_python_after_pip(self):
        source = generate_notebook(
            workspace_task_path="/Workspace/ubunye/test",
        )
        assert "restartPython()" in source
        pip_pos = source.index("%pip install")
        restart_pos = source.index("restartPython()")
        assert restart_pos > pip_pos

    def test_includes_dt_when_provided(self):
        source = generate_notebook(
            workspace_task_path="/Workspace/ubunye/fraud/ingestion/claim_etl",
            mode="DEV",
            dt="2026-01-15",
        )
        assert '"2026-01-15"' in source
        assert "dt=" in source

    def test_no_dt_when_none(self):
        source = generate_notebook(
            workspace_task_path="/Workspace/ubunye/fraud/ingestion/claim_etl",
        )
        assert "dt=" not in source

    def test_custom_pip_install(self):
        source = generate_notebook(
            workspace_task_path="/Workspace/ubunye/test",
            pip_install="ubunye-engine==0.1.7",
        )
        assert "%pip install ubunye-engine==0.1.7" in source

    def test_output_display_section(self):
        source = generate_notebook(workspace_task_path="/Workspace/test")
        assert "df.show(5)" in source
        assert "df.count()" in source
