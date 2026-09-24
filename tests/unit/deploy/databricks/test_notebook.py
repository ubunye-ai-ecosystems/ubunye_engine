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
        # The scaffold used to print df.count() per output: a full scan of every
        # output just to show a number, teaching new users the exact habit the
        # lineage rework removed from the engine itself. Columns are free.
        assert "df.columns" in source
        assert "df.count()" not in source


class TestThePackagesTheTransformNeeds:
    """A deployed task gets what its transform imports and the cluster lacks (ADR 005).

    Databricks brings Spark and pandas; it does not bring Narwhals. A task
    written with Narwhals deployed by `ubunye deploy databricks` failed on its
    first line, because the notebook installed the engine alone.
    """

    def _notebook(self, tmp_path, transform: str) -> str:
        from ubunye.deploy.databricks import DatabricksDeployAdapter
        from ubunye.interfaces.deploy import DeployContext

        task_dir = tmp_path / "uc" / "pipe" / "task"
        task_dir.mkdir(parents=True)
        (task_dir / "transformations.py").write_text(transform, encoding="utf-8")
        ctx = DeployContext(
            task_dir=task_dir,
            usecase="uc",
            pipeline="pipe",
            task_name="task",
            config={},
            target_name="dev",
        )
        result = DatabricksDeployAdapter().deploy(
            ctx, {"host": "https://adb-1.azuredatabricks.net"}, dry_run=True
        )
        return result.metadata["notebook_source"]

    def test_a_narwhals_transform_gets_narwhals(self, tmp_path):
        source = self._notebook(tmp_path, "import narwhals as nw\n")
        assert "%pip install ubunye-engine narwhals\n" in source

    def test_a_spark_transform_gets_the_engine_alone(self, tmp_path):
        source = self._notebook(tmp_path, "from pyspark.sql import functions as F\n")
        assert "%pip install ubunye-engine\n" in source
