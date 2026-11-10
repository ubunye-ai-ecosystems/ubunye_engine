"""Unit tests for the ``ubunye export`` CLI command."""

import json

import yaml
from typer.testing import CliRunner

from ubunye.cli.main import app

runner = CliRunner()


_BASE_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {"src": {"format": "hive", "db_name": "raw", "tbl_name": "claims"}},
        "transform": {"type": "noop"},
        "outputs": {"out": {"format": "hive", "db_name": "clean", "tbl_name": "claims"}},
    },
}


def _write_config(tmp_path, orchestration=None):
    cfg = dict(_BASE_CONFIG)
    if orchestration is not None:
        cfg["ORCHESTRATION"] = orchestration
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(cfg), encoding="utf-8")
    return path


def test_export_airflow_uses_orchestration_block(tmp_path):
    cfg = _write_config(
        tmp_path,
        orchestration={
            "type": "airflow",
            "schedule": "0 2 * * *",
            "owner": "fraud-team",
            "retries": 5,
            "tags": ["fraud", "daily"],
        },
    )
    out = tmp_path / "dags" / "claims.py"

    result = runner.invoke(
        app, ["export", "airflow", "-c", str(cfg), "-o", str(out), "--profile", "prod"]
    )

    assert result.exit_code == 0, result.stdout
    dag_src = out.read_text(encoding="utf-8")
    assert "\"owner\": 'fraud-team'" in dag_src
    assert '"retries": 5' in dag_src
    # schedule=, not schedule_interval=: Airflow 3 removed schedule_interval.
    assert "schedule='0 2 * * *'" in dag_src
    assert "schedule_interval" not in dag_src
    # NOT "--profile prod": there is no --profile on `ubunye run`, and there never was.
    # The old assertion enshrined the bug -- it checked that the exporter emitted a
    # command the CLI would reject with "no such option". A test can only catch a broken
    # artifact if it asserts the artifact WORKS, not that it looks the way it always has.
    assert "-m prod" in dag_src
    assert "ubunye run -d " in dag_src
    assert "--profile" not in dag_src


def test_export_airflow_without_orchestration_block_uses_defaults(tmp_path):
    cfg = _write_config(tmp_path)
    out = tmp_path / "dag.py"

    result = runner.invoke(app, ["export", "airflow", "-c", str(cfg), "-o", str(out)])

    assert result.exit_code == 0, result.stdout
    dag_src = out.read_text(encoding="utf-8")
    assert "\"owner\": 'ubunye'" in dag_src  # exporter default
    # NOT "--profile prod": there is no --profile on `ubunye run`, and there never was.
    # The old assertion enshrined the bug -- it checked that the exporter emitted a
    # command the CLI would reject with "no such option". A test can only catch a broken
    # artifact if it asserts the artifact WORKS, not that it looks the way it always has.
    assert "-m prod" in dag_src
    assert "ubunye run -d " in dag_src
    assert "--profile" not in dag_src


def test_export_databricks_flattens_nested_cluster_block(tmp_path):
    cfg = _write_config(
        tmp_path,
        orchestration={
            "type": "databricks",
            "schedule": "0 0 2 * * ?",
            "databricks": {
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 4,
                "spark_version": "14.3.x-scala2.12",
            },
        },
    )
    out = tmp_path / "jobs" / "claims.json"

    result = runner.invoke(
        app, ["export", "databricks", "-c", str(cfg), "-o", str(out), "--profile", "prod"]
    )

    assert result.exit_code == 0, result.stdout
    spec = json.loads(out.read_text(encoding="utf-8"))
    cluster = spec["tasks"][0]["new_cluster"]
    assert cluster["node_type_id"] == "Standard_DS3_v2"
    assert cluster["num_workers"] == 4
    assert cluster["spark_version"] == "14.3.x-scala2.12"
    assert spec["schedule"]["quartz_cron_expression"] == "0 0 2 * * ?"
    assert spec["tasks"][0]["spark_python_task"]["parameters"][-1] == "prod"


def test_export_rejects_invalid_config(tmp_path):
    bad = tmp_path / "config.yaml"
    bad.write_text("MODEL: etl\nVERSION: not-semver\n", encoding="utf-8")
    out = tmp_path / "dag.py"

    result = runner.invoke(app, ["export", "airflow", "-c", str(bad), "-o", str(out)])
    assert result.exit_code != 0


def test_the_dag_runs_on_airflow_2_and_3_and_keeps_the_environment(tmp_path):
    cfg = _write_config(tmp_path)
    out = tmp_path / "dag.py"
    args = ["export", "airflow", "-c", str(cfg), "-o", str(out), "--usecase-dir", "/opt/pipelines",
            "--backend", "pandas", "--lineage"]  # fmt: skip
    assert runner.invoke(app, args).exit_code == 0
    dag_src = out.read_text(encoding="utf-8")
    compile(dag_src, "dag.py", "exec")
    assert "from airflow.providers.standard.operators.bash import BashOperator" in dag_src
    assert "from airflow.operators.bash import BashOperator" in dag_src
    # env= alone replaces the environment, and then `ubunye` is not on PATH.
    assert "append_env=True" in dag_src
    assert "ubunye run -d /opt/pipelines " in dag_src
    assert "--backend pandas --lineage -dt {{ ds }}" in dag_src


def test_a_config_that_uses_dt_exports(tmp_path):
    """Found by running the exported DAG on real Airflow: `{{ dt }}` broke the export."""
    cfg = dict(_BASE_CONFIG)
    cfg["CONFIG"] = {
        "inputs": {"src": {"format": "s3", "path": "in/{{ dt }}.csv", "file_format": "csv"}},
        "outputs": {"out": {"format": "s3", "path": "out/{{ dt }}", "file_format": "parquet"}},
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(cfg), encoding="utf-8")
    result = runner.invoke(
        app, ["export", "airflow", "-c", str(path), "-o", str(tmp_path / "d.py")]
    )
    assert result.exit_code == 0, result.output


def test_export_databricks_defaults_follow_the_installed_engine(tmp_path):
    """The wheel is this engine's version and the runtime a current LTS (F-095)."""
    import ubunye

    cfg = _write_config(tmp_path, orchestration={"type": "databricks"})
    out = tmp_path / "job.json"
    result = runner.invoke(app, ["export", "databricks", "-c", str(cfg), "-o", str(out)])
    assert result.exit_code == 0, result.stdout
    task = json.loads(out.read_text(encoding="utf-8"))["tasks"][0]
    assert task["libraries"] == [
        {"whl": f"dbfs:/libs/ubunye_engine-{ubunye.__version__}-py3-none-any.whl"}
    ]
    assert task["new_cluster"]["spark_version"] == "17.3.x-scala2.13"


def test_export_databricks_wheel_can_still_be_given(tmp_path):
    cfg = _write_config(
        tmp_path,
        orchestration={"type": "databricks", "databricks": {"wheel_dbfs_path": "dbfs:/x.whl"}},
    )
    out = tmp_path / "job.json"
    assert (
        runner.invoke(app, ["export", "databricks", "-c", str(cfg), "-o", str(out)]).exit_code == 0
    )
    assert json.loads(out.read_text())["tasks"][0]["libraries"] == [{"whl": "dbfs:/x.whl"}]


def test_export_help_says_its_profile_default_differs_from_run():
    """export defaults to prod while `run` defaults to DEV: the help says both."""
    for target in ("airflow", "databricks"):
        result = runner.invoke(app, ["export", target, "--help"], env={"COLUMNS": "200"})
        assert "prod" in result.stdout and "DEV" in result.stdout


def test_each_help_says_export_defaults_to_production_and_run_to_dev():
    """The defaults differ on purpose and stay; each help says so plainly (F-115)."""
    import re

    expected = {
        ("export", "airflow"): "Default prod, not the DEV",
        ("export", "databricks"): "Default prod, not the DEV",
        ("export", "spark-pipeline"): "Default PROD, not the DEV",
        ("run",): "Default DEV; `ubunye export` defaults to production",
    }
    for command, phrase in expected.items():
        result = runner.invoke(app, [*command, "--help"], env={"COLUMNS": "300"})
        text = " ".join(re.sub(r"\x1b\[[0-9;]*m|[│╭╮╰╯─]", " ", result.output).split())
        assert phrase in text, (command, text)
