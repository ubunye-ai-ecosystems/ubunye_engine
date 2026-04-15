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
    assert '"owner": "fraud-team"' in dag_src
    assert '"retries": 5' in dag_src
    assert 'schedule_interval="0 2 * * *"' in dag_src
    assert "--profile prod" in dag_src


def test_export_airflow_without_orchestration_block_uses_defaults(tmp_path):
    cfg = _write_config(tmp_path)
    out = tmp_path / "dag.py"

    result = runner.invoke(app, ["export", "airflow", "-c", str(cfg), "-o", str(out)])

    assert result.exit_code == 0, result.stdout
    dag_src = out.read_text(encoding="utf-8")
    assert '"owner": "ubunye"' in dag_src  # exporter default
    assert "--profile prod" in dag_src


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
