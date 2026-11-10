"""-p/--pipeline, and --package as its deprecated old name (0.9.0)."""

import re

import pytest
import yaml
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.cli.pipeline_flag import rewrite_package_flag

runner = CliRunner()
_ANSI = re.compile(r"\x1b\[[0-9;]*m")

_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {"source": {"format": "hive", "db_name": "db", "tbl_name": "tbl"}},
        "transform": {"type": "noop"},
        "outputs": {"sink": {"format": "hive", "db_name": "db", "tbl_name": "out"}},
    },
}

# Every command that takes the pipeline name.
_COMMANDS = [
    ["config"],
    ["validate"],
    ["plan"],
    ["run"],
    ["doctor"],
    ["gate"],
    ["init"],
    ["init", "pipeline"],
    ["init", "github-actions"],
    ["test", "run"],
    ["runs", "health"],
    ["prove", "observe"],
    ["deploy", "databricks"],
    ["deploy", "glue"],
    ["deploy", "dataproc"],
    ["deploy", "k8s"],
    ["deploy", "container-apps"],
    ["deploy", "emr-serverless"],
    ["lineage", "show"],
    ["lineage", "list"],
    ["lineage", "openlineage"],
    ["lineage", "compare"],
    ["lineage", "search"],
    ["lineage", "trace"],
    ["lineage", "focus"],
]


def _validate(tmp_path, *pipeline_args):
    task = tmp_path / "fraud" / "ingestion" / "claim_etl"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(yaml.dump(_CONFIG), encoding="utf-8")
    args = ["validate", "-d", str(tmp_path), "-u", "fraud", *pipeline_args, "-t", "claim_etl"]
    return runner.invoke(app, args)


def _text(result):
    return _ANSI.sub("", result.output)


@pytest.mark.parametrize("command", _COMMANDS, ids=" ".join)
def test_help_shows_pipeline_and_hides_package(command):
    result = runner.invoke(app, [*command, "--help"], terminal_width=200)
    out = _text(result)
    assert result.exit_code == 0, out
    assert "--pipeline" in out
    assert "--package" not in out


def test_new_long_flag_works(tmp_path):
    result = _validate(tmp_path, "--pipeline", "ingestion")
    assert result.exit_code == 0, _text(result)
    assert "renamed" not in result.stderr


def test_short_flag_still_works(tmp_path):
    result = _validate(tmp_path, "-p", "ingestion")
    assert result.exit_code == 0, _text(result)
    assert "renamed" not in result.stderr


@pytest.mark.parametrize("old", [["--package", "ingestion"], ["--package=ingestion"]])
def test_old_flag_works_and_warns_once_on_stderr(tmp_path, old):
    result = _validate(tmp_path, *old)
    assert result.exit_code == 0, _text(result)
    assert result.stderr.count("--package is renamed --pipeline") == 1
    assert "removed in 1.0" in result.stderr


@pytest.mark.parametrize("new", ["-p", "--pipeline"])
def test_old_and_new_together_is_an_error(tmp_path, new):
    result = _validate(tmp_path, new, "ingestion", "--package", "ingestion")
    assert result.exit_code == 2
    assert "--package" in _text(result)


def test_old_flag_works_on_the_init_group(tmp_path):
    args = ["init", "-d", str(tmp_path), "-u", "demo", "--package", "starter", "-t", "t1"]
    result = runner.invoke(app, args)
    assert result.exit_code == 0, _text(result)
    assert (tmp_path / "demo" / "starter" / "t1" / "config.yaml").exists()
    assert "--package is renamed --pipeline" in result.stderr


def test_old_flag_works_on_a_nested_command(tmp_path):
    args = ["init", "pipeline", "-d", str(tmp_path), "-u", "demo", "--package", "s", "-t", "t1"]
    result = runner.invoke(app, args)
    assert result.exit_code == 0, _text(result)
    assert (tmp_path / "demo" / "s" / "t1").is_dir()


def test_nothing_after_double_dash_is_rewritten():
    args, old, new = rewrite_package_flag(["-p", "x", "--", "--package"])
    assert args == ["-p", "x", "--", "--package"] and new and not old
