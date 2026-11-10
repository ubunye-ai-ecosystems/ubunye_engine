"""`ubunye deploy` and `ubunye export`: an expected error is one clean line (F-096).

A missing targets.yaml, token, cloud CLI or config is something the user fixes. It
ends in `[ERROR] <what, where, what to try>` and exit code 1, not a traceback, and
never prints the job launcher's script.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.deploy import cloud, containers

try:  # Click before 8.2 mixes stderr into stdout unless asked not to
    runner = CliRunner(mix_stderr=False)
except TypeError:  # Click 8.2+ always keeps them apart
    runner = CliRunner()
_ANSI = re.compile(r"\x1b\[[0-9;]*m")

CONFIG = """\
CONFIG:
  inputs:
    src: {format: hive, db_name: default, tbl_name: sample}
  outputs:
    out: {format: delta, path: /tmp/out, mode: overwrite}
"""


def _pipelines(root: Path) -> Path:
    task = root / "pipelines" / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(CONFIG, encoding="utf-8")
    (task / "transformations.py").write_text("pass\n", encoding="utf-8")
    return root / "pipelines"


def _clean_error(result, *needles: str) -> str:
    """The command failed with exit 1 and one [ERROR] message, no traceback."""
    err = _ANSI.sub("", result.stderr)
    assert result.exit_code == 1, (result.exception, result.output, err)
    assert isinstance(result.exception, SystemExit), result.exception
    assert err.lstrip().startswith("[ERROR]"), err
    assert "Traceback" not in err + result.stdout
    for needle in needles:
        assert needle in err, err
    return err


def _no_cli(monkeypatch):
    monkeypatch.setattr(cloud.shutil, "which", lambda name: None)


def test_databricks_without_targets_yaml_says_what_to_create(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)  # --host reads it (F-110)
    d = _pipelines(tmp_path)
    args = ["deploy", "databricks", "-d", str(d), "-u", "uc", "-p", "pkg", "-t", "t"]
    _clean_error(runner.invoke(app, args), "No targets.yaml found", "Hint:")


def test_databricks_without_a_token_names_the_env_var(tmp_path, monkeypatch):
    monkeypatch.delenv("UBUNYE_TEST_NO_TOKEN", raising=False)
    d = _pipelines(tmp_path)
    (d / "uc" / "targets.yaml").write_text(
        "targets:\n  dev:\n    host: https://adb-1.azuredatabricks.net\n"
        "    token_env: UBUNYE_TEST_NO_TOKEN\n",
        encoding="utf-8",
    )
    args = ["deploy", "databricks", "-d", str(d), "-u", "uc", "-p", "pkg", "-t", "t"]
    _clean_error(runner.invoke(app, args), "UBUNYE_TEST_NO_TOKEN")


def test_glue_without_the_aws_cli_says_what_to_install(tmp_path, monkeypatch):
    _no_cli(monkeypatch)
    args = ["deploy", "glue", "-d", str(_pipelines(tmp_path)), "-u", "uc", "-p", "pkg",
            "-t", "t", "--bucket", "b", "--role", "arn:aws:iam::1:role/r"]  # fmt: skip
    err = _clean_error(runner.invoke(app, args), "`aws` is not on PATH", "--dry-run")
    assert len(err.strip().splitlines()) <= 3


@pytest.mark.parametrize(
    "args, tool, script",
    [
        (["k8s", "--image", "i"], "kubectl", containers._K8S_RUN),
        (
            ["container-apps", "--image", "i", "-g", "rg", "--environment", "e"],
            "az",
            containers._ACA_RUN,
        ),
    ],
)
def test_container_jobs_without_their_cli_never_print_the_launcher(monkeypatch, args, tool, script):
    _no_cli(monkeypatch)
    cmd = ["deploy", args[0], "-u", "uc", "-p", "pkg", "-t", "t", *args[1:]]
    result = runner.invoke(app, cmd)
    err = _clean_error(result, f"`{tool}` is not on PATH")
    assert script.strip().splitlines()[1] not in err + result.stdout


def test_a_failed_launcher_is_named_not_printed(monkeypatch):
    """The launcher ran and failed: the error names it and never prints its script."""

    def fail(argv, **kw):
        return cloud.subprocess.CompletedProcess(argv, 3, stdout="", stderr="no such context")

    monkeypatch.setattr(cloud.subprocess, "run", fail)
    monkeypatch.setattr(cloud, "_cli", lambda name: name)
    cmd = ["deploy", "k8s", "-u", "uc", "-p", "pkg", "-t", "t", "--image", "i"]
    result = runner.invoke(app, cmd)
    err = _clean_error(result, "exit code 3", "no such context")
    assert "import json, subprocess" not in err + result.stdout


def test_a_bad_task_list_is_one_line(tmp_path):
    args = ["deploy", "glue", "-d", str(_pipelines(tmp_path)), "-u", "uc", "-p", "pkg",
            "-t", "t", "-t", "t", "--bucket", "b", "--role", "r"]  # fmt: skip
    _clean_error(runner.invoke(app, args), "given twice")


def test_export_of_a_config_that_does_not_validate_is_one_message(tmp_path):
    bad = tmp_path / "config.yaml"
    bad.write_text("MODEL: etl\nVERSION: not-semver\n", encoding="utf-8")
    args = ["export", "airflow", "-c", str(bad), "-o", str(tmp_path / "dag.py")]
    _clean_error(runner.invoke(app, args))


def test_an_unexpected_error_still_shows_its_traceback(tmp_path, monkeypatch):
    """A bug is not an expected error: it keeps its exception (and traceback)."""

    def boom(*a, **kw):
        raise ZeroDivisionError("a bug")

    monkeypatch.setattr(cloud, "plan_glue", boom)
    args = ["deploy", "glue", "-d", str(_pipelines(tmp_path)), "-u", "uc", "-p", "pkg",
            "-t", "t", "--bucket", "b", "--role", "r"]  # fmt: skip
    result = runner.invoke(app, args)
    assert isinstance(result.exception, ZeroDivisionError)
