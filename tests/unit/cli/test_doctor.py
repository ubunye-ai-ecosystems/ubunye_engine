"""``ubunye doctor``: what is installed, what will fail, and why, before a run.

Environment problems (a backend's package, Java, Delta's pairing with Spark) are
warnings, because they matter only for the backend you use. Problems with a task
you name (an unset environment variable, an invalid config) are failures, and
so is having no usable backend at all: those make the run fail.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ubunye.cli import doctor
from ubunye.cli.main import app
from ubunye.config.resolver import required_env_references

runner = CliRunner()
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _ok(name: str) -> doctor.Check:
    return doctor.Check(name, doctor.OK, "fine")


@pytest.fixture
def quiet_environment(monkeypatch):
    """Every environment check passes, so a test sees only what it sets up."""
    monkeypatch.setattr(doctor, "environment_checks", lambda: [_ok("python")])
    monkeypatch.setattr(doctor, "backend_checks", lambda: [_ok("backend: pandas")])
    monkeypatch.setattr(doctor, "plugin_checks", lambda: [_ok("plugins")])


def _task(root: Path, config: str) -> None:
    task = root / "uc" / "pkg" / "t1"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(config, encoding="utf-8")
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        return sources\n",
        encoding="utf-8",
    )


CONFIG = """\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{{ env.UBUNYE_DOCTOR_TEST_ROOT }}/in.csv"
      file_format: csv
  outputs:
    out:
      format: s3
      path: "{{ env.UBUNYE_DOCTOR_TEST_OUT | default('/tmp/out') }}"
      file_format: parquet
"""


# --- the pieces -----------------------------------------------------------------


def test_required_env_references_ignores_ones_with_a_default():
    raw = "a: '{{ env.A }}'\nb: \"{{ env.B | default('x') }}\"\nc: '{{env.C|default(1)}}'"
    assert required_env_references(raw) == {"A"}


def test_a_variable_used_once_without_a_default_is_required():
    raw = "a: \"{{ env.A | default('x') }}\"\nb: '{{ env.A }}'"
    assert required_env_references(raw) == {"A"}


@pytest.mark.parametrize(
    "banner, major",
    [
        ('openjdk version "17.0.2" 2022-01-18', 17),
        ('openjdk version "21" 2023-09-19', 21),
        ('java version "1.8.0_292"', 8),
        ('openjdk version "11.0.24" 2024-07-16 LTS', 11),
        ("no version here", None),
    ],
)
def test_java_major_is_read_from_the_version_banner(banner, major):
    assert doctor.java_major(banner) == major


@pytest.mark.parametrize(
    "spark, java, ok",
    [
        ("4.0.1", 17, True),
        ("4.1.0", 21, True),
        ("4.0.0", 11, False),
        ("3.5.3", 11, True),
        ("3.5.3", 17, True),
        ("3.5.3", 8, True),
        ("3.5.3", 21, False),
    ],
)
def test_java_must_suit_the_installed_spark(spark, java, ok):
    assert doctor.java_suits_spark(spark, java) is ok


@pytest.mark.parametrize(
    "spark, delta, ok",
    [
        ("4.0.1", "4.0.0", True),
        ("3.5.3", "3.2.0", True),
        ("4.0.1", "3.2.0", False),
        ("3.5.3", "4.0.0", False),
    ],
)
def test_delta_must_pair_with_spark(spark, delta, ok):
    assert doctor.delta_pairs_with_spark(spark, delta) is ok


def test_a_plugin_that_cannot_load_is_reported(monkeypatch):
    class Broken:
        name = "fancy"
        value = "fancy_pkg:Reader"

        def load(self):
            raise ImportError("No module named 'fancy_pkg'")

    def entry_points(group):
        return [Broken()] if group == "ubunye.readers" else []

    monkeypatch.setattr(doctor.md, "entry_points", entry_points)
    checks = doctor.plugin_checks()
    bad = [c for c in checks if c.status == doctor.WARN]
    assert len(bad) == 1
    assert "fancy" in bad[0].name and "fancy_pkg" in bad[0].detail


# --- the command ----------------------------------------------------------------


def test_doctor_json_is_one_document_and_exits_0_when_nothing_fails(quiet_environment):
    result = runner.invoke(app, ["doctor", "--json"])
    assert result.exit_code == 0, result.output
    doc = json.loads(result.output)
    assert doc["ok"] is True
    assert {c["name"] for c in doc["checks"]} == {"python", "backend: pandas", "plugins"}
    assert set(doc["checks"][0]) >= {"name", "status", "detail", "fix"}


def test_doctor_text_marks_each_check(quiet_environment):
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0, result.output
    assert "[OK] python" in ANSI.sub("", result.output)


def test_no_usable_backend_is_a_failure(monkeypatch, quiet_environment):
    monkeypatch.setattr(
        doctor.backends_view,
        "describe_all",
        lambda: [
            {"name": "spark", "loaded": False, "default": True, "error": "needs pyspark"},
            {"name": "pandas", "loaded": False, "default": False, "error": "needs pandas"},
        ],
    )
    monkeypatch.setattr(doctor, "backend_checks", doctor._backend_checks)
    result = runner.invoke(app, ["doctor", "--json"])
    assert result.exit_code == 1
    doc = json.loads(result.output)
    assert doc["ok"] is False
    assert any(c["status"] == "fail" and "no backend" in c["detail"] for c in doc["checks"])


def test_an_unusable_default_backend_says_which_flag_to_pass(monkeypatch, quiet_environment):
    monkeypatch.setattr(
        doctor.backends_view,
        "describe_all",
        lambda: [
            {"name": "pandas", "loaded": True, "default": False, "capabilities": {}},
            {
                "name": "spark",
                "loaded": False,
                "default": True,
                "error": "needs pyspark (pip install 'ubunye-engine[spark]')",
            },
        ],
    )
    monkeypatch.setattr(doctor, "backend_checks", doctor._backend_checks)
    result = runner.invoke(app, ["doctor", "--json"])
    assert result.exit_code == 0, result.output
    checks = {c["name"]: c for c in json.loads(result.output)["checks"]}
    spark = checks["backend: spark (default)"]
    assert spark["detail"] == "needs pyspark"
    assert spark["fix"] == "pip install 'ubunye-engine[spark]'"
    assert checks["default backend"]["status"] == "warn"
    assert "--backend pandas" in checks["default backend"]["fix"]


def test_an_unset_variable_a_task_needs_fails_and_is_named(
    tmp_path, monkeypatch, quiet_environment
):
    monkeypatch.delenv("UBUNYE_DOCTOR_TEST_ROOT", raising=False)
    monkeypatch.delenv("UBUNYE_DOCTOR_TEST_OUT", raising=False)
    _task(tmp_path, CONFIG)
    args = ["doctor", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t1", "--json"]
    result = runner.invoke(app, args)
    assert result.exit_code == 1, result.output
    doc = json.loads(result.output)
    env = next(c for c in doc["checks"] if c["name"] == "task t1: environment")
    assert env["status"] == "fail"
    assert "UBUNYE_DOCTOR_TEST_ROOT" in env["detail"]
    # The one with a default is not asked for.
    assert "UBUNYE_DOCTOR_TEST_OUT" not in env["detail"]


def test_a_task_with_everything_set_passes(tmp_path, monkeypatch, quiet_environment):
    monkeypatch.setenv("UBUNYE_DOCTOR_TEST_ROOT", str(tmp_path))
    _task(tmp_path, CONFIG)
    args = ["doctor", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t1", "--json"]
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.output
    names = {c["name"]: c["status"] for c in json.loads(result.output)["checks"]}
    assert names["task t1: environment"] == "ok"
    assert names["task t1: config"] == "ok"


def test_an_invalid_task_config_fails(tmp_path, monkeypatch, quiet_environment):
    monkeypatch.setenv("UBUNYE_DOCTOR_TEST_ROOT", str(tmp_path))
    _task(tmp_path, CONFIG.replace("MODEL: etl", "MODEL: not-a-model"))
    args = ["doctor", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t1", "--json"]
    result = runner.invoke(app, args)
    assert result.exit_code == 1
    config = next(c for c in json.loads(result.output)["checks"] if c["name"] == "task t1: config")
    assert config["status"] == "fail"


def test_a_missing_task_folder_fails(tmp_path, quiet_environment):
    args = ["doctor", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "nope", "--json"]
    result = runner.invoke(app, args)
    assert result.exit_code == 1
    check = json.loads(result.output)["checks"][-1]
    assert check["status"] == "fail" and "config.yaml" in check["detail"]


def test_the_real_environment_checks_run_here():
    """No mocks: doctor must not crash on whatever this machine has."""
    result = runner.invoke(app, ["doctor", "--json"])
    doc = json.loads(result.output)
    assert result.exit_code == (0 if doc["ok"] else 1)
    assert any(c["name"] == "python" for c in doc["checks"])
