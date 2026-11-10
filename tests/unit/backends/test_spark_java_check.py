"""Spark with no usable Java fails before it starts, in one clean message (F-112).

With no Java, `ubunye run` printed pyspark's raw `[JAVA_GATEWAY_EXITED]` traceback,
and `ubunye doctor` called the spark backend usable. The backend now looks for Java
(JAVA_HOME, then java on PATH) and its version before it starts a session, and the
doctor asks the same function.
"""

from __future__ import annotations

import os
import re
import stat
import sys
import types

import pytest
from typer.testing import CliRunner

from ubunye.backends import spark_backend
from ubunye.backends.spark_backend import SparkBackend
from ubunye.cli import backend_choice
from ubunye.core.errors import SparkSessionError

try:  # Click before 8.2 mixes stderr into stdout unless asked not to
    runner = CliRunner(mix_stderr=False)
except TypeError:  # Click 8.2+ always keeps them apart
    runner = CliRunner()
_ANSI = re.compile(r"\x1b\[[0-9;]*m")

pytestmark = pytest.mark.skipif(os.name == "nt", reason="fakes java with a shell script")


@pytest.fixture
def no_java(monkeypatch, tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setenv("PATH", str(empty))


@pytest.fixture
def java_home(monkeypatch, tmp_path):
    """JAVA_HOME pointing at a fake `java` that prints the given version banner."""

    def make(version: str) -> None:
        home = tmp_path / "jdk"
        (home / "bin").mkdir(parents=True)
        java = home / "bin" / "java"
        java.write_text(
            f"#!/bin/sh\necho 'openjdk version \"{version}\" 2024-01-01' >&2\n", encoding="utf-8"
        )
        java.chmod(java.stat().st_mode | stat.S_IEXEC)
        monkeypatch.setenv("JAVA_HOME", str(home))

    return make


@pytest.fixture
def fake_pyspark(monkeypatch):
    """A fake `pyspark.sql` that records whether a session was asked for."""

    class Builder:
        def appName(self, _name):
            return self

        def config(self, _key, _value):
            return self

        def getOrCreate(self):
            SparkSession.created = True
            SparkSession.active = types.SimpleNamespace(stop=lambda: None)
            return SparkSession.active

    class SparkSession:
        active = None
        created = False
        builder = Builder()

        @classmethod
        def getActiveSession(cls):
            return cls.active

    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = SparkSession
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(SparkBackend, "_platform_master", staticmethod(lambda: None))
    monkeypatch.setattr(spark_backend, "_spark_version", lambda: "4.0.1")
    return SparkSession


def test_no_java_is_a_clean_error_before_spark_starts(no_java, fake_pyspark):
    with pytest.raises(SparkSessionError) as caught:
        SparkBackend().start()
    assert "Java" in str(caught.value)
    assert "Spark 4 runs on Java 17 or 21; Spark 3.5 on Java 8, 11 or 17" in str(caught.value)
    assert not fake_pyspark.created


def test_a_java_too_old_for_spark_is_a_clean_error(java_home, fake_pyspark):
    java_home("11.0.2")
    with pytest.raises(SparkSessionError, match="Java 11 .*too old for Spark 4.0.1"):
        SparkBackend().start()
    assert not fake_pyspark.created


@pytest.mark.parametrize("version", ["17.0.9", "21.0.1", "25"])
def test_a_suitable_or_newer_java_starts(java_home, fake_pyspark, version):
    java_home(version)  # newer than Spark lists is left for Spark to judge
    SparkBackend().start()
    assert fake_pyspark.created


def test_a_session_already_running_needs_no_java_here(no_java, fake_pyspark):
    fake_pyspark.active = types.SimpleNamespace(stop=lambda: None)
    SparkBackend().start()


def test_doctor_and_backends_call_spark_unusable_without_java(no_java):
    if backend_choice.backends.missing_packages(SparkBackend):
        pytest.skip("pyspark not installed")
    spark = next(row for row in backend_choice.describe_all() if row["name"] == "spark")
    assert spark["loaded"] is False
    assert "Java" in spark["error"]


def test_doctor_names_every_java_spark_supports(no_java):
    from ubunye.cli import doctor

    if not doctor._version("pyspark"):
        pytest.skip("pyspark not installed")
    java = next(c for c in doctor.environment_checks() if c.name == "java")
    assert "Spark 4 runs on Java 17 or 21; Spark 3.5 on Java 8, 11 or 17" in java.fix


def test_ubunye_run_without_java_is_one_error_line(no_java, tmp_path):
    pytest.importorskip("pyspark")
    from ubunye.cli.main import app

    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(
        "CONFIG:\n  inputs:\n    src: {format: hive, db_name: d, tbl_name: t}\n"
        "  outputs:\n    out: {format: hive, db_name: d, tbl_name: o}\n",
        encoding="utf-8",
    )
    (task / "transformations.py").write_text("pass\n", encoding="utf-8")
    args = ["run", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t", "--backend", "spark"]
    result = runner.invoke(app, args)
    err = _ANSI.sub("", result.stderr)
    assert result.exit_code == 1, (result.output, err, result.exception)
    assert "[ERROR]" in err and "Java" in err, err
    assert "Traceback" not in err + result.stdout
    assert "JAVA_GATEWAY_EXITED" not in err + result.stdout
