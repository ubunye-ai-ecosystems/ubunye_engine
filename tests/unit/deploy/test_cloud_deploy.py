"""`ubunye deploy glue|dataproc|dockerfile`: the plans, the bundle, the entry script.

The entry script is run for real here, from a folder that is not the task's, the
way a cloud job runs it: it must unpack the bundle, apply --env, run the task and
print a run record that `ubunye gate` can read.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.deploy import cloud, package

runner = CliRunner()

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path) -> Path:
    task = root / "pipelines" / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id\n1\n2\n3\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "__pycache__").mkdir()
    (task / "__pycache__" / "junk.pyc").write_bytes(b"x")
    (task / "config.yaml").write_text(
        """\
CONFIG:
  inputs:
    src: {format: s3, path: "{{ env.UBUNYE_TEST_ROOT }}/in.csv", file_format: csv, options: {header: "true"}}
  outputs:
    out: {format: s3, path: "{{ env.UBUNYE_TEST_ROOT }}/out", file_format: parquet, mode: overwrite}
""",
        encoding="utf-8",
    )
    return root / "pipelines"


# --- the bundle and the entry script -------------------------------------------------------


def test_the_bundle_keeps_the_task_path_and_drops_caches(tmp_path):
    names = zipfile.ZipFile(
        io.BytesIO(package.bundle(_task(tmp_path), "uc", "pkg", "t"))
    ).namelist()
    assert sorted(names) == ["uc/pkg/t/config.yaml", "uc/pkg/t/transformations.py"]


def test_the_entry_script_runs_the_task_and_prints_its_record(tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    pipelines = _task(tmp_path)
    job = tmp_path / "job"
    job.mkdir()
    (job / "bundle.zip").write_bytes(package.bundle(pipelines, "uc", "pkg", "t"))
    (job / "ubunye_entry.py").write_text(package.ENTRY_SCRIPT, encoding="utf-8")
    elsewhere = tmp_path / "cwd"
    elsewhere.mkdir()
    done = subprocess.run(
        [
            sys.executable, str(job / "ubunye_entry.py"),
            "--task", "uc/pkg/t", "--bundle", str(job / "bundle.zip"), "--backend", "pandas",
            "--env", f"UBUNYE_TEST_ROOT={tmp_path.as_posix()}",
            "--JOB_ID", "ignored-platform-argument",
        ],
        cwd=elsewhere, capture_output=True, text=True, timeout=300,
    )  # fmt: skip
    assert done.returncode == 0, done.stderr[-2000:]
    record = package.read_record(done.stdout)
    assert record["status"] == "success" and record["task_path"] == "uc/pkg/t"
    [out] = record["outputs"]
    assert out["row_count"] == 3 and out["data_hash"].startswith("sha256:")
    assert (tmp_path / "out").exists()


def test_a_record_is_read_back_through_log_prefixes():
    log = (
        "noise\n" + package.RECORD_BEGIN + "\n"
        '2026-09-24 INFO {"status": "success", "outputs": []}\n' + package.RECORD_END + "\n"
    )
    assert package.read_record(log) == {"status": "success", "outputs": []}
    assert package.read_record("no record here") is None


# --- the plans --------------------------------------------------------------------------------


def test_the_glue_plan(tmp_path):
    wheel = tmp_path / "ubunye_engine-0.7.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    plan = cloud.plan_glue(
        _task(tmp_path), "uc", "pkg", "t",
        bucket="b", role="arn:aws:iam::1:role/r", region="eu-west-1",
        engine_wheel=wheel, env={"UBUNYE_SINK": "s3"}, dt="2026-07-13",
    )  # fmt: skip
    assert [uri for uri, _ in plan.uploads] == [
        "s3://b/ubunye/uc/pkg/t/bundle.zip",
        "s3://b/ubunye/uc/pkg/t/ubunye_entry.py",
        "s3://b/ubunye/uc/pkg/t/ubunye_engine-0.7.0-py3-none-any.whl",
    ]
    upsert, run = plan.commands
    spec = json.loads(upsert[4])
    args = spec["DefaultArguments"]
    assert spec["GlueVersion"] == "5.0" and spec["Role"] == "arn:aws:iam::1:role/r"
    assert (
        args["--additional-python-modules"]
        == "s3://b/ubunye/uc/pkg/t/ubunye_engine-0.7.0-py3-none-any.whl"
    )
    assert args["--datalake-formats"] == "delta" and args["--dt"] == "2026-07-13"
    assert json.loads(args["--env_json"]) == {"UBUNYE_SINK": "s3"}
    assert upsert[-2:] == ["--region", "eu-west-1"] and run[-2:] == ["--region", "eu-west-1"]
    assert plan.job == "ubunye-uc-pkg-t"


def test_the_dataproc_plan(tmp_path):
    plan = cloud.plan_dataproc(
        _task(tmp_path), "uc", "pkg", "t",
        project="p", region="europe-west1", bucket="b", image="img:1",
        env={"UBUNYE_DATA_ROOT": "gs://b/data"}, batch="ubunye-t-1",
    )  # fmt: skip
    [command] = plan.commands
    assert command[:5] == ["gcloud", "dataproc", "batches", "submit", "pyspark"]
    assert command[command.index("--container-image") + 1] == "img:1"
    assert command[command.index("--archives") + 1] == "gs://b/ubunye/uc/pkg/t/bundle.zip#bundle"
    props = next(c for c in command if c.startswith("--properties="))
    assert props.startswith(
        "--properties=^#^spark.jars=file:///opt/jars/delta-spark_2.13-3.2.0.jar,"
    )
    assert "spark.dataproc.driverEnv.UBUNYE_DATA_ROOT=gs://b/data" in props.split("#")
    assert command[command.index("--") + 1 :] == ["--task", "uc/pkg/t", "--mode", "PROD"]


def test_execute_uploads_then_runs_and_keeps_the_last_log(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(cloud, "_upload", lambda uri, data: calls.append(("upload", uri)))
    monkeypatch.setattr(
        cloud,
        "_run",
        lambda argv, capture=False: calls.append(("run", argv[0])) or ("LOG" if capture else ""),
    )
    plan = cloud.plan_glue(_task(tmp_path), "uc", "pkg", "t", bucket="b", role="r")
    cloud.execute(plan)
    assert [c[0] for c in calls] == ["upload", "upload", "run", "run"]
    assert plan.log == "LOG"


def test_a_missing_cli_says_what_to_install(monkeypatch):
    monkeypatch.setattr(cloud.shutil, "which", lambda name: None)
    with pytest.raises(cloud.DeployError, match="`gcloud` is not on PATH"):
        cloud._cli("gcloud")


# --- the commands -----------------------------------------------------------------------------


def test_dry_run_prints_the_plan_and_runs_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(cloud, "execute", lambda plan: pytest.fail("a dry run ran something"))
    args = ["deploy", "dataproc", "-d", str(_task(tmp_path)), "-u", "uc", "-p", "pkg", "-t", "t",
            "--project", "p", "--region", "r", "--bucket", "b", "--image", "i", "--dry-run"]  # fmt: skip
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.output
    plan = json.loads(result.output.split("\n\n[DRY RUN]")[0])
    assert plan["platform"] == "dataproc" and len(plan["uploads"]) == 2


@pytest.mark.parametrize(
    "platform, must_have",
    [("dataproc", ["FROM debian:12-slim", "useradd -u 1099", "delta-spark_2.13"]),
     ("emr-serverless", ["emr-serverless/spark", "USER hadoop:hadoop", "delta-spark_2.12"])],
)  # fmt: skip
def test_the_dockerfile_follows_each_platforms_rules(tmp_path, platform, must_have):
    out = tmp_path / "Dockerfile"
    result = runner.invoke(app, ["deploy", "dockerfile", platform, "--out", str(out)])
    assert result.exit_code == 0, result.output
    text = out.read_text(encoding="utf-8")
    assert all(piece in text for piece in must_have), text
    assert (tmp_path / "ubunye_entry.py").read_text(encoding="utf-8") == package.ENTRY_SCRIPT


def test_a_record_is_found_when_log_lines_come_back_out_of_order():
    """Log Analytics returns lines with the same timestamp in any order.

    The entry prints begin, the record and end in the same instant; Azure gave them
    back as begin, end, record, and the record was lost.
    """
    log = (
        package.RECORD_BEGIN + "\n" + package.RECORD_END + "\n"
        '{"run_id": "r1", "status": "success", "outputs": []}\n'
    )
    assert package.read_record(log) == {"run_id": "r1", "status": "success", "outputs": []}


def test_a_run_whose_record_is_missing_fails_when_one_was_asked_for(tmp_path, monkeypatch):
    def ran(plan):
        plan.log = package.RECORD_BEGIN + "\n" + package.RECORD_END + "\n"

    monkeypatch.setattr(cloud, "execute", ran)
    out = tmp_path / "rec.json"
    args = ["deploy", "dataproc", "-d", str(_task(tmp_path)), "-u", "uc", "-p", "pkg", "-t", "t",
            "--project", "p", "--region", "r", "--bucket", "b", "--image", "i",
            "--record-out", str(out)]  # fmt: skip
    result = runner.invoke(app, args)
    assert result.exit_code == 1
    assert "run record" in result.output and "not found" in result.output
    assert not out.exists()
