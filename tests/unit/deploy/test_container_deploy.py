"""`ubunye deploy k8s|container-apps|emr-serverless` and the `container` image."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.deploy import containers, package

runner = CliRunner()


def test_the_k8s_job_runs_the_entry_with_the_task_and_env():
    plan = containers.plan_k8s(
        "uc", "pkg", "my_task", image="img:1", namespace="jobs",
        env={"UBUNYE_SINK": "s3"}, dt="2026-07-13", job="ubunye-my-task-1",
    )  # fmt: skip
    manifest = json.loads(plan.commands[0][3])
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert manifest["kind"] == "Job" and manifest["metadata"]["namespace"] == "jobs"
    assert manifest["spec"]["backoffLimit"] == 0
    assert container["image"] == "img:1"
    assert container["args"] == ["--task", "uc/pkg/my_task", "--mode", "PROD", "--dt", "2026-07-13"]
    assert container["env"] == [{"name": "UBUNYE_SINK", "value": "s3"}]


def test_job_names_are_dns_safe():
    assert containers._name("Document_Index v2") == "ubunye-document-index-v2"


def test_container_apps_passes_the_arguments_in_an_env_var():
    plan = containers.plan_container_apps(
        "uc", "pkg", "t", image="acr.io/i:1", resource_group="rg", environment="env",
        registry_identity="/subs/x/identity", env={"A": "1"},
    )  # fmt: skip
    spec = json.loads(plan.commands[0][3])
    assert spec["env"][0] == "A=1"
    key, _, value = spec["env"][1].partition("=")
    assert key == "UBUNYE_ENTRY_ARGS"
    assert json.loads(value) == ["--task", "uc/pkg/t", "--mode", "PROD"]
    assert spec["registry_identity"] == "/subs/x/identity"


def test_emr_serverless_starts_the_entry_in_the_image():
    plan = containers.plan_emr_serverless(
        "uc", "pkg", "t", application_id="app", role="arn:role", bucket="b",
        env={"UBUNYE_DATA_ROOT": "s3://b/data"}, region="eu-west-1",
    )  # fmt: skip
    command = plan.commands[0]
    driver = json.loads(command[command.index("--job-driver") + 1])["sparkSubmit"]
    assert driver["entryPoint"] == "local:///app/ubunye_entry.py"
    assert driver["entryPointArguments"] == ["--task", "uc/pkg/t", "--mode", "PROD"]
    params = driver["sparkSubmitParameters"]
    assert "spark.emr-serverless.driverEnv.UBUNYE_DATA_ROOT=s3://b/data" in params
    assert "delta-spark_2.12-3.2.0.jar" in params
    assert command[-2:] == ["--region", "eu-west-1"]


def test_the_entry_script_reads_its_arguments_from_the_environment(tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (tmp_path / "in.csv").write_text("id\n1\n2\n", encoding="utf-8")
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class Copy(Task):\n    def transform(self, sources):\n        return {'out': sources['src']}\n",
        encoding="utf-8",
    )
    (task / "config.yaml").write_text(
        "CONFIG:\n"
        f"  inputs:\n    src: {{format: s3, path: '{(tmp_path / 'in.csv').as_posix()}', file_format: csv, options: {{header: 'true'}}}}\n"
        f"  outputs:\n    out: {{format: s3, path: '{(tmp_path / 'out').as_posix()}', file_format: parquet, mode: overwrite}}\n",
        encoding="utf-8",
    )
    entry = tmp_path / "ubunye_entry.py"
    entry.write_text(package.ENTRY_SCRIPT, encoding="utf-8")
    env = dict(
        os.environ, UBUNYE_ENTRY_ARGS=json.dumps(["--task", "uc/pkg/t", "--backend", "pandas"])
    )
    done = subprocess.run(
        [sys.executable, str(entry)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert done.returncode == 0, done.stderr[-2000:]
    assert package.read_record(done.stdout)["outputs"][0]["row_count"] == 2


def test_the_container_image_is_self_contained():
    text = package.dockerfile("container", engine="ubunye-engine==0.7.0")
    for piece in (
        "openjdk-17-jre-headless",
        '"pyspark==3.5.*" "delta-spark==3.2.0"',
        "PYSPARK_SUBMIT_ARGS=",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        'ENTRYPOINT ["python", "/app/ubunye_entry.py"]',
        "WORKDIR /app/pipelines",
        'ARG UBUNYE_ENGINE="ubunye-engine==0.7.0"',
    ):
        assert piece in text, piece


@pytest.mark.parametrize(
    "args",
    [
        ["deploy", "k8s", "-u", "uc", "-p", "pkg", "-t", "t", "--image", "i", "--dry-run"],
        ["deploy", "container-apps", "-u", "uc", "-p", "pkg", "-t", "t", "--image", "i",
         "-g", "rg", "--environment", "e", "--dry-run"],
        ["deploy", "emr-serverless", "-u", "uc", "-p", "pkg", "-t", "t", "--application-id", "a",
         "--role", "r", "--bucket", "b", "--dry-run"],
    ],
)  # fmt: skip
def test_each_dry_run_prints_a_plan(args):
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.output
    plan = json.loads(result.output.split("\n\n[DRY RUN]")[0])
    assert plan["commands"] and not plan["uploads"]


def test_dockerfile_container_writes_the_entry_script_too(tmp_path):
    out = Path(tmp_path) / "Dockerfile"
    assert (
        runner.invoke(app, ["deploy", "dockerfile", "container", "--out", str(out)]).exit_code == 0
    )
    assert (tmp_path / "ubunye_entry.py").read_text(encoding="utf-8") == package.ENTRY_SCRIPT
