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


@pytest.mark.parametrize("exists", [False, True])
def test_container_apps_sets_env_vars_the_way_create_and_update_each_take_them(monkeypatch, exists):
    """`job create` takes --env-vars; `job update` refuses it and takes --replace-env-vars.

    The first live run created the job and passed; every later run updated it and
    failed with "unrecognized arguments: --env-vars".
    """
    calls = []

    def fake_run(argv, capture_output=False, text=False):
        calls.append(argv)
        code = 0 if (argv[2:4] != ["job", "show"] or exists) else 3
        out = "exec-1\n" if argv[2:4] == ["job", "start"] else ""
        return subprocess.CompletedProcess(argv, code, stdout=out, stderr="")

    settings = {
        "job": "ubunye-t", "resource_group": "rg", "environment": "env", "image": "img",
        "cpu": "0.5", "memory": "1Gi", "timeout": 600, "env": ["A=1", "B=2"],
        "registry_identity": "", "registry_server": "", "wait": False,
    }  # fmt: skip
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["-c", json.dumps(settings)])
    with pytest.raises(SystemExit) as done:
        exec(containers._ACA_RUN, {"__name__": "__main__"})
    assert done.value.code == 0
    (write,) = [c for c in calls if c[2:4] in (["job", "create"], ["job", "update"])]
    assert write[3] == ("update" if exists else "create")
    flag = "--replace-env-vars" if exists else "--env-vars"
    assert write[write.index(flag) + 1 : write.index(flag) + 3] == ["A=1", "B=2"]
    other = "--env-vars" if exists else "--replace-env-vars"
    assert other not in write


def test_container_jobs_run_several_tasks_in_one_launch():
    k8s = containers.plan_k8s("uc", "pkg", ["clean", "monitor"], image="img:1")
    manifest = json.loads(k8s.commands[0][3])
    args = manifest["spec"]["template"]["spec"]["containers"][0]["args"]
    assert args[:2] == ["--task", "uc/pkg/clean,uc/pkg/monitor"]
    assert k8s.job.startswith("ubunye-clean-monitor-")
    aca = containers.plan_container_apps(
        "uc", "pkg", ["clean", "monitor"], image="i", resource_group="rg", environment="e"
    )
    spec = json.loads(aca.commands[0][3])
    assert json.loads(spec["env"][-1].partition("=")[2])[:2] == [
        "--task",
        "uc/pkg/clean,uc/pkg/monitor",
    ]
    assert aca.job.startswith("ubunye-clean-monitor-")


@pytest.mark.parametrize("condition, code", [("Failed", 1), ("Complete", 0)])
def test_a_k8s_job_is_followed_to_failed_as_well_as_complete(monkeypatch, condition, code):
    """F-037: `kubectl wait --for=condition=complete` sat out the whole timeout (30
    minutes on kind) after R1's Job had already failed. Kubernetes says a Job ended with
    a Complete or a Failed condition; the deploy stops at either, and fails on Failed.
    """
    import time as time_module

    calls = []
    clock = [0.0]

    def fake_run(argv, input=None, capture_output=False, text=False, check=False):
        calls.append(argv)
        out = ""
        if argv[1] == "wait":  # the old way: never returns for a failed Job
            clock[0] += 1800
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="timed out")
        if argv[1] == "get":
            out = condition if len([c for c in calls if c[1] == "get"]) > 2 else ""
        if argv[1] == "logs":
            out = "the job's log"
        return subprocess.CompletedProcess(argv, 0, stdout=out, stderr="")

    def fake_sleep(seconds):
        clock[0] += seconds

    plan = containers.plan_k8s("uc", "pkg", "t", image="img:1", timeout_s=1800)
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(time_module, "sleep", fake_sleep)
    monkeypatch.setattr(time_module, "time", lambda: clock[0])
    monkeypatch.setattr(sys, "argv", plan.commands[0][2:])
    try:
        exec(containers._K8S_RUN, {"__name__": "__main__"})
        exit_code = 0
    except SystemExit as done:
        exit_code = done.code
    assert exit_code == code
    assert clock[0] < 60, "waited out the timeout instead of stopping at the Job's end"
    assert ["kubectl", "logs", "-n", "default", f"job/{plan.job}"] in calls
