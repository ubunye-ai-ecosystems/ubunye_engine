"""Deploy a task to a serverless Spark service: AWS Glue, GCP Dataproc Serverless.

Each deploy is a :class:`Plan`: files to upload, then cloud CLI calls (``aws``,
``gcloud``), run in order. ``--dry-run`` prints the plan and runs nothing. The
CLIs log in the way they always do (profiles, ``gcloud auth``, workload
identity in CI); the engine never takes a credential.

These are the recipes proven by hand on the sandboxes before they became a
command: the same task, unchanged, gives the same data on every platform.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ubunye.core.errors import DeployError
from ubunye.deploy import package

DELTA_VERSION = "3.2.0"


@dataclass
class Plan:
    """What a deploy will do, in order: upload these, then run these."""

    platform: str
    job: str
    uploads: List[Tuple[str, bytes]] = field(default_factory=list)  # (remote URI, content)
    commands: List[List[str]] = field(default_factory=list)
    #: Filled by the run: what the job printed, when it was followed to the end.
    log: str = ""

    def describe(self) -> Dict[str, Any]:
        return {
            "platform": self.platform,
            "job": self.job,
            "uploads": [{"to": uri, "bytes": len(data)} for uri, data in self.uploads],
            "commands": self.commands,
        }


def _cli(name: str) -> str:
    found = shutil.which(name) or (shutil.which(name + ".cmd") if sys.platform == "win32" else None)
    if not found:
        raise DeployError(
            f"`{name}` is not on PATH; this deploy runs the {name} CLI.",
            hint=f"Install the {name} CLI and log in, or run with --dry-run to see the plan.",
        )
    return found


def _run(argv: List[str], capture: bool = False) -> str:
    # "python" is this Python: the helper programs must not pick another one.
    exe = [sys.executable if argv[0] == "python" else _cli(argv[0])] + argv[1:]
    done = subprocess.run(exe, capture_output=capture, text=True, check=False)
    if capture:
        # gcloud streams the job's output to stderr; keep both, and show them.
        text = (done.stdout or "") + (done.stderr or "")
        sys.stdout.write(text)
    if done.returncode != 0:
        detail = (done.stderr or done.stdout or "").strip()[-2000:] if capture else ""
        raise DeployError(
            f"`{' '.join(argv[:4])} ...` failed with exit code {done.returncode}.",
            context={"Output": detail} if detail else None,
        )
    return text if capture else ""


def _upload(uri: str, data: bytes) -> None:
    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uri).suffix) as fh:
        fh.write(data)
        local = fh.name
    try:
        if uri.startswith("s3://"):
            _run(["aws", "s3", "cp", local, uri, "--only-show-errors"])
        elif uri.startswith("gs://"):
            _run(["gcloud", "storage", "cp", local, uri, "--quiet"])
        else:
            raise DeployError(f"cannot upload to {uri}")
    finally:
        Path(local).unlink(missing_ok=True)


def execute(plan: Plan) -> Plan:
    """Upload, then run each command; the last command's output is the job's log."""
    for uri, data in plan.uploads:
        _upload(uri, data)
    for argv in plan.commands[:-1]:
        _run(argv)
    if plan.commands:
        plan.log = _run(plan.commands[-1], capture=True)
    return plan


def _entry_args(
    task: str, mode: str, dt: Optional[str], env: Dict[str, str], variables: Dict[str, str]
) -> List[str]:
    args = ["--task", task, "--mode", mode]
    if dt:
        args += ["--dt", dt]
    for key, value in env.items():
        args += ["--env", f"{key}={value}"]
    for key, value in variables.items():
        args += ["--var", f"{key}={value}"]
    return args


# --- AWS Glue --------------------------------------------------------------------------


def plan_glue(
    usecase_dir: Path,
    usecase: str,
    pkg: str,
    task: str,
    *,
    bucket: str,
    role: str,
    region: Optional[str] = None,
    job: Optional[str] = None,
    engine: str = "ubunye-engine",
    engine_wheel: Optional[Path] = None,
    glue_version: str = "5.0",
    workers: int = 2,
    worker_type: str = "G.1X",
    mode: str = "PROD",
    dt: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    variables: Optional[Dict[str, str]] = None,
    wait: bool = True,
) -> Plan:
    """A Glue job (Glue 5: Spark 3.5, Python 3.11) that runs the task once.

    The engine is pip-installed by Glue (``--additional-python-modules``): a PyPI
    requirement, or a local wheel uploaded next to the task. Delta comes from
    Glue itself (``--datalake-formats delta``).
    """
    path = package.task_path(usecase, pkg, task)
    job = job or f"ubunye-{usecase}-{pkg}-{task}".replace("_", "-")
    prefix = f"s3://{bucket}/ubunye/{path}"
    plan = Plan("glue", job)
    plan.uploads.append((f"{prefix}/bundle.zip", package.bundle(usecase_dir, usecase, pkg, task)))
    plan.uploads.append((f"{prefix}/ubunye_entry.py", package.ENTRY_SCRIPT.encode("utf-8")))
    modules = engine
    if engine_wheel is not None:
        wheel_uri = f"{prefix}/{Path(engine_wheel).name}"
        plan.uploads.append((wheel_uri, Path(engine_wheel).read_bytes()))
        modules = wheel_uri
    conf = (
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf "
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    arguments = {
        "--additional-python-modules": modules,
        "--datalake-formats": "delta",
        "--conf": conf,
        "--task": path,
        "--mode": mode,
        "--bundle": f"{prefix}/bundle.zip",
        "--env_json": json.dumps(env or {}),
        "--var_json": json.dumps(variables or {}),
        "--enable-continuous-cloudwatch-log": "false",
    }
    if dt:
        arguments["--dt"] = dt
    spec = {
        "Role": role,
        "GlueVersion": glue_version,
        "WorkerType": worker_type,
        "NumberOfWorkers": workers,
        "Timeout": 60,
        "MaxRetries": 0,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": f"{prefix}/ubunye_entry.py",
            "PythonVersion": "3",
        },
        "DefaultArguments": arguments,
    }
    region_args = ["--region", region] if region else []
    plan.commands.append(["python", "-c", _GLUE_UPSERT, job, json.dumps(spec), *region_args])
    plan.commands.append(
        ["python", "-c", _GLUE_RUN, job, "wait" if wait else "nowait", *region_args]
    )
    return plan


# Small programs run with the same Python, through the aws CLI, so a deploy needs
# no AWS SDK in the engine. They stay tiny: create-or-update, then start-and-follow.
_GLUE_UPSERT = r"""
import json, subprocess, sys
job, spec, region = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3:]
def aws(*args):
    return ["aws", "glue", *args, *region]
exists = subprocess.run(aws("get-job", "--job-name", job), capture_output=True).returncode == 0
if exists:
    cmd = aws("update-job", "--job-name", job, "--job-update", json.dumps(spec))
else:
    cmd = aws("create-job", "--cli-input-json", json.dumps({"Name": job, **spec}))
subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
print(("updated" if exists else "created"), "Glue job", job, file=sys.stderr)
"""

_GLUE_RUN = r"""
import json, subprocess, sys, time
job, wait, region = sys.argv[1], sys.argv[2], sys.argv[3:]
def aws(*args):
    return ["aws", *args, *region]
run = json.loads(subprocess.run(aws("glue", "start-job-run", "--job-name", job),
                                check=True, capture_output=True, text=True).stdout)["JobRunId"]
print("started", run, file=sys.stderr)
if wait != "wait":
    sys.exit(0)
while True:
    state = json.loads(subprocess.run(aws("glue", "get-job-run", "--job-name", job, "--run-id", run),
                                      check=True, capture_output=True, text=True).stdout)["JobRun"]
    print(time.strftime("%H:%M:%S"), state["JobRunState"], file=sys.stderr)
    if state["JobRunState"] not in ("STARTING", "RUNNING", "STOPPING", "WAITING"):
        break
    time.sleep(20)
time.sleep(10)
out = subprocess.run(aws("logs", "get-log-events", "--log-group-name", "/aws-glue/jobs/output",
                          "--log-stream-name", run, "--query", "events[].message", "--output", "json"),
                     capture_output=True, text=True)
for message in (json.loads(out.stdout) if out.returncode == 0 and out.stdout.strip() else []):
    print(message)
if state["JobRunState"] != "SUCCEEDED":
    print("Glue run", run, state["JobRunState"], state.get("ErrorMessage", ""), file=sys.stderr)
    sys.exit(1)
"""


# --- GCP Dataproc Serverless ----------------------------------------------------------


def plan_dataproc(
    usecase_dir: Path,
    usecase: str,
    pkg: str,
    task: str,
    *,
    project: str,
    region: str,
    bucket: str,
    image: str,
    service_account: Optional[str] = None,
    runtime: str = "2.2",
    batch: Optional[str] = None,
    mode: str = "PROD",
    dt: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    variables: Optional[Dict[str, str]] = None,
    wait: bool = True,
) -> Plan:
    """A Dataproc Serverless batch that runs the task once, in ``image``.

    The image carries the engine and Delta (``ubunye deploy dockerfile dataproc``
    writes its Dockerfile); Dataproc mounts Spark and Java. The task arrives as an
    archive Dataproc unpacks next to the entry script.
    """
    path = package.task_path(usecase, pkg, task)
    batch = batch or f"ubunye-{task}-{int(time.time())}".replace("_", "-").lower()
    prefix = f"gs://{bucket}/ubunye/{path}"
    plan = Plan("dataproc", batch)
    plan.uploads.append((f"{prefix}/bundle.zip", package.bundle(usecase_dir, usecase, pkg, task)))
    plan.uploads.append((f"{prefix}/ubunye_entry.py", package.ENTRY_SCRIPT.encode("utf-8")))
    jars = ",".join(
        f"file:///opt/jars/{j}"
        for j in (f"delta-spark_2.13-{DELTA_VERSION}.jar", f"delta-storage-{DELTA_VERSION}.jar")
    )
    props = [
        f"spark.jars={jars}",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    ]
    for key, value in (env or {}).items():
        props += [f"spark.dataproc.driverEnv.{key}={value}", f"spark.executorEnv.{key}={value}"]
    command = [
        "gcloud", "dataproc", "batches", "submit", "pyspark", f"{prefix}/ubunye_entry.py",
        "--project", project, "--region", region, "--batch", batch,
        "--container-image", image, "--version", runtime,
        "--archives", f"{prefix}/bundle.zip#bundle",
        "--deps-bucket", bucket,
        # "#" separates properties: the jar list itself has commas.
        "--properties=^#^" + "#".join(props),
    ]  # fmt: skip
    if service_account:
        command += ["--service-account", service_account]
    if not wait:
        command.append("--async")
    command += ["--", *_entry_args(path, mode, dt, {}, variables or {})]
    plan.commands.append(command)
    return plan
