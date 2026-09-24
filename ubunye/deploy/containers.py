"""Deploy a task as a container job: Kubernetes, Azure Container Apps, EMR Serverless.

Kubernetes and Container Apps run the image ``ubunye deploy dockerfile container``
writes (Java, Spark in local mode, Delta, the engine and the pipelines baked in).
EMR Serverless runs the ``emr-serverless`` image in an existing application.

Like :mod:`ubunye.deploy.cloud`, each deploy is a :class:`~ubunye.deploy.cloud.Plan`
run with the platform's own CLI (``kubectl``, ``az``, ``aws``) and login; the run
record the job prints is read back from its log.
"""

from __future__ import annotations

import json
import re
import time
from typing import Dict, List, Optional

from ubunye.deploy import package
from ubunye.deploy.cloud import Plan, _entry_args


def _name(task: str, prefix: str = "ubunye") -> str:
    """A DNS-safe job name: lowercase letters, digits and dashes."""
    return re.sub(r"[^a-z0-9-]+", "-", f"{prefix}-{task}".lower()).strip("-")[:52]


# --- Kubernetes ---------------------------------------------------------------------------


def plan_k8s(
    usecase: str,
    pkg: str,
    task: str,
    *,
    image: str,
    namespace: str = "default",
    job: Optional[str] = None,
    cpu: str = "1",
    memory: str = "2Gi",
    timeout_s: int = 1800,
    mode: str = "PROD",
    dt: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    variables: Optional[Dict[str, str]] = None,
    wait: bool = True,
) -> Plan:
    """A Kubernetes Job that runs the task once, in ``image``."""
    path = package.task_path(usecase, pkg, task)
    job = job or f"{_name(task)}-{int(time.time()) % 100000}"
    manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job, "namespace": namespace, "labels": {"app": "ubunye"}},
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "metadata": {"labels": {"app": "ubunye", "ubunye/job": job}},
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "task",
                            "image": image,
                            "args": _entry_args(path, mode, dt, {}, variables or {}),
                            "env": [{"name": k, "value": v} for k, v in (env or {}).items()],
                            "resources": {
                                "requests": {"cpu": cpu, "memory": memory},
                                "limits": {"memory": memory},
                            },
                        }
                    ],
                },
            },
        },
    }
    plan = Plan("kubernetes", job)
    plan.commands.append(
        [
            "python",
            "-c",
            _K8S_RUN,
            json.dumps(manifest),
            str(timeout_s),
            "wait" if wait else "nowait",
        ]
    )
    return plan


_K8S_RUN = r"""
import json, subprocess, sys
manifest, timeout, wait = json.loads(sys.argv[1]), sys.argv[2], sys.argv[3]
name, ns = manifest["metadata"]["name"], manifest["metadata"]["namespace"]
subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(manifest), text=True, check=True)
if wait != "wait":
    sys.exit(0)
done = subprocess.run(["kubectl", "wait", "-n", ns, f"job/{name}", "--for=condition=complete",
                       f"--timeout={timeout}s"], capture_output=True, text=True)
logs = subprocess.run(["kubectl", "logs", "-n", ns, f"job/{name}"], capture_output=True, text=True)
print(logs.stdout)
print(logs.stderr, file=sys.stderr)
if done.returncode != 0:
    print(done.stderr, file=sys.stderr)
    sys.exit(1)
"""


# --- Azure Container Apps -----------------------------------------------------------------


def plan_container_apps(
    usecase: str,
    pkg: str,
    task: str,
    *,
    image: str,
    resource_group: str,
    environment: str,
    job: Optional[str] = None,
    registry_server: Optional[str] = None,
    registry_identity: Optional[str] = None,
    cpu: str = "2",
    memory: str = "4Gi",
    timeout_s: int = 1800,
    mode: str = "PROD",
    dt: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    variables: Optional[Dict[str, str]] = None,
    wait: bool = True,
) -> Plan:
    """An Azure Container Apps job that runs the task once, in ``image``.

    With ``registry_identity`` the job pulls with a managed identity (no password).
    The run record is read back from the environment's Log Analytics workspace,
    which receives a job's output a few minutes after it ends.
    """
    path = package.task_path(usecase, pkg, task)
    job = job or _name(task)[:32].rstrip("-")
    spec = {
        "job": job,
        "resource_group": resource_group,
        "environment": environment,
        "image": image,
        "cpu": cpu,
        "memory": memory,
        "timeout": timeout_s,
        "registry_server": registry_server,
        "registry_identity": registry_identity,
        # az cannot take "--task ..." as --args values; the entry reads them from here.
        "env": [f"{k}={v}" for k, v in (env or {}).items()]
        + ["UBUNYE_ENTRY_ARGS=" + json.dumps(_entry_args(path, mode, dt, {}, variables or {}))],
        "wait": wait,
    }
    plan = Plan("container-apps", job)
    plan.commands.append(["python", "-c", _ACA_RUN, json.dumps(spec)])
    return plan


_ACA_RUN = r"""
import json, subprocess, sys, time
s = json.loads(sys.argv[1])
rg = ["-g", s["resource_group"]]
def az(*args, capture=True):
    done = subprocess.run(["az", *args, "--only-show-errors"], capture_output=capture, text=True)
    if done.returncode != 0:
        print(done.stderr, file=sys.stderr)
        sys.exit(done.returncode)
    return done.stdout
exists = subprocess.run(["az", "containerapp", "job", "show", "-n", s["job"], *rg, "--only-show-errors"],
                        capture_output=True).returncode == 0
common = ["--image", s["image"], "--cpu", s["cpu"], "--memory", s["memory"],
          "--replica-timeout", str(s["timeout"]), "--env-vars", *s["env"]]
if exists:
    az("containerapp", "job", "update", "-n", s["job"], *rg, *common, "-o", "none")
else:
    extra = []
    if s["registry_identity"]:
        extra += ["--mi-user-assigned", s["registry_identity"], "--registry-identity", s["registry_identity"]]
    if s["registry_server"]:
        extra += ["--registry-server", s["registry_server"]]
    az("containerapp", "job", "create", "-n", s["job"], *rg, "--environment", s["environment"],
       "--trigger-type", "Manual", "--replica-retry-limit", "0", "--parallelism", "1",
       "--replica-completion-count", "1", *common, *extra, "-o", "none")
execution = az("containerapp", "job", "start", "-n", s["job"], *rg, "--query", "name", "-o", "tsv").strip()
print("execution", execution, file=sys.stderr)
if not s["wait"]:
    sys.exit(0)
while True:
    state = az("containerapp", "job", "execution", "show", "-n", s["job"], *rg,
               "--job-execution-name", execution, "--query", "properties.status", "-o", "tsv").strip()
    print(time.strftime("%H:%M:%S"), state, file=sys.stderr)
    if state not in ("Running", "Processing", ""):
        break
    time.sleep(20)
workspace = az("containerapp", "env", "show", "-n", s["environment"], *rg, "--query",
               "properties.appLogsConfiguration.logAnalyticsConfiguration.customerId", "-o", "tsv").strip()
query = ("ContainerAppConsoleLogs_CL | where ContainerGroupName_s startswith '" + execution +
         "' | order by TimeGenerated asc | project Log_s")
for attempt in range(40):  # Log Analytics lags a few minutes behind the job
    out = subprocess.run(["az", "monitor", "log-analytics", "query", "-w", workspace,
                          "--analytics-query", query, "--query", "[].Log_s", "-o", "tsv",
                          "--only-show-errors"], capture_output=True, text=True).stdout
    if "UBUNYE-RUN-RECORD-END" in out or (state != "Succeeded" and out.strip()):
        break
    time.sleep(20)
print(out)
if state != "Succeeded":
    print("Container Apps execution", execution, state, file=sys.stderr)
    sys.exit(1)
"""


# --- EMR Serverless -------------------------------------------------------------------------


def plan_emr_serverless(
    usecase: str,
    pkg: str,
    task: str,
    *,
    application_id: str,
    role: str,
    bucket: str,
    region: Optional[str] = None,
    mode: str = "PROD",
    dt: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    variables: Optional[Dict[str, str]] = None,
) -> Plan:
    """A job run in an EMR Serverless application that uses the ``emr-serverless`` image.

    The application's image carries the engine, Delta and the pipelines (``ubunye
    deploy dockerfile emr-serverless``); set it on the application once with
    ``aws emr-serverless update-application --image-configuration``.
    """
    path = package.task_path(usecase, pkg, task)
    py = "/opt/ubunye/bin/python"
    delta = "3.2.0"
    conf: List[str] = [
        f"spark.jars=local:///opt/jars/delta-spark_2.12-{delta}.jar,"
        f"local:///opt/jars/delta-storage-{delta}.jar",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        f"spark.emr-serverless.driverEnv.PYSPARK_PYTHON={py}",
        f"spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON={py}",
        f"spark.executorEnv.PYSPARK_PYTHON={py}",
    ]
    for key, value in (env or {}).items():
        conf += [
            f"spark.emr-serverless.driverEnv.{key}={value}",
            f"spark.executorEnv.{key}={value}",
        ]
    driver = {
        "sparkSubmit": {
            "entryPoint": "local:///app/ubunye_entry.py",
            "entryPointArguments": _entry_args(path, mode, dt, {}, variables or {}),
            "sparkSubmitParameters": " ".join(f"--conf {c}" for c in conf),
        }
    }
    overrides = {
        "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": f"s3://{bucket}/logs/"}}
    }
    plan = Plan("emr-serverless", f"ubunye-{task}")
    command = [
        "aws", "emr-serverless", "start-job-run", "--application-id", application_id,
        "--execution-role-arn", role, "--name", plan.job,
        "--job-driver", json.dumps(driver), "--configuration-overrides", json.dumps(overrides),
    ]  # fmt: skip
    if region:
        command += ["--region", region]
    plan.commands.append(command)
    return plan
