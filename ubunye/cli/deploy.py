"""``ubunye deploy`` — deploy task(s) to execution environments.

Usage
-----
    ubunye deploy databricks -d pipelines -u fraud -p ingestion -t claim_etl --target dev --dry-run
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer

deploy_app = typer.Typer(
    name="deploy",
    help="Deploy task(s): Databricks, Glue, Dataproc, Kubernetes, Container Apps, EMR Serverless.",
    add_completion=False,
)


@deploy_app.command("databricks")
def deploy_databricks(
    usecase_dir: Path = typer.Option(
        ...,
        "-d",
        "--usecase-dir",
        exists=True,
        file_okay=False,
        help="Root directory of pipelines.",
    ),
    usecase: str = typer.Option(..., "-u", "--usecase", help="Usecase name."),
    package: str = typer.Option(..., "-p", "--package", help="Pipeline/package name."),
    task: str = typer.Option(..., "-t", "--task", help="Task name."),
    target: str = typer.Option("dev", "--target", help="Deploy target (dev, prod, etc.)."),
    mode: str = typer.Option("PROD", "-m", "--mode", help="Config profile/mode for the job."),
    dt: Optional[str] = typer.Option(
        None, "-dt", "--data-timestamp", help="Data timestamp passed to the task."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print job spec without deploying."),
    host: Optional[str] = typer.Option(
        None, "--host", help="Databricks host for ad-hoc deploy (skips targets.yaml)."
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        help="Env var name holding the Databricks token (default: DATABRICKS_TOKEN).",
    ),
) -> None:
    """Deploy a task to Databricks as a scheduled job."""
    from ubunye.deploy.databricks import deploy_task

    task_dir = usecase_dir / usecase / package / task

    result = deploy_task(
        task_dir=task_dir,
        target=target,
        mode=mode,
        dt=dt,
        dry_run=dry_run,
        host=host,
        token=token,
    )

    if dry_run:
        typer.echo(json.dumps(result["job_spec"], indent=2))
        typer.secho(
            f"\n[DRY RUN] Job spec for '{result['job_name']}' printed above. "
            f"No changes made to Databricks.",
            fg=typer.colors.YELLOW,
        )
    else:
        typer.secho(
            f"[OK] Job '{result['job_name']}' deployed "
            f"(id={result['job_id']}, target={target})",
            fg=typer.colors.GREEN,
        )


# --- serverless Spark services (B5): Glue, Dataproc Serverless ----------------------


def _pairs(values: Optional[List[str]], flag: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in values or []:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise typer.BadParameter(f"{flag} takes KEY=VALUE, got {item!r}")
        out[key] = value
    return out


def _finish(plan: Any, dry_run: bool, record_out: Optional[Path]) -> None:
    """Print the plan, or run it and report the run record the job printed."""
    from ubunye.deploy import cloud, package

    if dry_run:
        typer.echo(json.dumps(plan.describe(), indent=2))
        typer.secho(
            f"\n[DRY RUN] {plan.platform} plan for '{plan.job}'. Nothing ran.",
            fg=typer.colors.YELLOW,
        )
        return
    cloud.execute(plan)
    record = package.read_record(plan.log)
    if record is None and record_out is not None:
        # Asked for the record and did not get it: never report that as success.
        typer.secho(
            f"[FAIL] {plan.platform}: '{plan.job}' ran, but its run record was not found "
            "in the job's log, so --record-out has nothing to write.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    if record is None:
        typer.secho(f"[OK] {plan.platform}: '{plan.job}' submitted.", fg=typer.colors.GREEN)
        return
    if record_out is not None:
        record_out.write_text(json.dumps(record, indent=2), encoding="utf-8")
    ok = record.get("status") == "success"
    typer.secho(
        f"[{'OK' if ok else 'FAIL'}] {plan.platform} run {record.get('status')}: {plan.job}",
        fg=typer.colors.GREEN if ok else typer.colors.RED,
    )
    for step in record.get("outputs", []):
        typer.echo(f"  {step['name']}: {step.get('row_count')} rows, {step.get('data_hash')}")
    if record_out is not None:
        typer.echo(f"  run record: {record_out}")
    if not ok:
        raise typer.Exit(code=1)


_ENV_HELP = "Environment for the task, KEY=VALUE (repeatable)."
_VAR_HELP = "Template variable, KEY=VALUE (repeatable)."
_OUT_HELP = "Save the run's record here (for `ubunye gate`)."


@deploy_app.command("glue")
def deploy_glue(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", exists=True, file_okay=False),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    bucket: str = typer.Option(..., "--bucket", help="S3 bucket for the task, script and wheel."),
    role: str = typer.Option(..., "--role", help="IAM role ARN the Glue job runs as."),
    region: Optional[str] = typer.Option(None, "--region"),
    job: Optional[str] = typer.Option(None, "--job", help="Glue job name (default from the task)."),
    engine: str = typer.Option("ubunye-engine", "--engine", help="pip requirement for the engine."),
    engine_wheel: Optional[Path] = typer.Option(
        None, "--engine-wheel", exists=True, dir_okay=False, help="Install this wheel instead."
    ),
    workers: int = typer.Option(2, "--workers"),
    worker_type: str = typer.Option("G.1X", "--worker-type"),
    mode: str = typer.Option("PROD", "-m", "--mode"),
    dt: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    env: Optional[List[str]] = typer.Option(None, "--env", help=_ENV_HELP),
    var: Optional[List[str]] = typer.Option(None, "--var", help=_VAR_HELP),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Follow the run to its end."),
    record_out: Optional[Path] = typer.Option(None, "--record-out", help=_OUT_HELP),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the plan; run nothing."),
) -> None:
    """Run a task on AWS Glue 5 (Spark 3.5), with the aws CLI."""
    from ubunye.deploy.cloud import plan_glue

    plan = plan_glue(
        usecase_dir,
        usecase,
        package,
        task,
        bucket=bucket,
        role=role,
        region=region,
        job=job,
        engine=engine,
        engine_wheel=engine_wheel,
        workers=workers,
        worker_type=worker_type,
        mode=mode,
        dt=dt,
        env=_pairs(env, "--env"),
        variables=_pairs(var, "--var"),
        wait=wait,
    )
    _finish(plan, dry_run, record_out)


@deploy_app.command("dataproc")
def deploy_dataproc(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", exists=True, file_okay=False),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    project: str = typer.Option(..., "--project"),
    region: str = typer.Option(..., "--region"),
    bucket: str = typer.Option(..., "--bucket", help="GCS bucket for the task and script."),
    image: str = typer.Option(
        ..., "--image", help="Container image with the engine (see `deploy dockerfile dataproc`)."
    ),
    service_account: Optional[str] = typer.Option(None, "--service-account"),
    runtime: str = typer.Option("2.2", "--runtime", help="Dataproc Serverless runtime version."),
    batch: Optional[str] = typer.Option(None, "--batch", help="Batch id (default from the task)."),
    mode: str = typer.Option("PROD", "-m", "--mode"),
    dt: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    env: Optional[List[str]] = typer.Option(None, "--env", help=_ENV_HELP),
    var: Optional[List[str]] = typer.Option(None, "--var", help=_VAR_HELP),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Follow the run to its end."),
    record_out: Optional[Path] = typer.Option(None, "--record-out", help=_OUT_HELP),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the plan; run nothing."),
) -> None:
    """Run a task on GCP Dataproc Serverless, in a container image, with gcloud."""
    from ubunye.deploy.cloud import plan_dataproc

    plan = plan_dataproc(
        usecase_dir,
        usecase,
        package,
        task,
        project=project,
        region=region,
        bucket=bucket,
        image=image,
        service_account=service_account,
        runtime=runtime,
        batch=batch,
        mode=mode,
        dt=dt,
        env=_pairs(env, "--env"),
        variables=_pairs(var, "--var"),
        wait=wait,
    )
    _finish(plan, dry_run, record_out)


@deploy_app.command("dockerfile")
def deploy_dockerfile(
    platform: str = typer.Argument(..., help="dataproc, emr-serverless or container"),
    pipelines: str = typer.Option(
        "pipelines", "--pipelines", help="Folder copied into the image as /app/pipelines."
    ),
    engine: str = typer.Option("ubunye-engine", "--engine", help="pip requirement for the engine."),
    out: Optional[Path] = typer.Option(None, "--out", help="Write here instead of printing."),
) -> None:
    """Write a Dockerfile for a Spark job image on PLATFORM, and the entry script next to it."""
    from ubunye.deploy import package as packaging

    if platform not in packaging.IMAGE_KINDS:
        raise typer.BadParameter(f"PLATFORM is one of {', '.join(packaging.IMAGE_KINDS)}")
    text = packaging.dockerfile(platform, pipelines=pipelines, engine=engine)
    if out is None:
        typer.echo(text)
        return
    out.write_text(text, encoding="utf-8")
    entry = out.parent / "ubunye_entry.py"
    entry.write_text(packaging.ENTRY_SCRIPT, encoding="utf-8")
    typer.secho(f"[OK] wrote {out} and {entry}", fg=typer.colors.GREEN)


# --- container jobs (B5b): Kubernetes, Azure Container Apps, EMR Serverless ---------------


@deploy_app.command("k8s")
def deploy_k8s(
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    image: str = typer.Option(
        ..., "--image", help="Image from `deploy dockerfile container` (pipelines baked in)."
    ),
    namespace: str = typer.Option("default", "--namespace", "-n"),
    job: Optional[str] = typer.Option(None, "--job", help="Job name (default from the task)."),
    cpu: str = typer.Option("1", "--cpu"),
    memory: str = typer.Option("2Gi", "--memory"),
    timeout: int = typer.Option(1800, "--timeout", help="Seconds to wait for the job."),
    mode: str = typer.Option("PROD", "-m", "--mode"),
    dt: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    env: Optional[List[str]] = typer.Option(None, "--env", help=_ENV_HELP),
    var: Optional[List[str]] = typer.Option(None, "--var", help=_VAR_HELP),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Follow the run to its end."),
    record_out: Optional[Path] = typer.Option(None, "--record-out", help=_OUT_HELP),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the plan; run nothing."),
) -> None:
    """Run a task as a Kubernetes Job, with kubectl (its current context)."""
    from ubunye.deploy.containers import plan_k8s

    plan = plan_k8s(
        usecase,
        package,
        task,
        image=image,
        namespace=namespace,
        job=job,
        cpu=cpu,
        memory=memory,
        timeout_s=timeout,
        mode=mode,
        dt=dt,
        env=_pairs(env, "--env"),
        variables=_pairs(var, "--var"),
        wait=wait,
    )
    _finish(plan, dry_run, record_out)


@deploy_app.command("container-apps")
def deploy_container_apps(
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    image: str = typer.Option(
        ..., "--image", help="Image from `deploy dockerfile container` (pipelines baked in)."
    ),
    resource_group: str = typer.Option(..., "--resource-group", "-g"),
    environment: str = typer.Option(..., "--environment", help="Container Apps environment."),
    job: Optional[str] = typer.Option(None, "--job", help="Job name (default from the task)."),
    registry_server: Optional[str] = typer.Option(None, "--registry-server"),
    registry_identity: Optional[str] = typer.Option(
        None, "--registry-identity", help="Managed identity (resource id) that pulls the image."
    ),
    cpu: str = typer.Option("2", "--cpu"),
    memory: str = typer.Option("4Gi", "--memory"),
    timeout: int = typer.Option(1800, "--timeout", help="Seconds a replica may run."),
    mode: str = typer.Option("PROD", "-m", "--mode"),
    dt: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    env: Optional[List[str]] = typer.Option(None, "--env", help=_ENV_HELP),
    var: Optional[List[str]] = typer.Option(None, "--var", help=_VAR_HELP),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Follow the run to its end."),
    record_out: Optional[Path] = typer.Option(None, "--record-out", help=_OUT_HELP),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the plan; run nothing."),
) -> None:
    """Run a task as an Azure Container Apps job, with the az CLI."""
    from ubunye.deploy.containers import plan_container_apps

    plan = plan_container_apps(
        usecase,
        package,
        task,
        image=image,
        resource_group=resource_group,
        environment=environment,
        job=job,
        registry_server=registry_server,
        registry_identity=registry_identity,
        cpu=cpu,
        memory=memory,
        timeout_s=timeout,
        mode=mode,
        dt=dt,
        env=_pairs(env, "--env"),
        variables=_pairs(var, "--var"),
        wait=wait,
    )
    _finish(plan, dry_run, record_out)


@deploy_app.command("emr-serverless")
def deploy_emr_serverless(
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    application_id: str = typer.Option(
        ...,
        "--application-id",
        help="An application whose image is `deploy dockerfile emr-serverless`.",
    ),
    role: str = typer.Option(..., "--role", help="Job execution role ARN."),
    bucket: str = typer.Option(..., "--bucket", help="S3 bucket for the job's logs."),
    region: Optional[str] = typer.Option(None, "--region"),
    mode: str = typer.Option("PROD", "-m", "--mode"),
    dt: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    env: Optional[List[str]] = typer.Option(None, "--env", help=_ENV_HELP),
    var: Optional[List[str]] = typer.Option(None, "--var", help=_VAR_HELP),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the plan; run nothing."),
) -> None:
    """Start a task run on AWS EMR Serverless, with the aws CLI (does not wait)."""
    from ubunye.deploy.containers import plan_emr_serverless

    plan = plan_emr_serverless(
        usecase,
        package,
        task,
        application_id=application_id,
        role=role,
        bucket=bucket,
        region=region,
        mode=mode,
        dt=dt,
        env=_pairs(env, "--env"),
        variables=_pairs(var, "--var"),
    )
    _finish(plan, dry_run, None)
