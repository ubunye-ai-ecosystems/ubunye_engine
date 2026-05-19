"""``ubunye deploy`` — deploy task(s) to execution environments.

Usage
-----
    ubunye deploy databricks -d pipelines -u fraud -p ingestion -t claim_etl --target dev --dry-run
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

deploy_app = typer.Typer(
    name="deploy",
    help="Deploy task(s) to execution environments (Databricks, etc.).",
    add_completion=False,
)


@deploy_app.command("databricks")
def deploy_databricks(
    usecase_dir: Path = typer.Option(
        ..., "-d", "--usecase-dir", exists=True, file_okay=False, help="Root directory of pipelines."
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
        None, "--token", help="Env var name holding the Databricks token (default: DATABRICKS_TOKEN)."
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
