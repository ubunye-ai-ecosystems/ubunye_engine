"""Databricks deployment module for Ubunye Engine.

Usage::

    from ubunye.deploy.databricks import deploy_task

    result = deploy_task(
        task_dir="pipelines/fraud/ingestion/claim_etl",
        target="dev",
        dry_run=True,
    )
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Union


def deploy_task(
    task_dir: Union[str, Path],
    target: str = "dev",
    *,
    mode: str = "PROD",
    dt: Optional[str] = None,
    dry_run: bool = False,
    targets_path: Optional[Union[str, Path]] = None,
    host: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Deploy a single task to Databricks.

    Returns a dict with job metadata: ``job_name``, ``job_id`` (None if
    dry-run), ``job_spec``, and ``workspace_path``.
    """
    from ubunye.config import load_config
    from ubunye.deploy.databricks.auth import resolve_auth
    from ubunye.deploy.databricks.bundle import deploy_job
    from ubunye.deploy.databricks.notebook import generate_notebook
    from ubunye.deploy.databricks.targets import resolve_target
    from ubunye.deploy.databricks.workspace import upload_task_files

    task_dir = Path(task_dir)
    config_path = task_dir / "config.yaml"
    cfg = load_config(str(config_path), variables={})

    parts = task_dir.parts
    usecase = parts[-3]
    pipeline = parts[-2]
    task_name = parts[-1]

    target_cfg = resolve_target(
        target_name=target,
        task_dir=task_dir,
        host=host,
        token=token,
        targets_path=targets_path,
    )

    job_name = f"ubunye-{usecase}-{pipeline}-{task_name}-{target}"
    tags = {
        "ubunye_managed": "true",
        "ubunye_usecase": usecase,
        "ubunye_pipeline": pipeline,
        "ubunye_task": task_name,
        "ubunye_target": target,
    }

    schedule = None
    if cfg.ORCHESTRATION and cfg.ORCHESTRATION.schedule:
        schedule = cfg.ORCHESTRATION.schedule

    workspace_task_path = (
        f"{target_cfg.workspace_path}/{usecase}/{pipeline}/{task_name}"
    )
    notebook_path = f"{workspace_task_path}/run_{task_name}"

    notebook_source = generate_notebook(
        workspace_task_path=workspace_task_path,
        mode=mode,
        dt=dt,
    )

    if dry_run:
        from ubunye.deploy.databricks.bundle import build_job_spec

        job_spec = build_job_spec(
            job_name=job_name,
            notebook_path=notebook_path,
            target=target_cfg,
            tags=tags,
            schedule=schedule,
        )
        return {
            "job_name": job_name,
            "job_id": None,
            "job_spec": job_spec,
            "workspace_path": workspace_task_path,
            "notebook_source": notebook_source,
        }

    client = resolve_auth(target_cfg)

    upload_task_files(
        client=client,
        task_dir=task_dir,
        workspace_root=target_cfg.workspace_path,
        usecase=usecase,
        pipeline=pipeline,
        task=task_name,
        notebook_source=notebook_source,
        notebook_name=f"run_{task_name}",
    )

    result = deploy_job(
        client=client,
        job_name=job_name,
        notebook_path=notebook_path,
        target=target_cfg,
        tags=tags,
        schedule=schedule,
    )
    result["workspace_path"] = workspace_task_path
    return result
