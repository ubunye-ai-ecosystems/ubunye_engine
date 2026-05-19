"""Databricks deployment module for Ubunye Engine.

``DatabricksDeployAdapter`` implements the
:class:`~ubunye.interfaces.DeployAdapter` protocol.  The module-level
:func:`deploy_task` function is the backward-compatible entry point used
by the CLI.

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

from ubunye.interfaces.deploy import DeployContext, DeployResult


class DatabricksDeployAdapter:
    """DeployAdapter implementation for Databricks workspaces.

    Handles notebook generation, workspace file upload, and job
    creation/update via the Databricks SDK.
    """

    @property
    def name(self) -> str:
        return "databricks"

    def deploy(
        self,
        ctx: DeployContext,
        target: Dict[str, Any],
        *,
        dry_run: bool = False,
    ) -> DeployResult:
        from ubunye.deploy.databricks.auth import resolve_auth
        from ubunye.deploy.databricks.bundle import build_job_spec, deploy_job
        from ubunye.deploy.databricks.notebook import generate_notebook
        from ubunye.deploy.databricks.targets import DatabricksTargetConfig
        from ubunye.deploy.databricks.workspace import upload_task_files

        target_cfg = DatabricksTargetConfig(**target)

        job_name = f"ubunye-{ctx.usecase}-{ctx.pipeline}-{ctx.task_name}-{ctx.target_name}"
        tags = {
            "ubunye_managed": "true",
            "ubunye_usecase": ctx.usecase,
            "ubunye_pipeline": ctx.pipeline,
            "ubunye_task": ctx.task_name,
            "ubunye_target": ctx.target_name,
        }

        schedule = None
        cfg_orch = ctx.config.get("ORCHESTRATION") or {}
        if isinstance(cfg_orch, dict):
            schedule = cfg_orch.get("schedule")
        elif hasattr(cfg_orch, "schedule"):
            schedule = cfg_orch.schedule

        workspace_task_path = (
            f"{target_cfg.workspace_path}/{ctx.usecase}/{ctx.pipeline}/{ctx.task_name}"
        )
        notebook_path = f"{workspace_task_path}/run_{ctx.task_name}"

        notebook_source = generate_notebook(
            workspace_task_path=workspace_task_path,
            mode=ctx.mode,
            dt=ctx.dt,
        )

        if dry_run:
            job_spec = build_job_spec(
                job_name=job_name,
                notebook_path=notebook_path,
                target=target_cfg,
                tags=tags,
                schedule=schedule,
            )
            return DeployResult(
                job_name=job_name,
                job_id=None,
                job_spec=job_spec,
                workspace_path=workspace_task_path,
                metadata={"notebook_source": notebook_source},
            )

        client = resolve_auth(target_cfg)

        upload_task_files(
            client=client,
            task_dir=ctx.task_dir,
            workspace_root=target_cfg.workspace_path,
            usecase=ctx.usecase,
            pipeline=ctx.pipeline,
            task=ctx.task_name,
            notebook_source=notebook_source,
            notebook_name=f"run_{ctx.task_name}",
        )

        result = deploy_job(
            client=client,
            job_name=job_name,
            notebook_path=notebook_path,
            target=target_cfg,
            tags=tags,
            schedule=schedule,
        )

        job_url = self.get_job_url(str(result["job_id"]), target)
        return DeployResult(
            job_name=result["job_name"],
            job_id=result["job_id"],
            job_spec=result["job_spec"],
            workspace_path=workspace_task_path,
            job_url=job_url,
        )

    def validate_target(self, target: Dict[str, Any]) -> None:
        from ubunye.deploy.databricks.targets import DatabricksTargetConfig

        DatabricksTargetConfig(**target)

    def get_job_url(self, job_id: str, target: Dict[str, Any]) -> str:
        host = target.get("host", "").rstrip("/")
        return f"{host}/#job/{job_id}"


# ---------------------------------------------------------------------------
# Backward-compatible module-level function
# ---------------------------------------------------------------------------


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

    This is the backward-compatible entry point.  Internally it
    resolves the target, constructs a :class:`DeployContext`, and
    delegates to :class:`DatabricksDeployAdapter`.
    """
    from ubunye.config import load_config
    from ubunye.deploy.databricks.targets import resolve_target

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

    ctx = DeployContext(
        task_dir=task_dir,
        usecase=usecase,
        pipeline=pipeline,
        task_name=task_name,
        config=cfg.model_dump(mode="json") if hasattr(cfg, "model_dump") else {},
        target_name=target,
        mode=mode,
        dt=dt,
    )

    adapter = DatabricksDeployAdapter()
    deploy_result = adapter.deploy(
        ctx,
        target_cfg.model_dump() if hasattr(target_cfg, "model_dump") else {},
        dry_run=dry_run,
    )

    result: Dict[str, Any] = {
        "job_name": deploy_result.job_name,
        "job_id": deploy_result.job_id,
        "job_spec": deploy_result.job_spec,
        "workspace_path": deploy_result.workspace_path,
    }
    if dry_run and deploy_result.metadata.get("notebook_source"):
        result["notebook_source"] = deploy_result.metadata["notebook_source"]
    return result
