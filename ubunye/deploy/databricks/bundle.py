"""Job creation and update on Databricks via the SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ubunye.core.errors import BundleDeployError
from ubunye.deploy.databricks.targets import DatabricksTargetConfig

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


def build_job_spec(
    job_name: str,
    notebook_path: str,
    target: DatabricksTargetConfig,
    tags: Dict[str, str],
    schedule: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a Databricks job spec dict (does not call the API)."""
    task_spec: Dict[str, Any] = {
        "task_key": "run_ubunye_task",
        "notebook_task": {
            "notebook_path": notebook_path,
        },
    }

    if not target.serverless:
        cluster: Dict[str, Any] = {
            "spark_version": target.spark_version,
            "node_type_id": target.node_type_id,
            "num_workers": target.num_workers if target.num_workers is not None else 0,
        }
        if target.spark_conf:
            cluster["spark_conf"] = target.spark_conf
        if target.aws_attributes:
            cluster["aws_attributes"] = target.aws_attributes
        task_spec["new_cluster"] = cluster

    spec: Dict[str, Any] = {
        "name": job_name,
        "tags": tags,
        "max_concurrent_runs": target.max_concurrent_runs,
        "tasks": [task_spec],
    }

    if schedule:
        spec["schedule"] = {
            "quartz_cron_expression": schedule,
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        }

    return spec


def deploy_job(
    client: "WorkspaceClient",
    job_name: str,
    notebook_path: str,
    target: DatabricksTargetConfig,
    tags: Dict[str, str],
    schedule: Optional[str] = None,
) -> Dict[str, Any]:
    """Create or update a Databricks job. Returns metadata dict with job_id."""
    spec = build_job_spec(
        job_name=job_name,
        notebook_path=notebook_path,
        target=target,
        tags=tags,
        schedule=schedule,
    )

    existing = _find_existing_jobs(client, job_name)

    if len(existing) > 1:
        job_ids = [str(j.job_id) for j in existing]
        raise BundleDeployError(
            f"Multiple jobs named '{job_name}' found in workspace.",
            context={"Job name": job_name, "Job IDs": job_ids},
            hint="Manually remove duplicate jobs from the Databricks Jobs UI, "
            "then retry the deploy.",
        )

    try:
        if len(existing) == 1:
            job_id = existing[0].job_id
            client.jobs.reset(job_id=job_id, new_settings=spec)
        else:
            response = client.jobs.create(**spec)
            job_id = response.job_id
    except Exception as exc:
        raise BundleDeployError(
            f"Failed to {'update' if existing else 'create'} job '{job_name}'.",
            context={"Job name": job_name},
            hint="Check workspace permissions and cluster policy access.",
        ) from exc

    return {
        "job_name": job_name,
        "job_id": job_id,
        "job_spec": spec,
    }


def _find_existing_jobs(
    client: "WorkspaceClient", job_name: str
) -> List[Any]:
    """List jobs matching the exact name."""
    try:
        return [j for j in client.jobs.list(name=job_name) if j.settings and j.settings.name == job_name]
    except Exception as exc:
        raise BundleDeployError(
            f"Failed to list jobs matching '{job_name}'.",
            context={"Job name": job_name},
            hint="Check workspace permissions.",
        ) from exc
