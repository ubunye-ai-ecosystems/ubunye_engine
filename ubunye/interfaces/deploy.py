"""DeployAdapter protocol — contract for deployment backends.

A deploy adapter knows how to take a task's config and artifacts and
create a running job on a remote environment (Databricks, EMR, etc.).

Implementations are discovered via the ``ubunye.deploy_adapters``
entry-point group.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, runtime_checkable


@dataclass
class DeployContext:
    """Structured metadata for a deploy operation.

    Callers construct this from CLI flags and the parsed config; the
    adapter never derives task identity from filesystem paths.
    """

    task_dir: Path
    usecase: str
    pipeline: str
    task_name: str
    config: Dict[str, Any]
    target_name: str
    mode: str = "PROD"
    dt: Optional[str] = None


@dataclass
class DeployResult:
    """Returned by :meth:`DeployAdapter.deploy`.

    ``metadata`` carries adapter-specific extras (notebook source, cluster
    id, etc.) without polluting the core fields.
    """

    job_name: str
    job_id: Optional[str]
    job_spec: Dict[str, Any]
    workspace_path: str
    job_url: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)


@runtime_checkable
class DeployAdapter(Protocol):
    """Structural interface for deployment backends.

    Each adapter is responsible for:

    1. Validating that the target dict contains the fields it needs.
    2. Preparing artifacts (notebooks, config bundles) for the target.
    3. Uploading artifacts and creating/updating the remote job.
    4. Returning a :class:`DeployResult` with job metadata.
    """

    @property
    def name(self) -> str:
        """Short identifier for this adapter (e.g. ``"databricks"``)."""
        ...

    def deploy(
        self,
        ctx: DeployContext,
        target: Dict[str, Any],
        *,
        dry_run: bool = False,
    ) -> DeployResult:
        """Deploy a task to the target environment.

        Parameters
        ----------
        ctx:
            Task metadata, config dict, and filesystem paths.
        target:
            Adapter-specific target configuration (validated by the
            adapter via :meth:`validate_target`).
        dry_run:
            If ``True``, build and return the job spec without making
            any remote API calls.

        Returns
        -------
        DeployResult
            Job metadata including name, id, spec, and workspace path.
        """
        ...

    def validate_target(self, target: Dict[str, Any]) -> None:
        """Validate adapter-specific target config.

        Raises
        ------
        ubunye.core.errors.TargetNotFoundError
            When required keys are missing or values are invalid.
        """
        ...

    def get_job_url(self, job_id: str, target: Dict[str, Any]) -> str:
        """Return a browser URL to the deployed job.

        Parameters
        ----------
        job_id:
            The job identifier returned by :meth:`deploy`.
        target:
            The same target dict, which typically contains the host URL.
        """
        ...
