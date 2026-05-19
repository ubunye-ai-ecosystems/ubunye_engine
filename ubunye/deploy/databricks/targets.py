"""Target configuration for Databricks deployments.

Targets are loaded from ``targets.yaml`` with a two-level lookup:

1. Usecase directory (``{pipelines_dir}/{usecase}/targets.yaml``) — shared
2. Task directory (``{pipelines_dir}/{usecase}/{pipeline}/{task}/targets.yaml``) — overrides
3. CLI flags (``--host``, ``--token``) — ad-hoc single-target deploys
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict

from ubunye.core.errors import TargetNotFoundError


class DatabricksTargetConfig(BaseModel):
    """Configuration for a single Databricks deploy target (dev, prod, etc.)."""

    model_config = ConfigDict(extra="forbid")

    host: str
    token_env: str = "DATABRICKS_TOKEN"
    workspace_path: str = "/Workspace/ubunye"
    serverless: bool = True
    spark_version: str = "13.3.x-scala2.12"
    node_type_id: str = "i3.xlarge"
    num_workers: Optional[int] = None
    spark_conf: Dict[str, str] = {}
    aws_attributes: Dict[str, Any] = {}
    max_concurrent_runs: int = 1


class TargetsConfig(BaseModel):
    """Container for all deploy targets."""

    model_config = ConfigDict(extra="forbid")

    targets: Dict[str, DatabricksTargetConfig]


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-merge *override* into *base*. Override wins at leaf level."""
    result = copy.deepcopy(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = copy.deepcopy(val)
    return result


def load_targets(path: Union[str, Path]) -> Dict[str, Any]:
    """Load a ``targets.yaml`` file and return the raw dict."""
    path = Path(path)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_target(
    target_name: str,
    task_dir: Path,
    *,
    host: Optional[str] = None,
    token: Optional[str] = None,
    targets_path: Optional[Union[str, Path]] = None,
) -> DatabricksTargetConfig:
    """Resolve a named deploy target from the two-level targets.yaml lookup.

    Lookup order:
    1. Usecase-level ``targets.yaml`` (three levels up from task dir)
    2. Task-level ``targets.yaml`` (in task dir itself)
    3. CLI flags ``host`` / ``token`` for ad-hoc deploys

    When both files exist, task-level deep-merges on top of usecase-level.
    """
    if targets_path is not None:
        raw = load_targets(targets_path)
        cfg = TargetsConfig.model_validate(raw)
        if target_name not in cfg.targets:
            raise TargetNotFoundError(
                f"Target '{target_name}' not found.",
                context={
                    "Target": target_name,
                    "Available": sorted(cfg.targets.keys()),
                    "File": str(targets_path),
                },
                hint=f"Valid targets: {', '.join(sorted(cfg.targets.keys()))}",
            )
        return cfg.targets[target_name]

    usecase_dir = task_dir.parent.parent
    usecase_targets = usecase_dir / "targets.yaml"
    task_targets = task_dir / "targets.yaml"

    merged_raw: Dict[str, Any] = {}

    if usecase_targets.is_file():
        merged_raw = load_targets(usecase_targets)

    if task_targets.is_file():
        task_raw = load_targets(task_targets)
        merged_raw = _deep_merge(merged_raw, task_raw)

    if merged_raw:
        cfg = TargetsConfig.model_validate(merged_raw)
        if target_name not in cfg.targets:
            raise TargetNotFoundError(
                f"Target '{target_name}' not found.",
                context={
                    "Target": target_name,
                    "Available": sorted(cfg.targets.keys()),
                },
                hint=f"Valid targets: {', '.join(sorted(cfg.targets.keys()))}",
            )
        return cfg.targets[target_name]

    if host:
        return DatabricksTargetConfig(
            host=host,
            token_env="DATABRICKS_TOKEN" if token is None else token,
        )

    raise TargetNotFoundError(
        "No targets.yaml found and no --host flag provided.",
        context={
            "Target": target_name,
            "Searched": [str(usecase_targets), str(task_targets)],
        },
        hint="Create a targets.yaml in your usecase directory or pass --host.",
    )
