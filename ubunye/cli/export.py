"""``ubunye export`` — render a task config as a scheduler artifact.

Delegates to the exporters under :mod:`ubunye.orchestration`. The command
loads the task's ``config.yaml``, pulls defaults from its ``ORCHESTRATION``
block, and writes the generated artifact to ``--output``.

Usage
-----
    ubunye export airflow -c pipelines/fraud/etl/claims/config.yaml -o dags/claims.py
    ubunye export databricks -c pipelines/fraud/etl/claims/config.yaml -o jobs/claims.json
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import typer

from ubunye.config import load_config
from ubunye.orchestration import AirflowExporter, DatabricksExporter

export_app = typer.Typer(
    name="export",
    help="Export a task config to an orchestration artifact (Airflow DAG, Databricks job).",
    add_completion=False,
)


def _load_orchestration_options(config_path: Path) -> Dict[str, Any]:
    """Validate the config and return its ``ORCHESTRATION`` block as a plain dict.

    Returns ``{}`` when the block is absent — callers then fall back to the
    exporter's own defaults. The profile is not passed through here because
    artifact generation is profile-independent; ``--profile`` only gets embedded
    into the generated bash/spark-python command.
    """
    cfg = load_config(str(config_path), variables={})
    if cfg.ORCHESTRATION is None:
        return {}
    return cfg.ORCHESTRATION.model_dump(mode="json", exclude_none=True)


@export_app.command("airflow")
def export_airflow(
    config: Path = typer.Option(
        ..., "-c", "--config", exists=True, dir_okay=False, help="Path to task config.yaml."
    ),
    output: Path = typer.Option(..., "-o", "--output", help="Where to write the generated DAG."),
    profile: str = typer.Option("prod", "--profile", help="Profile embedded in the bash command."),
):
    """Generate an Airflow DAG Python file for the task."""
    opts = _load_orchestration_options(config)
    opts["profile"] = profile
    path = AirflowExporter().export(config, output_path=output, options=opts)
    typer.secho(f"[OK] Airflow DAG written to {path}", fg=typer.colors.GREEN)


@export_app.command("databricks")
def export_databricks(
    config: Path = typer.Option(
        ..., "-c", "--config", exists=True, dir_okay=False, help="Path to task config.yaml."
    ),
    output: Path = typer.Option(..., "-o", "--output", help="Where to write the generated job.json."),
    profile: str = typer.Option("prod", "--profile", help="Profile embedded in the job command."),
):
    """Generate a Databricks Jobs API spec (``job.json``) for the task."""
    raw = _load_orchestration_options(config)

    # Flatten: top-level OrchestrationConfig fields + nested `databricks` cluster block.
    opts: Dict[str, Any] = {"profile": profile}
    if "schedule" in raw:
        opts["schedule_quartz"] = raw["schedule"]
    db_opts = raw.get("databricks") or {}
    opts.update(db_opts)

    path = DatabricksExporter().export(config, output_path=output, options=opts)
    typer.secho(f"[OK] Databricks job spec written to {path}", fg=typer.colors.GREEN)
