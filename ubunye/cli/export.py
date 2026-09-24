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
from typing import Any, Dict, List, Optional

import typer

from ubunye.cli.variables import cli_variables, var_option
from ubunye.config import load_config
from ubunye.orchestration import AirflowExporter, DatabricksExporter

export_app = typer.Typer(
    name="export",
    help="Export a task to an orchestrator: Airflow DAG, Databricks job, Spark pipeline.",
    add_completion=False,
)


def _load_orchestration_options(config_path: Path) -> Dict[str, Any]:
    """Validate the config and return its ``ORCHESTRATION`` block as a plain dict.

    Returns ``{}`` when the block is absent — callers then fall back to the
    exporter's own defaults. The profile is not passed through here because
    artifact generation is profile-independent; ``--profile`` only gets embedded
    into the generated bash/spark-python command.
    """
    # The run's variables do not exist yet: each scheduled run brings its own dt. A
    # config that uses {{ dt }} (most scheduled ones) failed to export with
    # "undefined variable 'dt'", so it is rendered here with stand-ins; only the
    # ORCHESTRATION block is read from the result.
    cfg = load_config(
        str(config_path), variables={"dt": "1970-01-01", "dtf": "%Y-%m-%d", "mode": "PROD"}
    )
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
    usecase_dir: Optional[str] = typer.Option(
        None, "--usecase-dir", help="The pipelines folder where Airflow runs (default: this one)."
    ),
    backend: Optional[str] = typer.Option(None, "--backend", help="Backend for the run."),
    lineage: bool = typer.Option(False, "--lineage", help="Record each run (run record)."),
):
    """Generate an Airflow DAG (Airflow 2.4+ and 3) that runs the task with the CLI."""
    opts = _load_orchestration_options(config)
    opts["profile"] = profile
    opts["usecase_dir"] = usecase_dir
    opts["backend"] = backend
    opts["lineage"] = lineage
    path = AirflowExporter().export(config, output_path=output, options=opts)
    typer.secho(f"[OK] Airflow DAG written to {path}", fg=typer.colors.GREEN)


@export_app.command("databricks")
def export_databricks(
    config: Path = typer.Option(
        ..., "-c", "--config", exists=True, dir_okay=False, help="Path to task config.yaml."
    ),
    output: Path = typer.Option(
        ..., "-o", "--output", help="Where to write the generated job.json."
    ),
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


@export_app.command("spark-pipeline")
def export_spark_pipeline(
    config: Path = typer.Option(
        ..., "-c", "--config", exists=True, dir_okay=False, help="Path to task config.yaml."
    ),
    output: Path = typer.Option(..., "-o", "--output", help="Folder to write the pipeline into."),
    storage: Optional[str] = typer.Option(
        None, "--storage", help="Pipeline storage URI (default: a folder inside the output)."
    ),
    name: Optional[str] = typer.Option(None, "--name", help="Pipeline name."),
    catalog: Optional[str] = typer.Option(None, "--catalog"),
    database: Optional[str] = typer.Option(None, "--database"),
    mode: str = typer.Option("PROD", "-m", "--mode"),
    dt: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    var: Optional[List[str]] = var_option(),
):
    """Write the task as a Spark Declarative Pipeline (Spark 4.1+): run it with spark-pipelines."""
    from ubunye.orchestration.spark_pipeline_exporter import SparkPipelineExporter

    variables = cli_variables(dt=dt, dtf=None, mode=mode, var=var)
    cfg = load_config(str(config), variables).model_dump(mode="json")
    result = SparkPipelineExporter().export(
        config,
        output_path=output,
        options={
            "config": cfg,
            "storage": storage,
            "name": name,
            "catalog": catalog,
            "database": database,
        },
    )
    typer.secho(
        f"[OK] Spark Declarative Pipeline written to {result['path']} "
        f"(materialized views: {', '.join(result['datasets'])})",
        fg=typer.colors.GREEN,
    )
    for note in result["notes"]:
        typer.secho(f"  note: {note}", fg=typer.colors.YELLOW)
    typer.echo(f"  run it: cd {result['path']} && spark-pipelines run")
