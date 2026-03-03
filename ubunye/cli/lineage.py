"""CLI commands for inspecting run lineage records.

Mounted on the main app as a sub-command group:

    ubunye lineage show    -d DIR -u USECASE -p PKG -t TASK [--run-id ID]
    ubunye lineage list    -d DIR -u USECASE -p PKG -t TASK [-n 10]
    ubunye lineage compare -d DIR -u USECASE -p PKG -t TASK --run-id1 ID1 --run-id2 ID2
    ubunye lineage search  -d DIR [--status error] [--since 2025-01-01]
    ubunye lineage trace   -d DIR -u USECASE -p PKG -t TASK
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from ubunye.lineage.context import RunContext
from ubunye.lineage.storage import FileSystemLineageStore

lineage_app = typer.Typer(name="lineage", help="Inspect run lineage records.", add_completion=False)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_LINE = "-" * 60


def _store(lineage_dir: str) -> FileSystemLineageStore:
    return FileSystemLineageStore(lineage_dir)


def _task_path(usecase: str, package: str, task: str) -> str:
    return f"{usecase}/{package}/{task}"


def _fmt_row(ctx: RunContext) -> str:
    duration = f"{ctx.duration_sec:.1f}s" if ctx.duration_sec is not None else "-"
    in_rows = sum(s.row_count or 0 for s in ctx.inputs)
    out_rows = sum(s.row_count or 0 for s in ctx.outputs)
    return (
        f"{ctx.run_id[:8]}  {ctx.started_at[:19]}  "
        f"{ctx.status:<8}  {duration:>7}  "
        f"in:{in_rows}  out:{out_rows}"
    )


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------

@lineage_app.command("show")
def show(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", help="Root directory of pipelines."),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Specific run ID (default: latest)."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
):
    """Show a run record as formatted JSON (latest or specific run)."""
    store = _store(str(usecase_dir / lineage_dir))
    tp = _task_path(usecase, package, task)

    try:
        if run_id:
            ctx = store.load(tp, run_id)
        else:
            runs = store.list_runs(tp, n=1)
            if not runs:
                typer.secho(f"No lineage records found for task '{tp}'.", fg=typer.colors.YELLOW)
                raise typer.Exit(code=1)
            ctx = runs[0]
    except FileNotFoundError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    typer.echo(json.dumps(ctx.to_dict(), indent=2))


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

@lineage_app.command("list")
def list_runs(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    n: int = typer.Option(10, "-n", "--n", help="Number of recent runs to show."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
):
    """List recent runs for a task (newest first)."""
    store = _store(str(usecase_dir / lineage_dir))
    tp = _task_path(usecase, package, task)
    runs = store.list_runs(tp, n=n)

    if not runs:
        typer.secho(f"No lineage records found for '{tp}'.", fg=typer.colors.YELLOW)
        return

    typer.echo(_LINE)
    typer.echo(f"Task: {tp}")
    typer.echo(_LINE)
    typer.echo(f"{'RUN_ID':9}  {'STARTED':19}  {'STATUS':<8}  {'DUR':>7}  ROWS")
    typer.echo(_LINE)
    for ctx in runs:
        color = typer.colors.GREEN if ctx.status == "success" else (
            typer.colors.RED if ctx.status == "error" else typer.colors.YELLOW
        )
        typer.secho(_fmt_row(ctx), fg=color)


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------

@lineage_app.command("compare")
def compare(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    run_id1: str = typer.Option(..., "--run-id1", help="First run ID."),
    run_id2: str = typer.Option(..., "--run-id2", help="Second run ID."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
):
    """Diff two run records — highlight changes in hashes, row counts, and status."""
    store = _store(str(usecase_dir / lineage_dir))
    tp = _task_path(usecase, package, task)

    try:
        a = store.load(tp, run_id1)
        b = store.load(tp, run_id2)
    except FileNotFoundError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    typer.echo(_LINE)
    typer.echo(f"Comparing runs for '{tp}'")
    typer.echo(f"  A: {a.run_id[:8]}  {a.started_at[:19]}  {a.status}")
    typer.echo(f"  B: {b.run_id[:8]}  {b.started_at[:19]}  {b.status}")
    typer.echo(_LINE)

    def _cmp(label: str, va: object, vb: object) -> None:
        if va == vb:
            typer.echo(f"  {label}: {va}  (unchanged)")
        else:
            typer.secho(f"  {label}: {va} → {vb}  CHANGED", fg=typer.colors.YELLOW)

    _cmp("status", a.status, b.status)
    _cmp("duration_sec", a.duration_sec, b.duration_sec)
    _cmp("config_hash", a.config_hash, b.config_hash)

    # Compare outputs step by step
    a_out = {s.name: s for s in a.outputs}
    b_out = {s.name: s for s in b.outputs}
    all_names = sorted(set(a_out) | set(b_out))
    for name in all_names:
        sa = a_out.get(name)
        sb = b_out.get(name)
        typer.echo(f"  Output '{name}':")
        _cmp("    row_count", sa.row_count if sa else None, sb.row_count if sb else None)
        _cmp("    schema_hash", sa.schema_hash if sa else None, sb.schema_hash if sb else None)
        _cmp("    data_hash", sa.data_hash if sa else None, sb.data_hash if sb else None)


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

@lineage_app.command("search")
def search(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    task: Optional[str] = typer.Option(None, "-t", "--task", help="Restrict to this task name."),
    usecase: Optional[str] = typer.Option(None, "-u", "--usecase", help="Restrict to this usecase."),
    package: Optional[str] = typer.Option(None, "-p", "--package", help="Restrict to this package."),
    status: Optional[str] = typer.Option(None, "--status", help="Filter by status: success|error|running."),
    since: Optional[str] = typer.Option(None, "--since", help="Only runs started on or after this ISO date."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
):
    """Search all recorded runs across tasks with optional filters."""
    store = _store(str(usecase_dir / lineage_dir))

    tp = None
    if usecase and package and task:
        tp = _task_path(usecase, package, task)
    elif usecase and package:
        tp = f"{usecase}/{package}"
    elif usecase:
        tp = usecase

    runs = store.search(task_path=tp, status=status, since=since)

    if not runs:
        typer.secho("No matching lineage records found.", fg=typer.colors.YELLOW)
        return

    typer.echo(_LINE)
    typer.echo(f"{'RUN_ID':9}  {'TASK PATH':40}  {'STARTED':19}  {'STATUS':<8}  DUR")
    typer.echo(_LINE)
    for ctx in runs:
        color = typer.colors.GREEN if ctx.status == "success" else (
            typer.colors.RED if ctx.status == "error" else typer.colors.YELLOW
        )
        duration = f"{ctx.duration_sec:.1f}s" if ctx.duration_sec is not None else "-"
        typer.secho(
            f"{ctx.run_id[:8]}  {ctx.task_path:<40}  {ctx.started_at[:19]}  "
            f"{ctx.status:<8}  {duration}",
            fg=color,
        )


# ---------------------------------------------------------------------------
# trace
# ---------------------------------------------------------------------------

@lineage_app.command("trace")
def trace(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Specific run (default: latest)."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
):
    """Print the input → transform → output data flow graph for a run."""
    store = _store(str(usecase_dir / lineage_dir))
    tp = _task_path(usecase, package, task)

    try:
        if run_id:
            ctx = store.load(tp, run_id)
        else:
            runs = store.list_runs(tp, n=1)
            if not runs:
                typer.secho(f"No lineage records for '{tp}'.", fg=typer.colors.YELLOW)
                raise typer.Exit(code=1)
            ctx = runs[0]
    except FileNotFoundError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    typer.echo()
    typer.secho(f"Lineage trace: {ctx.task_path}", bold=True)
    typer.echo(f"Run:     {ctx.run_id}  [{ctx.status}]  {ctx.started_at[:19]}")
    typer.echo(f"Version: {ctx.model} v{ctx.version}")
    typer.echo()

    # INPUTS
    typer.secho("  INPUTS", fg=typer.colors.CYAN)
    for step in ctx.inputs:
        typer.echo(f"    [{step.format}] {step.name}")
        typer.echo(f"      location : {step.location}")
        if step.row_count is not None:
            typer.echo(f"      rows     : {step.row_count:,}")
        if step.schema_hash:
            typer.echo(f"      schema   : {step.schema_hash}")
        if step.data_hash:
            typer.echo(f"      data     : {step.data_hash}")

    # TRANSFORM
    typer.echo()
    typer.secho("  TRANSFORM", fg=typer.colors.CYAN)
    typer.echo("    (transformations.py)")

    # OUTPUTS
    typer.echo()
    typer.secho("  OUTPUTS", fg=typer.colors.CYAN)
    for step in ctx.outputs:
        typer.echo(f"    [{step.format}] {step.name}")
        typer.echo(f"      location : {step.location}")
        if step.row_count is not None:
            typer.echo(f"      rows     : {step.row_count:,}")
        if step.schema_hash:
            typer.echo(f"      schema   : {step.schema_hash}")
        if step.data_hash:
            typer.echo(f"      data     : {step.data_hash}")
    typer.echo()
