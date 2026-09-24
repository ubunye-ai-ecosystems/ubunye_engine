"""CLI commands for inspecting run lineage records.

Mounted on the main app as a sub-command group:

    ubunye lineage show    -d DIR -u USECASE -p PKG -t TASK [--run-id ID]
    ubunye lineage list    -d DIR -u USECASE -p PKG -t TASK [-n 10]
    ubunye lineage compare -d DIR -u USECASE -p PKG -t TASK --run-id1 ID1 --run-id2 ID2
    ubunye lineage search  -d DIR [--status error] [--since 2025-01-01]
    ubunye lineage trace   -d DIR -u USECASE -p PKG -t TASK

Every command takes ``--json`` for one JSON document on stdout.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import typer

from ubunye.cli.output import emit, fail, json_option
from ubunye.lineage.context import RunContext, StepRecord
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


def _rows(steps: Any) -> str:
    """The total row count, or "-" when none was recorded (inputs are not counted)."""
    counts = [s.row_count for s in steps if s.row_count is not None]
    return str(sum(counts)) if counts else "-"


def _fmt_row(ctx: RunContext) -> str:
    duration = f"{ctx.duration_sec:.1f}s" if ctx.duration_sec is not None else "-"
    return (
        f"{ctx.run_id[:8]}  {ctx.started_at[:19]}  "
        f"{ctx.status:<8}  {duration:>7}  "
        f"in:{_rows(ctx.inputs)}  out:{_rows(ctx.outputs)}"
    )


def _load_one(
    store: FileSystemLineageStore, tp: str, run_id: Optional[str], *, as_json: bool
) -> RunContext:
    """One run record: the given one, or the latest; a clear error otherwise."""
    try:
        if run_id:
            return store.load(tp, run_id)
        runs = store.list_runs(tp, n=1)
    except FileNotFoundError as e:
        fail(str(e), as_json=as_json)
    if not runs:
        if as_json:
            fail(f"No lineage records found for task '{tp}'.", as_json=True)
        typer.secho(f"No lineage records found for task '{tp}'.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=1)
    return runs[0]


def _color(status: str) -> Any:
    if status == "success":
        return typer.colors.GREEN
    return typer.colors.RED if status == "error" else typer.colors.YELLOW


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


@lineage_app.command("show")
def show(
    usecase_dir: Path = typer.Option(
        ..., "-d", "--usecase-dir", help="Root directory of pipelines."
    ),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    run_id: Optional[str] = typer.Option(
        None, "--run-id", help="Specific run ID (default: latest)."
    ),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
    as_json: bool = json_option(),
):
    """Show a run record as formatted JSON (latest or specific run)."""
    store = _store(str(usecase_dir / lineage_dir))
    ctx = _load_one(store, _task_path(usecase, package, task), run_id, as_json=as_json)
    emit(ctx.to_dict())  # the record is JSON either way


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
    as_json: bool = json_option(),
):
    """List recent runs for a task (newest first)."""
    store = _store(str(usecase_dir / lineage_dir))
    tp = _task_path(usecase, package, task)
    runs = store.list_runs(tp, n=n)

    if as_json:
        emit([ctx.to_dict() for ctx in runs])
        return
    if not runs:
        typer.secho(f"No lineage records found for '{tp}'.", fg=typer.colors.YELLOW)
        return

    typer.echo(_LINE)
    typer.echo(f"Task: {tp}")
    typer.echo(_LINE)
    typer.echo(f"{'RUN_ID':9}  {'STARTED':19}  {'STATUS':<8}  {'DUR':>7}  ROWS")
    typer.echo(_LINE)
    for ctx in runs:
        typer.secho(_fmt_row(ctx), fg=_color(ctx.status))


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------


def data_hash_state(sa: Optional[StepRecord], sb: Optional[StepRecord]) -> Dict[str, Any]:
    """Whether two outputs' data hashes can be compared, and if so, the verdict.

    ``state`` is ``unchanged``, ``changed``, ``unknown`` (a hash is missing: two
    missing hashes are unknown, not unchanged) or ``not comparable`` (made by
    different methods: records from before 0.6.0 hashed a sample, and saying
    "changed" against a ``rows-v1`` hash would be as wrong as "unchanged").
    """
    ha, hb = getattr(sa, "data_hash", None), getattr(sb, "data_hash", None)
    ma, mb = getattr(sa, "hash_method", None), getattr(sb, "hash_method", None)
    verdict: Dict[str, Any] = {"a": ha, "b": hb, "method_a": ma, "method_b": mb}
    if ha is None or hb is None:
        reasons = [getattr(s, "hash_error", None) for s in (sa, sb)]
        verdict.update(state="unknown", why="; ".join(r for r in reasons if r) or "not recorded")
    elif ma != mb:
        verdict.update(state="not comparable")
    else:
        verdict.update(state="unchanged" if ha == hb else "changed")
    return verdict


def _field(a: Any, b: Any) -> Dict[str, Any]:
    return {"a": a, "b": b, "changed": a != b}


def compare_records(a: RunContext, b: RunContext) -> Dict[str, Any]:
    """Two run records side by side, as data."""
    a_out = {s.name: s for s in a.outputs}
    b_out = {s.name: s for s in b.outputs}
    outputs = {}
    for name in sorted(set(a_out) | set(b_out)):
        sa, sb = a_out.get(name), b_out.get(name)
        outputs[name] = {
            "row_count": _field(sa.row_count if sa else None, sb.row_count if sb else None),
            "schema_hash": _field(sa.schema_hash if sa else None, sb.schema_hash if sb else None),
            "data_hash": data_hash_state(sa, sb),
        }
    return {
        "task": a.task_path,
        "a": {"run_id": a.run_id, "started_at": a.started_at, "status": a.status},
        "b": {"run_id": b.run_id, "started_at": b.started_at, "status": b.status},
        "status": _field(a.status, b.status),
        "duration_sec": _field(a.duration_sec, b.duration_sec),
        "config_hash": _field(a.config_hash, b.config_hash),
        "outputs": outputs,
    }


@lineage_app.command("compare")
def compare(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    run_id1: str = typer.Option(..., "--run-id1", help="First run ID."),
    run_id2: str = typer.Option(..., "--run-id2", help="Second run ID."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
    as_json: bool = json_option(),
):
    """Diff two run records, highlighting changes in hashes, row counts and status."""
    store = _store(str(usecase_dir / lineage_dir))
    tp = _task_path(usecase, package, task)

    try:
        a = store.load(tp, run_id1)
        b = store.load(tp, run_id2)
    except FileNotFoundError as e:
        fail(str(e), as_json=as_json)

    report = compare_records(a, b)
    if as_json:
        emit(report)
        return

    typer.echo(_LINE)
    typer.echo(f"Comparing runs for '{tp}'")
    typer.echo(f"  A: {a.run_id[:8]}  {a.started_at[:19]}  {a.status}")
    typer.echo(f"  B: {b.run_id[:8]}  {b.started_at[:19]}  {b.status}")
    typer.echo(_LINE)

    def _cmp(label: str, field: Dict[str, Any]) -> None:
        if not field["changed"]:
            typer.echo(f"  {label}: {field['a']}  (unchanged)")
        else:
            typer.secho(f"  {label}: {field['a']} -> {field['b']}  CHANGED", fg=typer.colors.YELLOW)

    _cmp("status", report["status"])
    _cmp("duration_sec", report["duration_sec"])
    _cmp("config_hash", report["config_hash"])
    for name, out in report["outputs"].items():
        typer.echo(f"  Output '{name}':")
        _cmp("    row_count", out["row_count"])
        _cmp("    schema_hash", out["schema_hash"])
        _print_data_hash(out["data_hash"])


def _print_data_hash(verdict: Dict[str, Any]) -> None:
    state = verdict["state"]
    if state == "unknown":
        typer.secho(f"      data_hash: unknown ({verdict['why']})", fg=typer.colors.YELLOW)
    elif state == "not comparable":
        typer.secho(
            f"      data_hash: not comparable (made by "
            f"{verdict['method_a'] or 'the pre-0.6 sample'} and "
            f"{verdict['method_b'] or 'the pre-0.6 sample'})",
            fg=typer.colors.YELLOW,
        )
    elif state == "unchanged":
        typer.echo(f"      data_hash: {verdict['a']}  (unchanged)")
    else:
        typer.secho(
            f"      data_hash: {verdict['a']} -> {verdict['b']}  CHANGED", fg=typer.colors.YELLOW
        )


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


@lineage_app.command("search")
def search(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    task: Optional[str] = typer.Option(None, "-t", "--task", help="Restrict to this task name."),
    usecase: Optional[str] = typer.Option(
        None, "-u", "--usecase", help="Restrict to this usecase."
    ),
    package: Optional[str] = typer.Option(
        None, "-p", "--package", help="Restrict to this package."
    ),
    status: Optional[str] = typer.Option(
        None, "--status", help="Filter by status: success|error|running."
    ),
    since: Optional[str] = typer.Option(
        None, "--since", help="Only runs started on or after this ISO date."
    ),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
    as_json: bool = json_option(),
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

    if as_json:
        emit([ctx.to_dict() for ctx in runs])
        return
    if not runs:
        typer.secho("No matching lineage records found.", fg=typer.colors.YELLOW)
        return

    typer.echo(_LINE)
    typer.echo(f"{'RUN_ID':9}  {'TASK PATH':40}  {'STARTED':19}  {'STATUS':<8}  DUR")
    typer.echo(_LINE)
    for ctx in runs:
        duration = f"{ctx.duration_sec:.1f}s" if ctx.duration_sec is not None else "-"
        typer.secho(
            f"{ctx.run_id[:8]}  {ctx.task_path:<40}  {ctx.started_at[:19]}  "
            f"{ctx.status:<8}  {duration}",
            fg=_color(ctx.status),
        )


# ---------------------------------------------------------------------------
# trace
# ---------------------------------------------------------------------------


def _print_steps(steps: List[StepRecord]) -> None:
    for step in steps:
        typer.echo(f"    [{step.format}] {step.name}")
        typer.echo(f"      location : {step.location}")
        if step.row_count is not None:
            typer.echo(f"      rows     : {step.row_count:,}")
        if step.schema_hash:
            typer.echo(f"      schema   : {step.schema_hash}")
        if step.data_hash:
            typer.echo(f"      data     : {step.data_hash}")
        elif getattr(step, "hash_error", None):
            typer.echo(f"      data     : unavailable ({step.hash_error})")


@lineage_app.command("trace")
def trace(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Specific run (default: latest)."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
    as_json: bool = json_option(),
):
    """Print the input, transform and output data flow graph for a run."""
    store = _store(str(usecase_dir / lineage_dir))
    ctx = _load_one(store, _task_path(usecase, package, task), run_id, as_json=as_json)

    if as_json:
        record = ctx.to_dict()
        emit(
            {
                "task": ctx.task_path,
                "run_id": ctx.run_id,
                "status": ctx.status,
                "inputs": record["inputs"],
                "transform": "transformations.py",
                "outputs": record["outputs"],
            }
        )
        return

    typer.echo()
    typer.secho(f"Lineage trace: {ctx.task_path}", bold=True)
    typer.echo(f"Run:     {ctx.run_id}  [{ctx.status}]  {ctx.started_at[:19]}")
    typer.echo(f"Version: {ctx.model} v{ctx.version}")
    typer.echo()
    typer.secho("  INPUTS", fg=typer.colors.CYAN)
    _print_steps(ctx.inputs)
    typer.echo()
    typer.secho("  TRANSFORM", fg=typer.colors.CYAN)
    typer.echo("    (transformations.py)")
    typer.echo()
    typer.secho("  OUTPUTS", fg=typer.colors.CYAN)
    _print_steps(ctx.outputs)
    typer.echo()
