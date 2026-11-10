"""``ubunye runs``: questions a scheduler asks, answered from run records (ADR 013).

    ubunye runs health   -d DIR (-u USECASE [-p PKG] | --task uc/pkg/t ...) [--max-age 36h]
    ubunye runs wait-for uc/pkg/t -d DIR [-dt DT] [--var k=v] [--timeout 2h]

Both read the run record store chosen by name (``--lineage-backend``,
``--lineage-dir``, ADR 011), so jobs that share no disk can check each other.
Both exit 1 when the answer is no, so they can gate a scheduled job.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import typer

from ubunye.cli.lineage import lineage_backend_option, open_store
from ubunye.cli.output import emit, fail, json_option
from ubunye.cli.variables import cli_variables, var_option
from ubunye.lineage import health as health_q
from ubunye.lineage import upstream as upstream_q

runs_app = typer.Typer(
    name="runs",
    help="Job health and upstream checks, from run records. Exit 1 when the answer is no.",
    add_completion=False,
)


def _duration(value: Optional[str], flag: str, as_json: bool) -> Optional[float]:
    if value is None:
        return None
    try:
        return health_q.parse_duration(value)
    except ValueError as exc:
        fail(f"{flag}: {exc}", as_json=as_json)


_COLOURS = {
    "ok": typer.colors.GREEN,
    "running": typer.colors.CYAN,
    "never_run": typer.colors.YELLOW,
}


@runs_app.command("health")
def health(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", help="Root of the pipelines."),
    usecase: Optional[str] = typer.Option(
        None, "-u", "--usecase", help="Every task of this use case (folders and records)."
    ),
    package: Optional[str] = typer.Option(None, "-p", "--package", help="Only this package."),
    tasks: Optional[List[str]] = typer.Option(
        None, "--task", help="A task as usecase/package/task. Repeatable."
    ),
    max_age: Optional[str] = typer.Option(
        None,
        "--max-age",
        help="Fail a task whose last success is older than this (90s, 15m, 36h, 2d).",
    ),
    stuck_after: str = typer.Option(
        "6h",
        "--stuck-after",
        help="A run still 'running' after this long likely died, and is reported.",
    ),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
    lineage_backend: Optional[str] = lineage_backend_option(),
    as_json: bool = json_option(),
) -> None:
    """For each task: last status, last success, failures in a row, runs likely dead.

    Exit 1 when any task is failing, has a run likely dead, or (with --max-age) has
    not succeeded recently enough. Schedule it as a check.
    """
    age = _duration(max_age, "--max-age", as_json)
    stuck = _duration(stuck_after, "--stuck-after", as_json)
    if not usecase and not tasks:
        fail("Say which tasks: -u <usecase> [-p <package>] or --task uc/pkg/t.", as_json=as_json)
    store = open_store(usecase_dir, lineage_dir, lineage_backend, as_json=as_json)
    names: List[str] = list(tasks or [])
    if usecase:
        names += [
            t for t in health_q.tasks_for(store, usecase_dir, usecase, package) if t not in names
        ]
    if not names:
        fail(
            f"No tasks found for '{usecase}{'/' + package if package else ''}': no task "
            f"folders under {usecase_dir} and no run records in the store.",
            as_json=as_json,
        )
    report = health_q.check(store, names, max_age=age, stuck_after=stuck or 0)
    ok = all(h.healthy for h in report)
    if as_json:
        emit({"ok": ok, "tasks": [h.as_dict() for h in report]})
    else:
        _print(report)
    if not ok:
        raise typer.Exit(code=1)


def _ago(seconds: Optional[float]) -> str:
    return "-" if seconds is None else health_q.format_duration(seconds) + " ago"


def _print(report: List[Any]) -> None:
    width = max(len("TASK"), *(len(h.task) for h in report))
    typer.echo(f"{'TASK':<{width}}  {'STATE':<9}  {'LAST':<11}  {'SUCCEEDED':<10}  FAILS  STUCK")
    for h in report:
        line = (
            f"{h.task:<{width}}  {h.state:<9}  {h.last_status or '-':<11}  "
            f"{_ago(h.success_age_sec):<10}  {h.consecutive_failures:>5}  {len(h.stuck_runs):>5}"
        )
        typer.secho(line, fg=_COLOURS.get(h.state, typer.colors.RED))
    problems = [(h.task, p) for h in report for p in h.problems]
    if problems:
        typer.echo("")
        for task, p in problems:
            typer.secho(f"{task}: {p}", fg=typer.colors.RED)


@runs_app.command("wait-for")
def wait_for(
    task: str = typer.Argument(..., help="The upstream task, usecase/package/task."),
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", help="Root of the pipelines."),
    data_timestamp: Optional[str] = typer.Option(
        None, "-dt", "--data-timestamp", help="The batch's dt."
    ),
    var: Optional[List[str]] = var_option(),
    timeout: str = typer.Option(
        "0",
        "--timeout",
        help="Wait up to this long (90s, 15m, 2h). Default 0: look once.",
    ),
    poll: str = typer.Option("60s", "--poll", help="How often to look again while waiting."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
    lineage_backend: Optional[str] = lineage_backend_option(),
    as_json: bool = json_option(),
) -> None:
    """Exit 0 once TASK has succeeded for this batch (-dt and every --var); else exit 1.

    Put it before a job that must not start until TASK has finished, such as a
    payment run after the monthly calculation. A task can say the same in its own
    config with CONFIG.requires.
    """
    _duration(timeout, "--timeout", as_json)
    _duration(poll, "--poll", as_json)
    variables = cli_variables(dt=data_timestamp, dtf=None, mode=None, var=var)
    store = open_store(usecase_dir, lineage_dir, lineage_backend, as_json=as_json)
    try:
        req = upstream_q.Requirement(task=task, variables=None, wait=timeout, poll=poll)
        found = upstream_q.wait_for(store, req, variables)
    except upstream_q.UpstreamNotReady as exc:
        fail(str(exc), as_json=as_json)
    if as_json:
        emit(
            {
                "ok": True,
                "task": task,
                "run_id": found.run_id,
                "ended_at": found.ended_at,
                "variables": found.variables,
            }
        )
    else:
        typer.secho(
            f"[OK] {task} succeeded for this batch (run {found.run_id}, "
            f"ended {found.ended_at or found.started_at}).",
            fg=typer.colors.GREEN,
        )
