"""``ubunye gate``: fail a pull request when a run's receipt regresses.

Two ways to name the runs:

    # two run records exported with `ubunye lineage show --json`
    ubunye gate --baseline base.json --candidate head.json

    # from the lineage store: by run id, or `latest` / `previous`
    ubunye gate -d pipelines -u shop -p orders -t clean                # previous vs latest
    ubunye gate -d pipelines -u shop -p orders -t clean --baseline 1a2b3c4d

Exit code 1 when any rule fails. ``--summary FILE`` appends a Markdown table
(point it at ``$GITHUB_STEP_SUMMARY`` in CI). See :mod:`ubunye.core.gate`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

import typer

from ubunye.cli.output import emit, fail, json_option
from ubunye.core import gate as gate_rules
from ubunye.lineage.context import RunContext

_MARK = {
    gate_rules.OK: ("[OK]", typer.colors.GREEN),
    gate_rules.WARN: ("[WARN]", typer.colors.YELLOW),
    gate_rules.FAIL: ("[FAIL]", typer.colors.RED),
}


def _from_store(task_path: str, lineage_dir: Path, which: str) -> RunContext:
    from ubunye.lineage.storage import FileSystemLineageStore

    store = FileSystemLineageStore(base_dir=str(lineage_dir))
    if which in ("latest", "previous"):
        runs: List[RunContext] = sorted(
            store.list_runs(task_path, n=1_000_000), key=lambda r: r.started_at, reverse=True
        )
        index = 0 if which == "latest" else 1
        if len(runs) <= index:
            raise FileNotFoundError(
                f"'{which}' needs {index + 1} recorded run(s) of {task_path}; found {len(runs)}"
            )
        return runs[index]
    # A run id, or its first characters as `lineage list` prints them.
    for run in store.list_runs(task_path, n=1_000_000):
        if run.run_id == which or run.run_id.startswith(which):
            return run
    raise FileNotFoundError(f"no run '{which}' of {task_path} in {lineage_dir}")


def _load(which: str, task_path: Optional[str], lineage_dir: Optional[Path]) -> RunContext:
    path = Path(which)
    if path.suffix == ".json" or path.is_file():
        doc = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(doc, list):  # `lineage list --json` output: take the newest
            doc = sorted(doc, key=lambda r: r["started_at"])[-1]
        return RunContext.from_dict(doc)
    if task_path is None or lineage_dir is None:
        raise FileNotFoundError(
            f"'{which}' is not a run record file; to name a stored run, also give -d -u -p -t"
        )
    return _from_store(task_path, lineage_dir, which)


def gate_command(
    baseline: str = typer.Option(
        "previous", "--baseline", help="A run record file, a run id, or 'previous'."
    ),
    candidate: str = typer.Option(
        "latest", "--candidate", help="A run record file, a run id, or 'latest'."
    ),
    usecase_dir: Optional[Path] = typer.Option(None, "-d", "--usecase-dir"),
    usecase: Optional[str] = typer.Option(None, "-u", "--usecase"),
    package: Optional[str] = typer.Option(None, "-p", "--package"),
    task: Optional[str] = typer.Option(None, "-t", "--task"),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
    allow_data_change: bool = typer.Option(
        False, "--allow-data-change", help="A changed output warns instead of failing."
    ),
    max_slowdown: Optional[float] = typer.Option(
        None,
        "--max-slowdown",
        help="Fail when slower than the baseline by more than this (0.5 = 50%).",
    ),
    max_seconds: Optional[float] = typer.Option(
        None, "--max-seconds", help="Fail when the candidate took longer than this."
    ),
    max_row_change: Optional[float] = typer.Option(
        None,
        "--max-row-change",
        help="Fail when an output's row count moved by more than this (0.1 = 10%).",
    ),
    summary: Optional[Path] = typer.Option(
        None, "--summary", help="Append a Markdown table to this file ($GITHUB_STEP_SUMMARY)."
    ),
    as_json: bool = json_option(),
) -> None:
    """Compare a run's receipt with a baseline; exit 1 if it regresses."""
    task_path = f"{usecase}/{package}/{task}" if usecase and package and task else None
    store_dir = usecase_dir / lineage_dir if usecase_dir is not None else None
    try:
        base = _load(baseline, task_path, store_dir)
        cand = _load(candidate, task_path, store_dir)
    except (FileNotFoundError, ValueError, KeyError) as exc:
        fail(str(exc), as_json=as_json)

    policy = gate_rules.Policy(
        allow_data_change=allow_data_change,
        max_slowdown=max_slowdown,
        max_seconds=max_seconds,
        max_row_change=max_row_change,
    )
    findings = gate_rules.evaluate(base, cand, policy)
    ok = gate_rules.passed(findings)

    if summary is not None:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(gate_rules.markdown(findings, base, cand))

    if as_json:
        emit(
            {
                "ok": ok,
                "task": cand.task_path,
                "baseline": {"run_id": base.run_id, "version": base.version},
                "candidate": {"run_id": cand.run_id, "version": cand.version},
                "findings": [f.as_dict() for f in findings],
            }
        )
    else:
        typer.echo(f"gate: {cand.task_path}  {base.run_id[:8]} -> {cand.run_id[:8]}")
        for f in findings:
            mark, colour = _MARK[f.status]
            where = f" {f.output}:" if f.output else ""
            typer.secho(f"{mark} {f.rule}{where}", fg=colour, nl=False)
            typer.echo(f" {f.detail}")
        typer.echo("\npasses." if ok else "\nfails.")
    if not ok:
        raise typer.Exit(code=1)
