"""``ubunye prove``: collect run records from many environments, compare, report.

ubunye prove observe --workload c01 --env pandas-local -d . -u uc -p pkg -t task -o evidence
ubunye prove observe --workload c01 --env aws-glue --record glue.json -o evidence
ubunye prove skip    --workload c01 --env azure --status not_run --reason "..." -o evidence
ubunye prove report  evidence --workload c01 --reference spark-local --expect aws-glue,...
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer

prove_app = typer.Typer(
    name="prove",
    help="The proving ground: compare one workload's run records across environments.",
    no_args_is_help=True,
)


def _record(
    record: Optional[Path],
    usecase_dir: Optional[Path],
    usecase: Optional[str],
    package: Optional[str],
    task: Optional[str],
    run_id: str,
) -> dict:
    if record is not None:
        doc = json.loads(record.read_text(encoding="utf-8"))
        if isinstance(doc, list):  # `lineage list --json`: newest
            doc = sorted(doc, key=lambda r: r["started_at"])[-1]
        return doc
    if not (usecase_dir and usecase and package and task):
        raise typer.BadParameter("give --record FILE, or -d -u -p -t for a stored run")
    from ubunye.cli.gate import _from_store

    lineage_dir = usecase_dir / ".ubunye" / "lineage"
    which = "latest" if run_id == "latest" else run_id
    return _from_store(f"{usecase}/{package}/{task}", lineage_dir, which).to_dict()


@prove_app.command("observe")
def observe(
    workload: str = typer.Option(
        ..., "--workload", help="The workload's id, e.g. c01-portable-etl."
    ),
    env: str = typer.Option(..., "--env", help="The environment's name, e.g. spark-local."),
    out: Path = typer.Option(Path("evidence"), "-o", "--out", help="The evidence folder."),
    record: Optional[Path] = typer.Option(None, "--record", help="A run record JSON file."),
    usecase_dir: Optional[Path] = typer.Option(None, "-d", "--usecase-dir"),
    usecase: Optional[str] = typer.Option(None, "-u", "--usecase"),
    package: Optional[str] = typer.Option(None, "-p", "--package"),
    task: Optional[List[str]] = typer.Option(
        None,
        "-t",
        "--task",
        help="A task. Give several (-t a -t b) to observe each as <workload>-<task>.",
    ),
    run_id: str = typer.Option("latest", "--run-id", help="A stored run's id, or 'latest'."),
    kind: str = typer.Option("local", "--kind", help="local, cloud or managed."),
    provider: str = typer.Option("", "--provider", help="aws, gcp, azure, databricks, ..."),
    runtime: str = typer.Option(
        "", "--runtime", help="e.g. 'Glue 5.0', 'Dataproc Serverless 2.2'."
    ),
    region: str = typer.Option("", "--region"),
    cost_amount: Optional[float] = typer.Option(None, "--cost-amount"),
    cost_currency: str = typer.Option("USD", "--cost-currency"),
    cost_basis: str = typer.Option(
        "", "--cost-basis", help="actual, provider_estimate, ubunye_estimate, local, unknown."
    ),
    cost_source: str = typer.Option("", "--cost-source", help="How the figure was obtained."),
    run_url: str = typer.Option("", "--run-url", help="Where a reader can check this run."),
) -> None:
    """Turn a run record into an observation of WORKLOAD in ENV.

    Several tasks (``-t a -t b``) give one observation each, named
    ``<workload>-<task>``; before F-056 only the last was kept, silently.
    """
    tasks = list(task or [])
    if len(set(tasks)) < len(tasks):
        repeated = sorted({t for t in tasks if tasks.count(t) > 1})
        typer.secho(
            f"Task {', '.join(repeated)} given more than once.", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(code=2)
    if len(tasks) > 1 and (record is not None or run_id != "latest"):
        typer.secho(
            "--record and --run-id name one run: give one task with them.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    chosen: List[Optional[str]] = list(tasks) if tasks else [None]
    for one in chosen:
        name = workload if len(tasks) <= 1 else f"{workload}-{one}"
        _observe_one(
            name, env, out, record, usecase_dir, usecase, package, one, run_id, kind,
            provider, runtime, region, cost_amount, cost_currency, cost_basis, cost_source,
            run_url,
        )  # fmt: skip


def _observe_one(
    workload: str,
    env: str,
    out: Path,
    record: Optional[Path],
    usecase_dir: Optional[Path],
    usecase: Optional[str],
    package: Optional[str],
    task: Optional[str],
    run_id: str,
    kind: str,
    provider: str,
    runtime: str,
    region: str,
    cost_amount: Optional[float],
    cost_currency: str,
    cost_basis: str,
    cost_source: str,
    run_url: str,
) -> None:
    from ubunye.proving import observe_record

    doc = _record(record, usecase_dir, usecase, package, task, run_id)
    basis = cost_basis or ("local" if kind == "local" else "unknown")
    cost: Dict[str, Any] = {"basis": basis, "currency": cost_currency}
    if cost_amount is not None:
        cost["amount"] = cost_amount
    if cost_source:
        cost["source"] = cost_source
    obs = observe_record(
        doc,
        workload=workload,
        environment=env,
        platform={
            k: v
            for k, v in dict(kind=kind, provider=provider, runtime=runtime, region=region).items()
            if v
        },
        cost=cost,
        provenance={k: v for k, v in dict(run_url=run_url).items() if v},
    )
    path = obs.save(out)
    typer.echo(
        f"observed {workload} in {env}: run {doc.get('run_id', '?')[:8]} "
        f"{doc.get('status')} -> {path}"
    )


@prove_app.command("skip")
def skip(
    workload: str = typer.Option(..., "--workload"),
    env: str = typer.Option(..., "--env"),
    status: str = typer.Option(
        "not_run", "--status", help="not_run, unsupported or failed_to_launch."
    ),
    reason: str = typer.Option(..., "--reason", help="Why there is no run: said in the report."),
    out: Path = typer.Option(Path("evidence"), "-o", "--out"),
    run_url: str = typer.Option("", "--run-url"),
) -> None:
    """Record that ENV did not execute WORKLOAD, and why."""
    from ubunye.proving import skipped

    obs = skipped(
        workload=workload,
        environment=env,
        status=status,
        reason=reason,
        provenance={"run_url": run_url} if run_url else {},
    )
    typer.echo(f"{workload} in {env}: {status} -> {obs.save(out)}")


@prove_app.command("report")
def report(
    evidence: Path = typer.Argument(..., help="The evidence folder."),
    workload: str = typer.Option(..., "--workload"),
    reference: str = typer.Option(
        ..., "--reference", help="The environment others are compared with."
    ),
    expect: str = typer.Option(
        "", "--expect", help="Comma-separated environments; one without evidence is NOT RUN."
    ),
    json_out: Optional[Path] = typer.Option(None, "--json", help="Write the matrix here."),
    md_out: Optional[Path] = typer.Option(None, "--md", help="Write the table here."),
) -> None:
    """Compare every observation of WORKLOAD with REFERENCE. Exit 1 if any run disagrees."""
    from ubunye.proving import compare, load_observations, render_markdown

    observations = load_observations(evidence, workload)
    if not observations:
        typer.secho(
            f"no observations of {workload} under {evidence}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(code=2)
    try:
        matrix = compare(
            observations,
            reference=reference,
            expect=[e.strip() for e in expect.split(",") if e.strip()],
        )
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    table = render_markdown(matrix)
    if json_out:
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(matrix, indent=2, sort_keys=True), encoding="utf-8")
    if md_out:
        md_out.parent.mkdir(parents=True, exist_ok=True)
        md_out.write_text(table, encoding="utf-8")
    typer.echo(table)
    if matrix["summary"]["FAIL"]:
        raise typer.Exit(code=1)
