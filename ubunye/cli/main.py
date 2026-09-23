"""Ubunye CLI implemented with Typer.

Commands:
- init:     scaffold a new usecase/package/tasks
- validate: validate config file(s) before execution
- run:      run task(s) in a package
- plugins:  list discovered plugins
- config:   show/validate config
- plan:     show resolved IO graph
- lineage:  inspect run lineage records
- models:   manage model versions and lifecycle
- test:     run task(s) in test mode with PASS/FAIL reporting
- version:  show version
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer

from ubunye.adapters.spark.catalog import set_catalog_and_schema
from ubunye.backends.spark_backend import SparkBackend
from ubunye.cli.deploy import deploy_app
from ubunye.cli.export import export_app
from ubunye.cli.init import init_app
from ubunye.cli.lineage import lineage_app
from ubunye.cli.models import models_app
from ubunye.cli.sync import sync_app
from ubunye.cli.test_cmd import test_app
from ubunye.config import load_config
from ubunye.core.planning import build_plan
from ubunye.core.runtime import EngineContext, Registry
from ubunye.core.task_runner import execute_user_task
from ubunye.telemetry.hooks import MonitorHook

app = typer.Typer(add_completion=False, help="Ubunye Engine CLI")
app.add_typer(deploy_app)
app.add_typer(export_app)
app.add_typer(init_app)
app.add_typer(lineage_app)
app.add_typer(models_app)
app.add_typer(sync_app)
app.add_typer(test_app)


def _task_path(usecase_dir: Path, usecase: str, package: str, task: str) -> Path:
    return usecase_dir / usecase / package / task


def _template_vars(
    data_timestamp: Optional[str],
    data_timestamp_format: Optional[str],
    mode: Optional[str],
    var: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build the Jinja context for a command.

    ``--var key=value`` was documented in the README and in three docs pages and
    was never implemented, so every example that used it failed. It works now, and
    it is the same on every command that renders a config.
    """
    variables: Dict[str, Any] = {
        "dt": data_timestamp,
        "dtf": data_timestamp_format,
        "mode": mode,
    }
    for item in var or []:
        if "=" not in item:
            raise typer.BadParameter(
                f"--var expects key=value, got '{item}'. Example: --var region=gauteng"
            )
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter(f"--var has an empty key in '{item}'.")
        variables[key] = value
    return variables


@app.command()
def plugins():
    """List discovered Reader/Writer/Transform plugins."""
    reg = Registry.from_entrypoints()
    typer.echo(f"Readers:   {', '.join(sorted(reg.readers)) or '-'}")
    typer.echo(f"Writers:   {', '.join(sorted(reg.writers)) or '-'}")
    typer.echo(f"Transforms:{', '.join(sorted(reg.transforms)) or '-'}")


@app.command()
def config(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task_list: List[str] = typer.Option(..., "-t", "--task-list"),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    data_timestamp_format: Optional[str] = typer.Option(None, "-dtf", "--data-timestamp-format"),
    mode: str = typer.Option("DEV", "-m", "--mode"),
):
    """Show and validate config files."""
    variables = {"dt": data_timestamp, "dtf": data_timestamp_format, "mode": mode}
    for task in task_list:
        config_path = _task_path(usecase_dir, usecase, package, task) / "config.yaml"
        try:
            cfg = load_config(str(config_path), variables)
            _ = cfg.merged_spark_conf(mode)
            typer.secho(f"[OK] Config valid: {config_path}", fg=typer.colors.GREEN)
        except Exception as e:
            typer.secho(f"[ERROR] Config error in {task}: {e}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)


@app.command()
def validate(
    usecase_dir: Path = typer.Option(
        ..., "-d", "--usecase-dir", help="Root directory of pipelines."
    ),
    usecase: str = typer.Option(..., "-u", "--usecase", help="Use case name."),
    package: str = typer.Option(..., "-p", "--package", help="Pipeline/package name."),
    task_list: List[str] = typer.Option(None, "-t", "--task-list", help="Task(s) to validate."),
    all_tasks: bool = typer.Option(False, "--all", help="Validate all tasks in the package."),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to validate against (e.g. dev, prod)."
    ),
    mode: Optional[str] = typer.Option(
        None,
        "-m",
        "--mode",
        help="Alias for --profile, matching `run`. The two commands used different "
        "words for the same idea, which made them feel like different systems.",
    ),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    var: List[str] = typer.Option(
        None, "--var", help="Extra template variable, key=value. Repeatable."
    ),
    as_json: bool = typer.Option(
        False, "--json", help="Emit the result as JSON, for a script or an agent."
    ),
):
    """Validate config file(s) without executing the pipeline.

    Runs Pydantic schema validation and Jinja template resolution so errors
    are caught before Spark starts. Exits non-zero if any task fails.

    Examples
    --------
    ubunye validate -d ./pipelines -u fraud_detection -p ingestion -t claim_etl

    ubunye validate -d ./pipelines -u fraud_detection -p ingestion --all

    ubunye validate -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --profile dev
    """
    package_dir = usecase_dir / usecase / package

    # Resolve which tasks to validate
    tasks_to_check: List[str] = list(task_list or [])
    if all_tasks:
        if not package_dir.exists():
            typer.secho(
                f"[ERROR] Package directory not found: {package_dir}", fg=typer.colors.RED, err=True
            )
            raise typer.Exit(code=1)
        tasks_to_check = [
            d.name
            for d in sorted(package_dir.iterdir())
            if d.is_dir() and (d / "config.yaml").exists()
        ]
        if not tasks_to_check:
            typer.secho(
                f"[WARN] No config.yaml files found under {package_dir}", fg=typer.colors.YELLOW
            )
            return

    if not tasks_to_check:
        typer.secho("[ERROR] Specify -t/--task-list or use --all", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    # -m and --profile are one idea. Whichever was given wins; both is a conflict.
    if mode and profile and mode != profile:
        typer.secho(
            f"[ERROR] --profile {profile} and -m {mode} disagree. Pass one.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)
    profile = profile or mode

    # Inject the SAME template variables `run` injects. validate used to pass only
    # dt, so a config using {{ mode }} or {{ dtf }} ran fine and failed validation —
    # the one command whose whole job is to catch problems BEFORE the run invented
    # one the run did not have.
    variables = _template_vars(data_timestamp, data_timestamp, profile or "DEV", var)
    failed = 0
    results = []

    for task in tasks_to_check:
        task_dir = _task_path(usecase_dir, usecase, package, task)
        try:
            load_config(str(task_dir), variables=variables, profile=profile)
            results.append({"task": task, "ok": True, "problems": []})
            if not as_json:
                typer.secho(f"  [OK]   {task}", fg=typer.colors.GREEN)
        except (ValueError, FileNotFoundError) as e:
            results.append({"task": task, "ok": False, "problems": str(e).splitlines()})
            if not as_json:
                typer.secho(f"  [FAIL] {task}", fg=typer.colors.RED)
                # Indent error details for readability
                for line in str(e).splitlines():
                    typer.echo(f"         {line}")
            failed += 1

    if as_json:
        typer.echo(
            json.dumps(
                {"ok": not failed, "checked": len(tasks_to_check), "results": results},
                indent=2,
                default=str,
            )
        )
    else:
        typer.echo()
        if failed:
            typer.secho(
                f"{failed}/{len(tasks_to_check)} task(s) failed validation.", fg=typer.colors.RED
            )
        else:
            typer.secho(
                f"All {len(tasks_to_check)} task(s) passed validation.", fg=typer.colors.GREEN
            )
    if failed:
        raise typer.Exit(code=1)


@app.command()
def plan(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task_list: List[str] = typer.Option(..., "-t", "--task-list"),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    data_timestamp_format: Optional[str] = typer.Option(None, "-dtf", "--data-timestamp-format"),
    mode: str = typer.Option("DEV", "-m", "--mode"),
    var: List[str] = typer.Option(
        None, "--var", help="Extra template variable, key=value. Repeatable."
    ),
    backend_kind: str = typer.Option(
        "spark", "--backend", help="Backend the plan should assume: 'spark' or 'pandas'."
    ),
    as_json: bool = typer.Option(
        False, "--json", help="Emit the plan as JSON, for a script or an agent."
    ),
):
    """Dry run a task: what it will read, what it will write, and what will stop it.

    Nothing here starts a session, reads a table or costs money. It resolves the
    config, checks the environment variables it needs, looks for the local inputs,
    loads the transform class and asks every writer to resolve its write mode.
    Exits non-zero when the run would fail.
    """
    variables = _template_vars(data_timestamp, data_timestamp_format, mode, var)
    plans = []
    failed = 0

    for task in task_list:
        task_dir = _task_path(usecase_dir, usecase, package, task)
        config_path = task_dir / "config.yaml"
        try:
            cfg = load_config(str(config_path), variables)
        except (ValueError, FileNotFoundError) as exc:
            failed += 1
            report = {
                "task": f"{usecase}/{package}/{task}",
                "ok": False,
                "problems": [str(exc)],
                "warnings": [],
            }
            plans.append(report)
            if not as_json:
                typer.secho(f"[FAIL] {task}", fg=typer.colors.RED)
                for line in str(exc).splitlines():
                    typer.echo(f"       {line}")
            continue

        raw_yaml = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
        report = build_plan(
            cfg,
            task_dir=task_dir,
            task_name=f"{usecase}/{package}/{task}",
            backend=backend_kind,
            variables=variables,
            raw_yaml=raw_yaml,
        )
        plans.append(report)
        if not report["ok"]:
            failed += 1
        if not as_json:
            _render_plan(report)

    if as_json:
        typer.echo(json.dumps(plans if len(plans) > 1 else plans[0], indent=2, default=str))

    if failed:
        raise typer.Exit(code=1)


def _render_plan(report: dict) -> None:
    """Print one plan for a person to read."""
    typer.echo(f"--- Task: {report['task']} ---")
    typer.echo(f"Backend: {report['backend']}   Config: {report['config_hash'][:19]}")

    typer.echo("Reads:")
    for item in report["inputs"]:
        detail = f"  - {item['name']}: {item['format']} <- {item['location']}"
        if item.get("checked"):
            if item.get("exists"):
                detail += f"  [{item['files']} file(s), {item['bytes']} bytes]"
            else:
                detail += "  [MISSING]"
        typer.echo(detail)

    tform = report["transform"]
    label = tform.get("class") or tform.get("type") or "transformations.py"
    typer.echo(f"Transform: {label}")

    typer.echo("Writes:")
    for item in report["outputs"]:
        mode_txt = item.get("resolved_mode") or "?"
        detail = f"  - {item['name']}: {item['format']} -> {item['location']}  mode={mode_txt}"
        if item.get("merge_keys"):
            detail += f"  keys={','.join(item['merge_keys'])}"
        typer.echo(detail)

    for warning in report["warnings"]:
        typer.secho(f"  warn: {warning}", fg=typer.colors.YELLOW)
    for problem in report["problems"]:
        typer.secho(f"  STOP: {problem}", fg=typer.colors.RED)

    if report["ok"]:
        typer.secho("Plan is runnable.", fg=typer.colors.GREEN)
    else:
        typer.secho(
            f"Plan would fail: {len(report['problems'])} problem(s).", fg=typer.colors.RED
        )
    typer.echo()


@app.command()
def run(
    usecase_dir: Path = typer.Option(
        ..., "-d", "--usecase-dir", help="Specifies the directory path for the use case."
    ),
    usecase: str = typer.Option(..., "-u", "--usecase", help="Selects the desired use case."),
    package: str = typer.Option(
        ..., "-p", "--package", help="Selects a package from the specified use case."
    ),
    task_list: List[str] = typer.Option(
        None, "-t", "--task-list", help="Specifies the task(s) to execute from the chosen package."
    ),
    all_tasks: bool = typer.Option(
        False,
        "--all",
        help="Run every task in the package. `validate` had this for years and `run` "
        "did not, so the two commands taught different habits for the same job.",
    ),
    data_timestamp: Optional[str] = typer.Option(
        None, "-dt", "--data-timestamp", help="Provides a data timestamp in the specified format."
    ),
    data_timestamp_format: Optional[str] = typer.Option(
        None, "-dtf", "--data-timestamp-format", help="Specifies the format for the data timestamp."
    ),
    mode: str = typer.Option("DEV", "-m", "--mode", help="Selects the run mode (DEV/PROD)."),
    deploy_mode: str = typer.Option(
        "client",
        "--deploy-mode",
        help="Specifies the deployment mode (cluster/client). Defaults to client.",
    ),
    lineage: bool = typer.Option(
        False, "--lineage", help="Record lineage for this run (stored as JSON under --lineage-dir)."
    ),
    lineage_dir: str = typer.Option(
        ".ubunye/lineage", "--lineage-dir", help="Root directory for lineage records."
    ),
    backend_kind: str = typer.Option(
        "spark",
        "--backend",
        help="Execution backend: 'spark' (default) or 'pandas' (a laptop run with "
        "no Spark and no JVM; path-based csv/parquet/json only).",
    ),
    var: List[str] = typer.Option(
        None, "--var", help="Extra template variable, key=value. Repeatable."
    ),
    sample: Optional[int] = typer.Option(
        None,
        "--sample",
        help="Bound every input to N rows. The cheap check: same code, same "
        "connectors, a slice of the data. The receipt records that it was sampled.",
    ),
    as_json: bool = typer.Option(
        False, "--json", help="Emit the result as JSON, for a script or an agent."
    ),
):
    """Run one or more tasks within a package sequentially."""
    variables = _template_vars(data_timestamp, data_timestamp_format, mode, var)

    # Resolve which tasks to run, the same way `validate` resolves them. The two
    # commands answered the same question differently for years: validate had
    # --all, run made you repeat -t per task.
    tasks_to_run: List[str] = list(task_list or [])
    if all_tasks:
        package_dir = usecase_dir / usecase / package
        if not package_dir.exists():
            typer.secho(
                f"[ERROR] Package directory not found: {package_dir}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=1)
        tasks_to_run = sorted(
            entry.name
            for entry in package_dir.iterdir()
            if entry.is_dir() and (entry / "config.yaml").exists()
        )
    if not tasks_to_run:
        typer.secho(
            "[ERROR] No tasks selected. Pass -t <task> (repeatable) or --all.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)
    task_list = tasks_to_run

    # Load and validate all configs before starting Spark — fails fast on bad configs
    configs = {}
    for task in task_list:
        task_dir = _task_path(usecase_dir, usecase, package, task)
        if not (task_dir / "config.yaml").exists():
            typer.secho(
                f"[ERROR] Missing config at {task_dir / 'config.yaml'}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=1)
        try:
            configs[task] = load_config(str(task_dir), variables)
        except (ValueError, FileNotFoundError) as e:
            typer.secho(
                f"[ERROR] Config validation failed for '{task}':\n{e}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=1)

    first_cfg = configs[task_list[0]]
    spark_conf = first_cfg.merged_spark_conf(mode)
    spark_conf["spark.submit.deployMode"] = deploy_mode

    run_id = str(uuid.uuid4())
    if backend_kind == "pandas":
        from ubunye.backends.pandas_backend import PandasBackend

        backend = PandasBackend(app_name=f"ubunye:{package}")
    elif backend_kind == "spark":
        backend = SparkBackend(app_name=f"ubunye:{package}", conf=spark_conf)
    else:
        typer.secho(
            f"[ERROR] Unknown --backend '{backend_kind}'. Use 'spark' or 'pandas'.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    # Build a lineage recorder if --lineage was requested
    lineage_recorder = None
    if lineage:
        from ubunye.lineage.recorder import LineageRecorder

        lineage_recorder = LineageRecorder(
            store="filesystem",
            base_dir=str(usecase_dir / lineage_dir),
        )

    backend.start()
    set_catalog_and_schema(
        backend,
        catalog=first_cfg.resolved_catalog(mode),
        schema=first_cfg.resolved_schema(mode),
    )
    extra_hooks = [MonitorHook(lineage_recorder)] if lineage_recorder is not None else []
    results: List[Dict[str, Any]] = []
    try:
        for task in task_list:
            if not as_json:
                typer.echo(f"Starting task: {task} (Mode: {mode}, Deploy: {deploy_mode})")
            cfg = configs[task]
            task_dir = _task_path(usecase_dir, usecase, package, task)
            context = EngineContext(
                run_id=run_id, profile=mode, task_name=f"{usecase}/{package}/{task}"
            )
            started = time.perf_counter()
            try:
                execute_user_task(
                    backend,
                    task_dir,
                    cfg,
                    context,
                    extra_hooks=extra_hooks,
                    sample_rows=sample,
                )
            except Exception as e:
                results.append(
                    {
                        "task": task,
                        "ok": False,
                        "run_id": run_id,
                        "error": str(e),
                        "duration_sec": round(time.perf_counter() - started, 3),
                    }
                )
                if as_json:
                    typer.echo(
                        json.dumps({"ok": False, "results": results}, indent=2, default=str)
                    )
                else:
                    typer.secho(
                        f"[ERROR] Run failed for {task}: {e}", fg=typer.colors.RED, err=True
                    )
                raise
            results.append(
                {
                    "task": task,
                    "ok": True,
                    "run_id": run_id,
                    "backend": backend_kind,
                    "sampled_rows": sample,
                    "duration_sec": round(time.perf_counter() - started, 3),
                    "receipt": (
                        str(usecase_dir / lineage_dir / usecase / package / task / f"{run_id}.json")
                        if lineage_recorder is not None
                        else None
                    ),
                }
            )
            if not as_json:
                typer.secho(f"[OK] Run complete for {task}", fg=typer.colors.GREEN)
                if sample:
                    typer.secho(
                        f"     sampled run: every input bounded to {sample} rows",
                        fg=typer.colors.YELLOW,
                    )
    finally:
        backend.stop()

    if as_json:
        typer.echo(json.dumps({"ok": True, "results": results}, indent=2, default=str))


@app.command()
def receipt(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Default: the latest run."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
):
    """Print the receipt for a run: what it read, what it wrote, and the hashes.

    This is the record `ubunye run --lineage` leaves behind. It is JSON on purpose:
    a person can read it, a script can diff it, and an agent can check its own work
    against it without parsing prose.
    """
    from ubunye.lineage.storage import FileSystemLineageStore

    store = FileSystemLineageStore(str(usecase_dir / lineage_dir))
    task_path = f"{usecase}/{package}/{task}"
    try:
        if run_id:
            ctx = store.load(task_path, run_id)
        else:
            runs = store.list_runs(task_path, n=1)
            if not runs:
                typer.secho(
                    f"No receipt for '{task_path}'. Run it with --lineage first.",
                    fg=typer.colors.YELLOW,
                    err=True,
                )
                raise typer.Exit(code=1)
            ctx = runs[0]
    except FileNotFoundError as exc:
        typer.secho(f"[ERROR] {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    typer.echo(json.dumps(ctx.to_dict(), indent=2, default=str))


@app.command()
def version():
    from ubunye import __version__

    typer.echo(f"Ubunye Engine v{__version__}")
