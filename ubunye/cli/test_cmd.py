"""``ubunye test run`` — run task(s) in test mode and report PASS/FAIL.

This command is identical to ``ubunye run`` but defaults to ``--profile test``
and always enables lineage recording so each test run is traceable. It reports
a clear per-task PASS/FAIL result and exits non-zero if any task fails.

Usage
-----
    ubunye test run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl
    ubunye test run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --profile dev
    ubunye test run -d ./pipelines -u fraud_detection -p ingestion -t task1 -t task2
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import List, Optional

import typer

from ubunye.backends.spark_backend import SparkBackend
from ubunye.config import load_config
from ubunye.core.runtime import EngineContext
from ubunye.core.task_runner import execute_user_task
from ubunye.telemetry.hooks import MonitorHook

test_app = typer.Typer(
    name="test", help="Run task(s) in test mode and report PASS/FAIL.", add_completion=False
)


def _task_path(usecase_dir: Path, usecase: str, package: str, task: str) -> Path:
    return usecase_dir / usecase / package / task


@test_app.command("run")
def run_test(
    usecase_dir: Path = typer.Option(
        ..., "-d", "--usecase-dir", help="Root directory of pipelines."
    ),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task_list: List[str] = typer.Option(..., "-t", "--task-list", help="Task(s) to test."),
    profile: str = typer.Option("test", "--profile", help="Config profile to use (default: test)."),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    lineage: bool = typer.Option(
        True, "--lineage/--no-lineage", help="Record lineage for each test run."
    ),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir"),
):
    """Run one or more tasks with a test profile and report PASS/FAIL per task.

    Config is validated before Spark starts; invalid configs are reported as
    [CONFIG FAIL] and the command exits non-zero immediately.

    Examples
    --------
    ubunye test run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl

    ubunye test run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --profile dev
    """
    variables = {"dt": data_timestamp}
    failed = 0
    passed = 0

    # --- Phase 1: Validate all configs before touching Spark ---
    configs = {}
    for task in task_list:
        task_dir = _task_path(usecase_dir, usecase, package, task)
        if not (task_dir / "config.yaml").exists():
            typer.secho(
                f"  [CONFIG FAIL] {task}: config.yaml not found at {task_dir}", fg=typer.colors.RED
            )
            failed += 1
            continue
        try:
            cfg = load_config(
                str(task_dir), variables=variables, profile=profile if profile != "test" else None
            )
            configs[task] = cfg
            typer.secho(f"  [CONFIG OK]   {task}", fg=typer.colors.GREEN)
        except (ValueError, FileNotFoundError) as e:
            typer.secho(f"  [CONFIG FAIL] {task}", fg=typer.colors.RED)
            for line in str(e).splitlines():
                typer.echo(f"               {line}")
            failed += 1

    if failed:
        typer.echo()
        typer.secho(
            f"{failed}/{len(task_list)} task(s) failed config validation.", fg=typer.colors.RED
        )
        raise typer.Exit(code=1)

    # --- Phase 2: Run valid tasks ---
    first_cfg = configs[task_list[0]]
    spark_conf = first_cfg.merged_spark_conf(profile)
    backend = SparkBackend(app_name=f"ubunye-test:{package}", conf=spark_conf)

    lineage_recorder = None
    if lineage:
        from ubunye.lineage.recorder import LineageRecorder

        lineage_recorder = LineageRecorder(
            store="filesystem",
            base_dir=str(usecase_dir / lineage_dir),
        )

    run_id = str(uuid.uuid4())
    backend.start()
    extra_hooks = [MonitorHook(lineage_recorder)] if lineage_recorder is not None else []
    try:
        for task in task_list:
            cfg = configs[task]
            task_dir = _task_path(usecase_dir, usecase, package, task)
            context = EngineContext(
                run_id=run_id, profile=profile, task_name=f"{usecase}/{package}/{task}"
            )
            task_start = time.perf_counter()
            try:
                execute_user_task(
                    backend, task_dir, cfg, context, extra_hooks=extra_hooks
                )
                duration = time.perf_counter() - task_start
                typer.secho(f"  [PASS] {task}  ({duration:.1f}s)", fg=typer.colors.GREEN)
                passed += 1
            except Exception as e:
                duration = time.perf_counter() - task_start
                typer.secho(f"  [FAIL] {task}  ({duration:.1f}s): {e}", fg=typer.colors.RED)
                failed += 1
    finally:
        backend.stop()

    typer.echo()
    total = passed + failed
    if failed:
        typer.secho(f"{failed}/{total} task(s) FAILED.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    else:
        typer.secho(f"All {total} task(s) PASSED.", fg=typer.colors.GREEN)


