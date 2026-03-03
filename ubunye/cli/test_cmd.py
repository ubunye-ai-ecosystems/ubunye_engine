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

import sys
import time
import uuid
from pathlib import Path
from typing import List, Optional

import typer

from ubunye.config import load_config
from ubunye.core.runtime import EngineContext, Registry
from ubunye.backends.spark_backend import SparkBackend
from ubunye.telemetry.monitors import load_monitors, safe_call

test_app = typer.Typer(name="test", help="Run task(s) in test mode and report PASS/FAIL.", add_completion=False)


def _task_path(usecase_dir: Path, usecase: str, package: str, task: str) -> Path:
    return usecase_dir / usecase / package / task


@test_app.command("run")
def run_test(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", help="Root directory of pipelines."),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task_list: List[str] = typer.Option(..., "-t", "--task-list", help="Task(s) to test."),
    profile: str = typer.Option("test", "--profile", help="Config profile to use (default: test)."),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    lineage: bool = typer.Option(True, "--lineage/--no-lineage", help="Record lineage for each test run."),
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
            typer.secho(f"  [CONFIG FAIL] {task}: config.yaml not found at {task_dir}", fg=typer.colors.RED)
            failed += 1
            continue
        try:
            cfg = load_config(str(task_dir), variables=variables, profile=profile if profile != "test" else None)
            configs[task] = cfg
            typer.secho(f"  [CONFIG OK]   {task}", fg=typer.colors.GREEN)
        except (ValueError, FileNotFoundError) as e:
            typer.secho(f"  [CONFIG FAIL] {task}", fg=typer.colors.RED)
            for line in str(e).splitlines():
                typer.echo(f"               {line}")
            failed += 1

    if failed:
        typer.echo()
        typer.secho(f"{failed}/{len(task_list)} task(s) failed config validation.", fg=typer.colors.RED)
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
    try:
        for task in task_list:
            cfg = configs[task]
            task_dir = _task_path(usecase_dir, usecase, package, task)
            context = EngineContext(run_id=run_id, profile=profile, task_name=f"{usecase}/{package}/{task}")
            monitors = load_monitors(cfg.model_dump(mode="json"))
            if lineage_recorder is not None:
                monitors = [lineage_recorder] + monitors

            for monitor in monitors:
                safe_call(monitor, "task_start", context=context, config=cfg.model_dump(mode="json"))

            task_start = time.perf_counter()
            try:
                _run_task(backend, task_dir, cfg, monitors, context)
                duration = time.perf_counter() - task_start
                typer.secho(f"  [PASS] {task}  ({duration:.1f}s)", fg=typer.colors.GREEN)
                passed += 1
            except Exception as e:
                duration = time.perf_counter() - task_start
                typer.secho(f"  [FAIL] {task}  ({duration:.1f}s): {e}", fg=typer.colors.RED)
                failed += 1
                for monitor in monitors:
                    safe_call(monitor, "task_end", context=context, config=cfg.model_dump(mode="json"),
                              outputs=None, status="error", duration_sec=duration)
    finally:
        backend.stop()

    typer.echo()
    total = passed + failed
    if failed:
        typer.secho(f"{failed}/{total} task(s) FAILED.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    else:
        typer.secho(f"All {total} task(s) PASSED.", fg=typer.colors.GREEN)


def _run_task(backend: SparkBackend, task_dir: Path, cfg, monitors: list, context: EngineContext) -> None:
    """Load transformations.py and execute the read → transform → write loop."""
    import importlib.util

    sys.path.insert(0, str(task_dir))
    try:
        fc_path = task_dir / "transformations.py"
        if not fc_path.exists():
            raise FileNotFoundError(f"Missing transformations.py at {fc_path}")

        spec = importlib.util.spec_from_file_location("transformations", str(fc_path))
        mod = importlib.util.module_from_spec(spec)  # type: ignore
        assert spec and spec.loader
        spec.loader.exec_module(mod)

        from ubunye.core.interfaces import Task
        task_cls = None
        for attr in mod.__dict__.values():
            if isinstance(attr, type) and issubclass(attr, Task) and attr is not Task:
                task_cls = attr
                break

        if not task_cls:
            raise RuntimeError(f"No Task subclass found in {fc_path}")

        task_obj = task_cls(config=cfg.model_dump(mode="json"))
        task_obj.setup()

        reg = Registry.from_entrypoints()
        sources = {}
        for name, icfg in cfg.CONFIG.inputs.items():
            reader_cls = reg.readers[icfg.format]
            sources[name] = reader_cls().read(icfg.model_dump(mode="json"), backend)

        outputs_map = task_obj.transform(sources)

        for name, ocfg in cfg.CONFIG.outputs.items():
            writer_cls = reg.writers[ocfg.format]
            df = outputs_map.get(name)
            if df is None:
                raise KeyError(f"Transform did not return output '{name}'")
            writer_cls().write(df, ocfg.model_dump(mode="json"), backend)

        duration = time.perf_counter()  # placeholder — actual duration tracked in caller
        for monitor in monitors:
            safe_call(monitor, "task_end", context=context, config=cfg.model_dump(mode="json"),
                      outputs=outputs_map, status="success", duration_sec=0.0)
    finally:
        if str(task_dir) in sys.path:
            sys.path.remove(str(task_dir))
