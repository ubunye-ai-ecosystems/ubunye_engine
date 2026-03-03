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

import sys
import time
import uuid
from pathlib import Path
from typing import Any, List, Optional

import typer

from ubunye.backends.spark_backend import SparkBackend
from ubunye.cli.lineage import lineage_app
from ubunye.cli.models import models_app
from ubunye.cli.test_cmd import test_app
from ubunye.config import load_config
from ubunye.core.runtime import EngineContext, Registry
from ubunye.telemetry.monitors import load_monitors, safe_call

app = typer.Typer(add_completion=False, help="Ubunye Engine CLI")
app.add_typer(lineage_app)
app.add_typer(models_app)
app.add_typer(test_app)


def _task_path(usecase_dir: Path, usecase: str, package: str, task: str) -> Path:
    return usecase_dir / usecase / package / task


@app.command()
def init(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", help="Specifies the directory path for the use case."),
    usecase: str = typer.Option(..., "-u", "--usecase", help="Selects the desired use case."),
    package: str = typer.Option(..., "-p", "--package", help="Selects a package from the specified use case."),
    task_list: List[str] = typer.Option(..., "-t", "--task-list", help="Specifies the task(s) to execute from the chosen package."),
    overwrite: bool = typer.Option(False, help="Overwrite existing files"),
):
    """Scaffold task folders with config.yaml and transformations.py."""
    for task in task_list:
        target = _task_path(usecase_dir, usecase, package, task)
        cfg_file = target / "config.yaml"
        feat_file = target / "transformations.py"
        target.mkdir(parents=True, exist_ok=True)

        if cfg_file.exists() and not overwrite:
            typer.echo(f"exists: {cfg_file}")
        else:
            cfg = f"""MODEL: "etl"
VERSION: "0.1.0"
ENGINE:
  spark_conf:
    spark.sql.shuffle.partitions: "50"

CONFIG:
  inputs:
    tx_data:
      format: unity
      db_name: raw_db
      tbl_name: {task}_input
  transform:
    type: noop
  outputs:
    output_features:
      format: s3
      path: "s3a://your-bucket/{usecase}/{package}/{task}/{{{{ dt | default('1970-01-01') }}}}"
      mode: overwrite
"""
            cfg_file.write_text(cfg, encoding="utf-8")
            typer.echo(f"created: {cfg_file}")

        if feat_file.exists() and not overwrite:
            typer.echo(f"exists: {feat_file}")
        else:
            class_name = "".join(s.capitalize() for s in task.replace("-", "_").split("_"))
            feat = f"""from typing import Dict, Any
from ubunye.core.interfaces import Task

class {class_name}(Task):
    \"\"\"User-defined Spark transformation task.\"\"\"
    def setup(self) -> None:
        pass

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        # Replace with your pure DataFrame transformations.
        df = sources.get("tx_data")
        return {{"output_features": df}}
"""
            feat_file.write_text(feat, encoding="utf-8")
            typer.echo(f"created: {feat_file}")

    typer.secho("[OK] Scaffold complete", fg=typer.colors.GREEN)


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
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", help="Root directory of pipelines."),
    usecase: str = typer.Option(..., "-u", "--usecase", help="Use case name."),
    package: str = typer.Option(..., "-p", "--package", help="Pipeline/package name."),
    task_list: List[str] = typer.Option(None, "-t", "--task-list", help="Task(s) to validate."),
    all_tasks: bool = typer.Option(False, "--all", help="Validate all tasks in the package."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile to validate against (e.g. dev, prod)."),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
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
            typer.secho(f"[ERROR] Package directory not found: {package_dir}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        tasks_to_check = [
            d.name for d in sorted(package_dir.iterdir())
            if d.is_dir() and (d / "config.yaml").exists()
        ]
        if not tasks_to_check:
            typer.secho(f"[WARN] No config.yaml files found under {package_dir}", fg=typer.colors.YELLOW)
            return

    if not tasks_to_check:
        typer.secho("[ERROR] Specify -t/--task-list or use --all", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    variables = {"dt": data_timestamp}
    failed = 0

    for task in tasks_to_check:
        task_dir = _task_path(usecase_dir, usecase, package, task)
        try:
            load_config(str(task_dir), variables=variables, profile=profile)
            typer.secho(f"  [OK]   {task}", fg=typer.colors.GREEN)
        except (ValueError, FileNotFoundError) as e:
            typer.secho(f"  [FAIL] {task}", fg=typer.colors.RED)
            # Indent error details for readability
            for line in str(e).splitlines():
                typer.echo(f"         {line}")
            failed += 1

    typer.echo()
    if failed:
        typer.secho(f"{failed}/{len(tasks_to_check)} task(s) failed validation.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    else:
        typer.secho(f"All {len(tasks_to_check)} task(s) passed validation.", fg=typer.colors.GREEN)


@app.command()
def plan(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir"),
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task_list: List[str] = typer.Option(..., "-t", "--task-list"),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    data_timestamp_format: Optional[str] = typer.Option(None, "-dtf", "--data-timestamp-format"),
    mode: str = typer.Option("DEV", "-m", "--mode"),
):
    """Print the planned inputs → transform → outputs for task(s)."""
    variables = {"dt": data_timestamp, "dtf": data_timestamp_format, "mode": mode}
    for task in task_list:
        config_path = _task_path(usecase_dir, usecase, package, task) / "config.yaml"
        cfg = load_config(str(config_path), variables)

        typer.echo(f"--- Task: {task} ---")
        typer.echo("Inputs (Extract):")
        for name, icfg in cfg.CONFIG.inputs.items():
            typer.echo(f"  - {name}: {icfg.format.value}")
        typer.echo(f"Transform (transformations.py): {cfg.CONFIG.transform.type}")
        typer.echo("Outputs (Load):")
        for name, ocfg in cfg.CONFIG.outputs.items():
            typer.echo(f"  - {name}: {ocfg.format.value}")
        typer.echo()


def _run_single_task(
    backend: SparkBackend,
    task_dir: Path,
    cfg: Any,
    monitors: list,
    context: EngineContext
):
    sys.path.insert(0, str(task_dir))

    # Load user-defined Task from transformations.py
    import importlib.util
    fc_path = task_dir / "transformations.py"
    if not fc_path.exists():
        typer.secho(f"[ERROR] Missing transformations.py at {fc_path}. Transforms must live here.", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

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
        typer.secho(f"[ERROR] No Task subclass found in {fc_path}. Define a class that subclasses Task in transformations.py.", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    task_obj = task_cls(config=cfg.model_dump(mode="json"))
    task_obj.setup()

    from ubunye.core.runtime import Registry
    reg = Registry.from_entrypoints()

    task_start = time.perf_counter()
    outputs_map = {}
    try:
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

        duration = time.perf_counter() - task_start
        for monitor in monitors:
            safe_call(monitor, "task_end", context=context, config=cfg.model_dump(mode="json"),
                      outputs=outputs_map, status="success", duration_sec=duration)
            typer.secho(f"[OK] Run complete for {task_dir.name}", fg=typer.colors.GREEN)
    except Exception as e:
        duration = time.perf_counter() - task_start
        for monitor in monitors:
            safe_call(monitor, "task_end", context=context, config=cfg.model_dump(mode="json"),
                      outputs=outputs_map, status="error", duration_sec=duration)
        typer.secho(f"[ERROR] Run failed for {task_dir.name}: {e}", fg=typer.colors.RED, err=True)
        raise
    finally:
        if str(task_dir) in sys.path:
            sys.path.remove(str(task_dir))


@app.command()
def run(
    usecase_dir: Path = typer.Option(..., "-d", "--usecase-dir", help="Specifies the directory path for the use case."),
    usecase: str = typer.Option(..., "-u", "--usecase", help="Selects the desired use case."),
    package: str = typer.Option(..., "-p", "--package", help="Selects a package from the specified use case."),
    task_list: List[str] = typer.Option(..., "-t", "--task-list", help="Specifies the task(s) to execute from the chosen package."),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp", help="Provides a data timestamp in the specified format."),
    data_timestamp_format: Optional[str] = typer.Option(None, "-dtf", "--data-timestamp-format", help="Specifies the format for the data timestamp."),
    mode: str = typer.Option("DEV", "-m", "--mode", help="Selects the run mode (DEV/PROD)."),
    deploy_mode: str = typer.Option("client", "--deploy-mode", help="Specifies the deployment mode (cluster/client). Defaults to client."),
    lineage: bool = typer.Option(False, "--lineage", help="Record lineage for this run (stored as JSON under --lineage-dir)."),
    lineage_dir: str = typer.Option(".ubunye/lineage", "--lineage-dir", help="Root directory for lineage records."),
):
    """Run one or more tasks within a package sequentially."""
    variables = {"dt": data_timestamp, "dtf": data_timestamp_format, "mode": mode}

    # Load and validate all configs before starting Spark — fails fast on bad configs
    configs = {}
    for task in task_list:
        task_dir = _task_path(usecase_dir, usecase, package, task)
        if not (task_dir / "config.yaml").exists():
            typer.secho(f"[ERROR] Missing config at {task_dir / 'config.yaml'}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        try:
            configs[task] = load_config(str(task_dir), variables)
        except (ValueError, FileNotFoundError) as e:
            typer.secho(f"[ERROR] Config validation failed for '{task}':\n{e}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)

    first_cfg = configs[task_list[0]]
    spark_conf = first_cfg.merged_spark_conf(mode)
    spark_conf["spark.submit.deployMode"] = deploy_mode

    run_id = str(uuid.uuid4())
    backend = SparkBackend(app_name=f"ubunye:{package}", conf=spark_conf)

    # Build a lineage recorder if --lineage was requested
    lineage_recorder = None
    if lineage:
        from ubunye.lineage.recorder import LineageRecorder
        lineage_recorder = LineageRecorder(
            store="filesystem",
            base_dir=str(usecase_dir / lineage_dir),
        )

    backend.start()
    try:
        for task in task_list:
            typer.echo(f"Starting task: {task} (Mode: {mode}, Deploy: {deploy_mode})")
            cfg = configs[task]
            task_dir = _task_path(usecase_dir, usecase, package, task)
            # Include full path so LineageRecorder can parse usecase/package/task
            context = EngineContext(run_id=run_id, profile=mode, task_name=f"{usecase}/{package}/{task}")
            monitors = load_monitors(cfg.model_dump(mode="json"))
            if lineage_recorder is not None:
                monitors = [lineage_recorder] + monitors

            for monitor in monitors:
                safe_call(monitor, "task_start", context=context, config=cfg.model_dump(mode="json"))

            _run_single_task(backend, task_dir, cfg, monitors, context)

    finally:
        backend.stop()


@app.command()
def version():
    from ubunye import __version__
    typer.echo(f"Ubunye Engine v{__version__}")
