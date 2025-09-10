"""Ubunye CLI implemented with Typer.

Commands:
- init: scaffold a new usecase/package/task
- run: run a task by coordinates or config path
- plugins: list discovered plugins
- config: show/validate config
- plan: show resolved IO graph
- version: show version
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import typer

from ubunye.config import load_config
from ubunye.core.runtime import Engine, Registry
from ubunye.backends.spark_backend import SparkBackend

app = typer.Typer(add_completion=False, help="Ubunye Engine CLI")


def _task_path(usecase: str, package: str, task: str) -> Path:
    return Path(usecase) / package / task


@app.command()
def init(
    usecase: str = typer.Option(..., "-u", "--usecase"),
    package: str = typer.Option(..., "-p", "--package"),
    task: str = typer.Option(..., "-t", "--task"),
    overwrite: bool = typer.Option(False, help="Overwrite existing files"),
):
    """Scaffold a task folder with config.yaml and feature_class.py."""
    target = _task_path(usecase, package, task)
    cfg_file = target / "config.yaml"
    feat_file = target / "feature_class.py"
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
    input:
      format: hive
      db_name: your_db
      tbl_name: your_table
  transform:
    type: noop
  outputs:
    output:
      format: s3
      path: "s3a://your-bucket/{usecase}/{package}/{task}/{{{{ ds | default('2025-01-01') }}}}"
      mode: overwrite
      format: parquet
"""
        cfg_file.write_text(cfg, encoding="utf-8")
        typer.echo(f"created: {cfg_file}")

    if feat_file.exists() and not overwrite:
        typer.echo(f"exists: {feat_file}")
    else:
        class_name = "".join(s.capitalize() for s in task.replace("-", "_").split("_"))
        feat = f"""from typing import Dict
from ubunye.core.interfaces import Task

class {class_name}(Task):
    """User-defined Spark transformation task."""
    def setup(self) -> None:
        pass

    def transform(self, sources: Dict[str, object]) -> Dict[str, object]:
        # Replace with your logic. Echo input -> output.
        return {{"output": sources["input"]}}
"""
        feat_file.write_text(feat, encoding="utf-8")
        typer.echo(f"created: {feat_file}")

    typer.echo("✅ Scaffold complete")


@app.command()
def plugins():
    """List discovered Reader/Writer/Transform plugins."""
    reg = Registry.from_entrypoints()
    typer.echo(f"Readers:   {', '.join(sorted(reg.readers)) or '-'}")
    typer.echo(f"Writers:   {', '.join(sorted(reg.writers)) or '-'}")
    typer.echo(f"Transforms:{', '.join(sorted(reg.transforms)) or '-'}")


@app.command()
def config(
    action: str = typer.Argument(..., help="show | validate"),
    config_path: Path = typer.Argument(..., help="Path to config.yaml"),
    profile: Optional[str] = typer.Option(None, "--profile", "-P"),
):
    """Show or validate a config file (after Jinja rendering)."""
    try:
        cfg = load_config(str(config_path))
        if action == "show":
            typer.echo(cfg.model_dump_json(indent=2))
        elif action == "validate":
            # Accessing merged conf triggers validation
            _ = cfg.merged_spark_conf(profile)
            typer.echo("✅ Config valid")
        else:
            typer.echo("Unknown action. Use: show | validate")
    except Exception as e:
        typer.echo(f"❌ Config error: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def plan(
    usecase: Optional[str] = typer.Option(None, "-u", "--usecase"),
    package: Optional[str] = typer.Option(None, "-p", "--package"),
    task: Optional[str] = typer.Option(None, "-t", "--task"),
    config_path: Optional[Path] = typer.Option(None, "-c", "--config"),
):
    """Print the planned inputs → transform → outputs for a task."""
    if not config_path:
        if not (usecase and package and task):
            typer.echo("Provide either -c CONFIG or -u/-p/-t coordinates")
            raise typer.Exit(code=2)
        config_path = _task_path(usecase, package, task) / "config.yaml"

    cfg = load_config(str(config_path))
    inputs = cfg.CONFIG.get("inputs", {}) or {}
    outputs = cfg.CONFIG.get("outputs", {}) or {}
    tcfg = cfg.CONFIG.get("transform", {"type": "noop"}) or {"type": "noop"}

    typer.echo(f"Task: {config_path.parent}")
    typer.echo("Inputs:")
    for name, icfg in inputs.items():
        typer.echo(f"  - {name}: {icfg.get('format')}")
    typer.echo(f"Transform: {tcfg.get('type', 'noop')}")
    typer.echo("Outputs:")
    for name, ocfg in outputs.items():
        typer.echo(f"  - {name}: {ocfg.get('format')}")


@app.command()
def run(
    usecase: Optional[str] = typer.Option(None, "-u", "--usecase"),
    package: Optional[str] = typer.Option(None, "-p", "--package"),
    task: Optional[str] = typer.Option(None, "-t", "--task"),
    config_path: Optional[Path] = typer.Option(None, "-c", "--config"),
    profile: Optional[str] = typer.Option(None, "--profile", "-P", help="Engine profile"),
):
    """Run a single task by coordinates or config path."""
    if not config_path:
        if not (usecase and package and task):
            typer.echo("Provide either -c CONFIG or -u/-p/-t coordinates")
            raise typer.Exit(code=2)
        config_path = _task_path(usecase, package, task) / "config.yaml"

    task_dir = config_path.parent
    sys.path.insert(0, str(task_dir))  # allow importing feature_class from task dir

    cfg = load_config(str(config_path))
    spark_conf = cfg.merged_spark_conf(profile)
    backend = SparkBackend(app_name=f"ubunye:{task_dir}", conf=spark_conf)

    # Load user-defined Task from feature_class.py
    import importlib.util
    fc_path = task_dir / "feature_class.py"
    if not fc_path.exists():
        typer.echo(f"❌ Missing feature_class.py at {fc_path}", err=True)
        raise typer.Exit(code=1)
    spec = importlib.util.spec_from_file_location("feature_class", str(fc_path))
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # load module

    # find subclass of Task
    from ubunye.core.interfaces import Task
    task_cls = None
    for attr in mod.__dict__.values():
        if isinstance(attr, type) and issubclass(attr, Task) and attr is not Task:
            task_cls = attr
            break
    if not task_cls:
        typer.echo("❌ No Task subclass found in feature_class.py", err=True)
        raise typer.Exit(code=1)

    # Instantiate and run
    task_obj = task_cls(config=cfg.model_dump())
    task_obj.setup()

    # Engine expects the transform to be part of a plugin, but we allow user tasks:
    # We'll wrap user transform into a NoOp transform by passing its outputs directly.
    # Use the standard engine pipeline: read -> (noop) -> write, with user in between.
    from ubunye.core.runtime import Engine, Registry
    reg = Registry.from_entrypoints()
    eng = Engine(backend=backend, registry=reg)

    # Monkey-patch: run read, call user.transform, then write (reuse Engine internals idea).
    backend.start()
    try:
        inputs_cfg = cfg.CONFIG.get("inputs", {}) or {}
        sources = {}
        for name, icfg in inputs_cfg.items():
            rtype = icfg.get("format")
            reader_cls = reg.readers[rtype]
            sources[name] = reader_cls().read(icfg, backend)

        outputs_map = task_obj.transform(sources)

        outputs_cfg = cfg.CONFIG.get("outputs", {}) or {}
        for name, ocfg in outputs_cfg.items():
            wtype = ocfg.get("format")
            writer_cls = reg.writers[wtype]
            df = outputs_map.get(name)
            if df is None:
                raise KeyError(f"Transform did not return output '{name}'")
            writer_cls().write(df, ocfg, backend)
        typer.echo("✅ Run complete")
    finally:
        backend.stop()


@app.command()
def version():
    from ubunye import __version__
    typer.echo(f"Ubunye Engine v{__version__}")
