"""Public Python API for running Ubunye tasks without the CLI.

Primary use case: Databricks notebooks and jobs where a SparkSession already
exists and subprocess-based CLI execution is wasteful or awkward.

Usage
-----
    import ubunye

    # Run a single task
    outputs = ubunye.run_task(
        task_dir="pipelines/fraud_detection/ingestion/claim_etl",
        mode="nonprod",
        dt="202510",
    )

    # Run multiple tasks sequentially
    results = ubunye.run_pipeline(
        usecase_dir="pipelines",
        usecase="fraud_detection",
        package="ingestion",
        tasks=["claim_etl", "feature_engineering"],
        mode="nonprod",
        dt="202510",
    )
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ubunye.config import load_config
from ubunye.core.catalog import set_catalog_and_schema
from ubunye.core.hooks import Hook
from ubunye.core.interfaces import Backend
from ubunye.core.runtime import EngineContext
from ubunye.core.task_runner import execute_user_task
from ubunye.telemetry.hooks import MonitorHook


def _make_app_name(
    usecase: Optional[str] = None, package: Optional[str] = None, task: Optional[str] = None
) -> str:
    """Build a descriptive Spark app name: ``ubunye:<usecase>.<package>.<task>``."""
    parts = [p for p in (usecase, package, task) if p]
    return f"ubunye:{'.'.join(parts)}" if parts else "ubunye"


def _detect_backend(
    spark: Optional[Any] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    app_name: str = "ubunye",
) -> Backend:
    """Pick the right backend: reuse an active session or create a new one.

    Resolution order:
    1. If *spark* is an explicit SparkSession, wrap it in DatabricksBackend.
    2. If an active SparkSession exists (Databricks), use DatabricksBackend.
    3. Otherwise fall back to SparkBackend with the given conf.
    """
    if spark is not None:
        from ubunye.backends.databricks_backend import DatabricksBackend

        return DatabricksBackend(spark=spark)

    # Probe for an active session without importing pyspark at module level
    try:
        from pyspark.sql import SparkSession  # type: ignore

        active = SparkSession.getActiveSession()
        if active is not None:
            from ubunye.backends.databricks_backend import DatabricksBackend

            return DatabricksBackend(spark=active)
    except ImportError:
        pass

    from ubunye.backends.spark_backend import SparkBackend

    return SparkBackend(app_name=app_name, conf=spark_conf or {})


def _build_extra_hooks(lineage_recorder: Optional[Any]) -> List[Hook]:
    """Wrap the optional lineage recorder as a hook, if present."""
    if lineage_recorder is None:
        return []
    return [MonitorHook(lineage_recorder)]


def run_task(
    task_dir: str,
    *,
    mode: str = "DEV",
    dt: Optional[str] = None,
    dtf: Optional[str] = None,
    spark: Optional[Any] = None,
    lineage: bool = False,
    lineage_dir: str = ".ubunye/lineage",
    profile: Optional[str] = None,
    hooks: Optional[Iterable[Hook]] = None,
) -> Dict[str, Any]:
    """Run a single Ubunye task and return the outputs map.

    Parameters
    ----------
    task_dir : str
        Path to the task directory containing ``config.yaml`` and
        ``transformations.py``.
    mode : str
        Run mode, used for Spark profile merging. Default ``"DEV"``.
    dt : str, optional
        Data timestamp, injected as ``{{ dt }}`` in Jinja templates.
    dtf : str, optional
        Data timestamp format, injected as ``{{ dtf }}``.
    spark : SparkSession, optional
        Explicit SparkSession to reuse. If *None*, auto-detects an active
        session (Databricks) or creates a new one.
    lineage : bool
        Record lineage for this run.
    lineage_dir : str
        Root directory for lineage records.
    profile : str, optional
        Config profile for validation (passed to ``load_config``).
    hooks : iterable of Hook, optional
        Replace the engine's default hooks entirely. Rarely needed; prefer
        the ``ubunye.hooks`` entry point for always-on hooks.

    Returns
    -------
    Dict[str, Any]
        Mapping of output name → DataFrame.
    """
    task_path = Path(task_dir).resolve()
    variables = {"dt": dt, "dtf": dtf, "mode": mode}

    cfg = load_config(str(task_path), variables=variables, profile=profile)
    spark_conf = cfg.merged_spark_conf(mode)

    # Derive usecase/package/task from path for app naming
    # Convention: <usecase_dir>/<usecase>/<package>/<task>
    parts = task_path.parts
    task_name = parts[-1] if len(parts) >= 1 else None
    package_name = parts[-2] if len(parts) >= 2 else None
    usecase_name = parts[-3] if len(parts) >= 3 else None
    app_name = _make_app_name(usecase_name, package_name, task_name)

    backend = _detect_backend(spark=spark, spark_conf=spark_conf, app_name=app_name)

    lineage_recorder = None
    if lineage:
        from ubunye.lineage.recorder import LineageRecorder

        lineage_recorder = LineageRecorder(
            store="filesystem",
            base_dir=str(task_path.parent / lineage_dir),
        )

    run_id = str(uuid.uuid4())
    context = EngineContext(run_id=run_id, profile=mode, task_name=task_path.name)

    backend.start()
    set_catalog_and_schema(
        backend,
        catalog=cfg.resolved_catalog(mode),
        schema=cfg.resolved_schema(mode),
    )
    try:
        return execute_user_task(
            backend,
            task_path,
            cfg,
            context,
            hooks=hooks,
            extra_hooks=_build_extra_hooks(lineage_recorder),
        )
    finally:
        backend.stop()


def run_pipeline(
    usecase_dir: str,
    usecase: str,
    package: str,
    tasks: List[str],
    *,
    mode: str = "DEV",
    dt: Optional[str] = None,
    dtf: Optional[str] = None,
    spark: Optional[Any] = None,
    lineage: bool = False,
    lineage_dir: str = ".ubunye/lineage",
    profile: Optional[str] = None,
    hooks: Optional[Iterable[Hook]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Run multiple tasks sequentially and return all outputs.

    Parameters
    ----------
    usecase_dir : str
        Root directory for use cases (e.g. ``"./pipelines"``).
    usecase : str
        Use case name.
    package : str
        Package/pipeline name.
    tasks : List[str]
        Task names to run in order.
    mode, dt, dtf, spark, lineage, lineage_dir, profile, hooks
        Same as :func:`run_task`.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Mapping of task name → outputs map.
    """
    base = Path(usecase_dir).resolve()
    variables = {"dt": dt, "dtf": dtf, "mode": mode}
    run_id = str(uuid.uuid4())

    # Validate all configs before starting backend
    configs = {}
    for task in tasks:
        task_path = base / usecase / package / task
        configs[task] = load_config(str(task_path), variables=variables, profile=profile)

    # Use Spark conf from first task
    first_cfg = configs[tasks[0]]
    spark_conf = first_cfg.merged_spark_conf(mode)
    app_name = _make_app_name(usecase, package, tasks[0])

    backend = _detect_backend(spark=spark, spark_conf=spark_conf, app_name=app_name)

    lineage_recorder = None
    if lineage:
        from ubunye.lineage.recorder import LineageRecorder

        lineage_recorder = LineageRecorder(
            store="filesystem",
            base_dir=str(base / lineage_dir),
        )

    backend.start()
    set_catalog_and_schema(
        backend,
        catalog=first_cfg.resolved_catalog(mode),
        schema=first_cfg.resolved_schema(mode),
    )
    extra_hooks = _build_extra_hooks(lineage_recorder)
    results: Dict[str, Dict[str, Any]] = {}
    try:
        for task in tasks:
            task_path = base / usecase / package / task
            cfg = configs[task]
            context = EngineContext(
                run_id=run_id,
                profile=mode,
                task_name=f"{usecase}/{package}/{task}",
            )
            results[task] = execute_user_task(
                backend,
                task_path,
                cfg,
                context,
                hooks=hooks,
                extra_hooks=extra_hooks,
            )
        return results
    finally:
        backend.stop()
