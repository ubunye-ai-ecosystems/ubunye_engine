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
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from ubunye.adapters.spark.catalog import set_catalog_and_schema
from ubunye.config import load_config
from ubunye.core import backends
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


def _task_identity(task_path: Path) -> Tuple[Path, str]:
    """``(usecase_dir, "usecase/package/task")`` for a task folder.

    The one identity every entry point records lineage under, so a run started
    from Python is found by ``ubunye lineage list -d <usecase_dir> -u -p -t``
    exactly like a CLI run. Before 0.6.0 ``run_task`` and ``notebook`` keyed
    lineage by the folder name alone, under the package folder, so their records
    were invisible to the CLI.
    """
    parts = task_path.parts
    if len(parts) >= 4:
        return task_path.parents[2], "/".join(parts[-3:])
    return task_path.parent, task_path.name


#: A backend as the API accepts it: an instance, a registered name, or ``None``.
BackendChoice = Union[Backend, str, None]


def _detect_backend(
    spark: Optional[Any] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    app_name: str = "ubunye",
    backend: BackendChoice = None,
) -> Backend:
    """Pick the backend for a run (ADR 003).

    Resolution order:
    1. *backend*: an instance is used as given; a name (``"pandas"``) is looked
       up in the ``ubunye.backends`` registry.
    2. *spark*: an explicit SparkSession, attached to (not owned).
    3. The platform's session: an active SparkSession (Databricks) is attached to.
    4. The default backend (``spark``), created with the given conf.

    The conf is always passed on. Before 0.4.0 it was computed at the call site
    and then dropped here whenever a session already existed.
    """
    if backend is not None and spark is not None:
        raise ValueError("Pass backend= or spark=, not both: spark= already picks the backend.")
    if isinstance(backend, Backend):
        return backend
    if backend is not None and not isinstance(backend, str):
        raise TypeError(
            f"backend= takes a Backend or a registered name, not {type(backend).__name__}."
        )
    if backend:
        return backends.create(backend, app_name=app_name, conf=spark_conf or {})
    if spark is not None:
        # Attaching to a given session is the Databricks backend's own constructor.
        attach: Any = backends.load_class("databricks")
        return attach(spark=spark, conf=spark_conf or {})
    return backends.resolve(None, app_name=app_name, conf=spark_conf or {})


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
    backend: BackendChoice = None,
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
    backend : Backend or str, optional
        The backend to run on: a registered name (``"pandas"`` for a run with no
        Spark and no Java, ``"spark"``, or any installed plugin) or an instance.
        If *None*: the ``spark`` session if given, else the platform's active
        session, else a new Spark session.
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
    usecase_dir, task_identity = _task_identity(task_path)
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

    backend = _detect_backend(
        spark=spark, spark_conf=spark_conf, app_name=app_name, backend=backend
    )

    lineage_recorder = None
    if lineage:
        from ubunye.lineage.recorder import LineageRecorder

        lineage_recorder = LineageRecorder(
            store="filesystem",
            base_dir=str(usecase_dir / lineage_dir),
        )

    run_id = str(uuid.uuid4())
    context = EngineContext(
        run_id=run_id, profile=mode, task_name=task_identity, variables=variables
    )

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
    backend: BackendChoice = None,
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
    mode, dt, dtf, spark, backend, lineage, lineage_dir, profile, hooks
        Same as :func:`run_task`. One backend runs every task.

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

    chosen = _detect_backend(spark=spark, spark_conf=spark_conf, app_name=app_name, backend=backend)

    lineage_recorder = None
    if lineage:
        from ubunye.lineage.recorder import LineageRecorder

        lineage_recorder = LineageRecorder(
            store="filesystem",
            base_dir=str(base / lineage_dir),
        )

    chosen.start()
    set_catalog_and_schema(
        chosen,
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
                variables=variables,
            )
            results[task] = execute_user_task(
                chosen,
                task_path,
                cfg,
                context,
                hooks=hooks,
                extra_hooks=extra_hooks,
            )
        return results
    finally:
        chosen.stop()
