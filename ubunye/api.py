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
        pipeline="ingestion",
        tasks=["claim_etl", "feature_engineering"],
        mode="nonprod",
        dt="202510",
    )
"""

from __future__ import annotations

import logging
import uuid
import warnings
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

from ubunye.adapters.spark.catalog import set_catalog_and_schema
from ubunye.config import load_config
from ubunye.config.variables import build_variables
from ubunye.core import backends
from ubunye.core.hooks import Hook
from ubunye.core.interfaces import Backend
from ubunye.core.runs import BatchFinished
from ubunye.core.runtime import EngineContext
from ubunye.core.task_runner import execute_user_task, finished_run
from ubunye.telemetry.hooks import MonitorHook

logger = logging.getLogger(__name__)


def _pipeline_name(
    pipeline: Optional[str], package: Optional[str], where: str, required: bool = True
) -> Optional[str]:
    """``pipeline``, or the old ``package`` with a DeprecationWarning (both: TypeError)."""
    if package is not None:
        if pipeline is not None:
            raise TypeError(
                f"{where}() got both pipeline= and package=; give pipeline= only "
                "(package= is its old name)."
            )
        warnings.warn(
            f"{where}(package=...) is renamed pipeline=; package= will be removed in 1.0.",
            DeprecationWarning,
            stacklevel=3,
        )
        pipeline = package
    if required and pipeline is None:
        raise TypeError(f"{where}() missing required argument: 'pipeline'")
    return pipeline


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
    exactly like a CLI run. Before 0.7.0 ``run_task`` and ``notebook`` keyed
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
        return attach(spark=spark, conf=backends.spark_conf_for(attach, spark_conf))
    return backends.resolve(None, app_name=app_name, conf=spark_conf or {})


def _build_extra_hooks(
    lineage_recorder: Optional[Any], hooks: Optional[Iterable[Hook]] = None
) -> List[Hook]:
    """The caller's hooks, then the lineage recorder as a hook, if present.

    Both are added to the engine's default hooks, never in place of them (F-101).
    """
    extra = list(hooks or [])
    return extra + [MonitorHook(lineage_recorder)] if lineage_recorder else extra


def run_task(
    task_dir: str,
    *,
    mode: str = "DEV",
    dt: Optional[str] = None,
    dtf: Optional[str] = None,
    spark: Optional[Any] = None,
    backend: BackendChoice = None,
    variables: Optional[Mapping[str, Any]] = None,
    lineage: bool = False,
    lineage_dir: str = ".ubunye/lineage",
    profile: Optional[str] = None,
    hooks: Optional[Iterable[Hook]] = None,
    rerun: bool = False,
    lineage_backend: Optional[str] = None,
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
    variables : dict, optional
        Extra template variables for the config (``{"region": "gauteng"}``
        makes ``{{ region }}`` available), as ``--var`` does on the command
        line. May set ``dt`` or ``dtf``; the same name with two values is refused.
    lineage : bool
        Record lineage for this run.
    lineage_dir : str
        Where lineage records go. For the filesystem store, a directory under the
        use case directory; for any other store, its location as given (a Delta
        table name or path).
    profile : str, optional
        Config profile for validation (passed to ``load_config``).
    hooks : iterable of Hook, optional
        Hooks to add to the engine's own (the ``ubunye.hooks`` entry point,
        ``CONFIG.monitors`` and the run record). They never replace them.
    rerun : bool
        Replace a batch (same ``dt`` and variables) that a finished run already
        appended. Without it, such a run is refused, since it would append the
        batch twice.
    lineage_backend : str, optional
        The run record store, by name (ADR 011): ``"filesystem"``, ``"delta"`` or
        an installed plugin. If *None*: ``$UBUNYE_LINEAGE_BACKEND``, else
        ``"filesystem"``.

    Returns
    -------
    Dict[str, Any]
        Mapping of output name → DataFrame.
    """
    task_path = Path(task_dir).resolve()
    usecase_dir, task_identity = _task_identity(task_path)
    variables = build_variables(dt=dt, dtf=dtf, mode=mode, extra=variables)

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
        from ubunye.lineage import stores
        from ubunye.lineage.recorder import LineageRecorder

        lineage_recorder = LineageRecorder(
            backend=stores.backend_name(lineage_backend),
            base_dir=stores.location(usecase_dir, lineage_dir, lineage_backend),
        )

    run_id = str(uuid.uuid4())
    context = EngineContext(
        run_id=run_id,
        profile=mode,
        task_name=task_identity,
        variables=variables,
        lineage_dir=str(usecase_dir / lineage_dir) if lineage else None,
        run_records=lineage_recorder.shared_store() if lineage_recorder else None,
        rerun=rerun,
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
            extra_hooks=_build_extra_hooks(lineage_recorder, hooks),
        )
    finally:
        backend.stop()


def run_pipeline(
    usecase_dir: str,
    usecase: str,
    pipeline: Optional[str] = None,
    tasks: Optional[List[str]] = None,
    *,
    mode: str = "DEV",
    dt: Optional[str] = None,
    dtf: Optional[str] = None,
    spark: Optional[Any] = None,
    backend: BackendChoice = None,
    variables: Optional[Mapping[str, Any]] = None,
    lineage: bool = False,
    lineage_dir: str = ".ubunye/lineage",
    profile: Optional[str] = None,
    hooks: Optional[Iterable[Hook]] = None,
    rerun: bool = False,
    lineage_backend: Optional[str] = None,
    resume: bool = False,
    package: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Run multiple tasks sequentially and return all outputs.

    Parameters
    ----------
    usecase_dir : str
        Root directory for use cases (e.g. ``"./pipelines"``).
    usecase : str
        Use case name.
    pipeline : str
        Pipeline name (the folder between the use case and the tasks).
    tasks : List[str]
        Task names to run in order.
    mode, dt, dtf, spark, backend, variables, lineage, lineage_dir, profile, hooks, rerun,
    lineage_backend
        Same as :func:`run_task`. One backend runs every task.
    resume : bool
        Finish a pipeline that stopped half way: a task that already finished this
        batch is skipped (with a warning, and is not in the result); the rest run.
        A task that appends knows it finished; one that only overwrites is known to
        have finished from its run records, so it is skipped only with ``lineage=True``
        (its latest run of the batch succeeded). Without ``resume`` a finished append
        is refused (:class:`~ubunye.core.runs.BatchFinished`).
    package : str, optional
        Old name of ``pipeline``. Still works, with a DeprecationWarning; removed in 1.0.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Mapping of task name → outputs map (without tasks skipped by ``resume``).
    """
    package = _pipeline_name(pipeline, package, "run_pipeline")
    assert package is not None  # narrowed for mypy
    if tasks is None:
        raise TypeError("run_pipeline() missing required argument: 'tasks'")
    base = Path(usecase_dir).resolve()
    variables = build_variables(dt=dt, dtf=dtf, mode=mode, extra=variables)

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
        from ubunye.lineage import stores
        from ubunye.lineage.recorder import LineageRecorder

        lineage_recorder = LineageRecorder(
            backend=stores.backend_name(lineage_backend),
            base_dir=stores.location(base, lineage_dir, lineage_backend),
        )

    chosen.start()
    set_catalog_and_schema(
        chosen,
        catalog=first_cfg.resolved_catalog(mode),
        schema=first_cfg.resolved_schema(mode),
    )
    extra_hooks = _build_extra_hooks(lineage_recorder, hooks)
    results: Dict[str, Dict[str, Any]] = {}
    try:
        for task in tasks:
            task_path = base / usecase / package / task
            cfg = configs[task]
            # Each task is its own run: one run id, one run record.
            context = EngineContext(
                run_id=str(uuid.uuid4()),
                profile=mode,
                task_name=f"{usecase}/{package}/{task}",
                variables=variables,
                lineage_dir=str(base / lineage_dir) if lineage else None,
                run_records=lineage_recorder.shared_store() if lineage_recorder else None,
                rerun=rerun,
            )
            done = finished_run(context) if resume else None
            if done is not None:
                logger.warning(
                    "Skipped %s (resume): run %s finished this batch.", task, done.run_id
                )
                continue
            try:
                results[task] = execute_user_task(
                    chosen,
                    task_path,
                    cfg,
                    context,
                    extra_hooks=extra_hooks,
                )
            except BatchFinished as exc:
                if not resume:
                    raise
                logger.warning("Skipped %s (resume): %s", task, exc)
    finally:
        chosen.stop()
    return results


def job_health(
    usecase_dir: Union[str, Path],
    *,
    usecase: Optional[str] = None,
    pipeline: Optional[str] = None,
    tasks: Optional[Iterable[str]] = None,
    max_age: Optional[Union[str, float]] = None,
    stuck_after: Union[str, float] = "6h",
    lineage_dir: str = ".ubunye/lineage",
    lineage_backend: Optional[str] = None,
    package: Optional[str] = None,
) -> List[Any]:
    """Job health per task, from run records (ADR 013); what ``ubunye runs health`` prints.

    Name the tasks (``tasks=["uc/pipeline/task"]``) or a use case (``usecase=``, with
    ``pipeline=`` to narrow it; ``package=`` is its old name, deprecated). Returns one
    :class:`~ubunye.lineage.health.TaskHealth` per task; ``.healthy`` is False when
    the task is failing, has a run likely dead (``running`` past ``stuck_after``) or,
    with ``max_age``, has not succeeded recently enough.
    """
    from ubunye.lineage import health, stores

    pipeline = _pipeline_name(pipeline, package, "job_health", required=False)
    store = stores.open_for(usecase_dir, lineage_dir, lineage_backend)
    names = list(tasks or [])
    if usecase:
        names += [
            t for t in health.tasks_for(store, usecase_dir, usecase, pipeline) if t not in names
        ]
    if not names:
        raise ValueError("Say which tasks: tasks=['usecase/pipeline/task'] or usecase='...'.")
    return health.check(store, names, max_age=max_age, stuck_after=stuck_after)


def wait_for_upstream(
    usecase_dir: Union[str, Path],
    task: str,
    *,
    dt: Optional[str] = None,
    variables: Optional[Mapping[str, Any]] = None,
    timeout: Union[str, float] = 0,
    poll: Union[str, float] = 60,
    lineage_dir: str = ".ubunye/lineage",
    lineage_backend: Optional[str] = None,
) -> Any:
    """The run of ``task`` that succeeded for this batch, waiting up to ``timeout``.

    The batch is ``dt`` and ``variables``; every one must match. Raises
    :class:`~ubunye.lineage.upstream.UpstreamNotReady` when there is none in time.
    What ``ubunye runs wait-for`` does; a task can ask the same in its config with
    ``CONFIG.requires``.
    """
    from ubunye.lineage import stores, upstream

    store = stores.open_for(usecase_dir, lineage_dir, lineage_backend)
    batch = build_variables(dt=dt, dtf=None, mode=None, extra=variables)
    req = upstream.Requirement(task=task, wait=timeout, poll=poll)
    return upstream.wait_for(store, req, batch)
