"""The notebook API: a task run step by step, as one recorded run (ADR 016).

A Databricks job notebook is three steps::

    import ubunye

    with ubunye.notebook(task_dir, dt=dt) as run:
        sources = run.read()
        outputs = run.transform(sources)
        run.write(outputs)

Everything else comes from the job: the run-record location (job parameter
``ubunye_run_records``, else ``$UBUNYE_RUN_RECORDS``), who ran it and from which
commit, and the ``{{ env.X }}`` values (widgets, secrets). The ``with`` block is one
run: a failure anywhere in it, setup included, leaves a run record that names the
stage that failed. Each step prints one line.

The older form, without ``with``, works as it always did::

    ctx = ubunye.notebook("/Workspace/pipelines/claims/claim_etl", mode="PROD", dt="2026-01-01")
    sources = ctx.read()
    outputs = ctx.transform(sources)
    ctx.write(outputs)
"""

from __future__ import annotations

import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Set

from ubunye.adapters.spark.catalog import set_catalog_and_schema
from ubunye.api import BackendChoice, _detect_backend, _task_identity
from ubunye.config import load_config
from ubunye.config.hashing import config_hash
from ubunye.config.resolver import extract_env_references
from ubunye.config.schema import UbunyeConfig
from ubunye.config.variables import build_variables
from ubunye.core.hooks import Hook
from ubunye.core.runtime import Engine, EngineContext, Registry
from ubunye.core.task_runner import (
    _USER_TASK_TRANSFORM_KEY,
    _check_upstream,
    _load_task_class,
    _make_user_task_transform,
)
from ubunye.lineage import auto
from ubunye.telemetry.hooks import MonitorHook

logger = logging.getLogger(__name__)


def _get_dbutils() -> Any:
    """Return ``dbutils`` if running on Databricks, else ``None``."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark:
            return DBUtils(spark)
    except (ImportError, Exception):
        pass
    import builtins

    return getattr(builtins, "dbutils", None)


def _auto_resolve_env(
    env_refs: Set[str],
    explicit_env: Dict[str, str],
    secrets_scope: Optional[str],
    secrets_map: Optional[Dict[str, str]],
) -> Dict[str, str]:
    """Resolve env-var references from multiple sources (waterfall)."""
    resolved: Dict[str, str] = {}
    dbutils = _get_dbutils()

    for var_name in sorted(env_refs):
        # 1. Explicit override from caller
        if var_name in explicit_env:
            resolved[var_name] = explicit_env[var_name]
            continue

        # 2. Already in os.environ (e.g. Databricks job parameters)
        if var_name in os.environ:
            resolved[var_name] = os.environ[var_name]
            continue

        # 3. Databricks widgets (convention: widget name = lowercase var name)
        if dbutils is not None:
            widget_name = var_name.lower()
            try:
                val = dbutils.widgets.get(widget_name)
                if val:
                    resolved[var_name] = val
                    continue
            except Exception:
                pass

        # 4. Databricks secrets
        if dbutils is not None and secrets_scope:
            secret_key = (secrets_map or {}).get(var_name, var_name.lower())
            try:
                val = dbutils.secrets.get(scope=secrets_scope, key=secret_key)
                if val:
                    resolved[var_name] = val
                    continue
            except Exception:
                pass

        # 5. Not resolved — Jinja will handle via | default() or raise
        logger.debug("env var %s not resolved from any source", var_name)

    return resolved


class NotebookContext:
    """A task run step by step from a notebook; with ``with``, one recorded run."""

    def __init__(
        self,
        task_dir: str,
        *,
        mode: Optional[str] = None,
        dt: Optional[str] = None,
        dtf: Optional[str] = None,
        spark: Optional[Any] = None,
        backend: BackendChoice = None,
        variables: Optional[Mapping[str, Any]] = None,
        env: Optional[Dict[str, str]] = None,
        secrets_scope: Optional[str] = None,
        secrets_map: Optional[Dict[str, str]] = None,
        lineage: Optional[bool] = None,
        lineage_dir: Optional[str] = None,
        profile: Optional[str] = None,
        auto_env: bool = True,
        hooks: Optional[Iterable[Hook]] = None,
        lineage_backend: Optional[str] = None,
        run_records: Optional[str] = None,
        quiet: bool = False,
    ) -> None:
        self._task_path = Path(task_dir).resolve()
        self._task_dir_str = str(self._task_path)
        self._added_to_path = False
        self._original_env: Dict[str, Optional[str]] = {}
        self._resolved_env: Dict[str, str] = {}
        self._quiet = quiet
        self._open_run: Any = None  # the engine's open_run() while in a with block
        self._run_state: Dict[str, Any] = {}
        self._run_started = 0.0
        self._usecase_dir, self._task_identity = _task_identity(self._task_path)

        # --- Phase 0: the job's settings, the run record store, who runs this ---
        given = dict(variables or {})
        found = auto.settings(
            {
                "run_records": run_records,
                "dt": None if "dt" in given else dt,
                "mode": mode,
            }
        )
        self._mode = found.mode or "DEV"
        self._choice = auto.choose(
            self._usecase_dir,
            lineage=lineage,
            lineage_dir=lineage_dir,
            lineage_backend=lineage_backend,
            run_records=run_records,
            found=found,
        )
        self._recorder = auto.recorder_for(self._choice) if self._choice else None
        self._identity = auto.identity(found, self._task_path)
        self._run_id = str(uuid.uuid4())
        self._variables: Dict[str, Any] = {}
        self._cfg_dict: Dict[str, Any] = {}

        started = time.perf_counter()
        try:
            self._variables = build_variables(
                dt=auto.blank_as_none(dt) if "dt" in given else found.dt,
                dtf=auto.blank_as_none(dtf),
                mode=self._mode,
                extra=given,
            )
            self._set_up(
                spark=spark,
                backend=backend,
                env=env,
                secrets_scope=secrets_scope,
                secrets_map=secrets_map,
                profile=profile,
                auto_env=auto_env,
                hooks=hooks,
            )
        except Exception as exc:
            self._say(f"setup FAILED after {_secs(started)}: {_oneline(exc)}")
            if self._recorder is not None:
                auto.record_failure(
                    self._recorder,
                    self._new_context(),
                    self._cfg_dict,
                    exc,
                    auto.SETUP_STAGE,
                )
            self._say(f"run {self._run_id[:8]} FAILED at setup; run record: {self._where()}")
            self.close()
            raise

    def _set_up(
        self,
        *,
        spark: Optional[Any],
        backend: BackendChoice,
        env: Optional[Dict[str, str]],
        secrets_scope: Optional[str],
        secrets_map: Optional[Dict[str, str]],
        profile: Optional[str],
        auto_env: bool,
        hooks: Optional[Iterable[Hook]],
    ) -> None:
        mode = self._mode
        # --- Phase A: auto-resolve env vars ---
        if auto_env:
            raw_yaml = (self._task_path / "config.yaml").read_text(encoding="utf-8")
            env_refs = extract_env_references(raw_yaml)
            if env_refs:
                self._resolved_env = _auto_resolve_env(
                    env_refs, env or {}, secrets_scope, secrets_map
                )
                self._inject_env(self._resolved_env)

        # --- Phase B: load config ---
        self._cfg = load_config(str(self._task_path), variables=self._variables, profile=profile)
        self._cfg_dict = self._cfg.model_dump(mode="json")

        # --- Phase C: start backend ---
        spark_conf = self._cfg.merged_spark_conf(mode)
        parts = self._task_path.parts
        task_name = parts[-1] if len(parts) >= 1 else None
        package_name = parts[-2] if len(parts) >= 2 else None
        usecase_name = parts[-3] if len(parts) >= 3 else None
        app_parts = [p for p in (usecase_name, package_name, task_name) if p]
        app_name = f"ubunye:{'.'.join(app_parts)}" if app_parts else "ubunye"

        self._backend = _detect_backend(
            spark=spark, spark_conf=spark_conf, app_name=app_name, backend=backend
        )
        self._backend.start()
        set_catalog_and_schema(
            self._backend,
            catalog=self._cfg.resolved_catalog(mode),
            schema=self._cfg.resolved_schema(mode),
        )

        # --- Phase D: the run's identity; CONFIG.requires against the same store ---
        loaded_config_hash = config_hash(self._cfg_dict)  # before the transform swap
        self._context = self._new_context(config_hash=loaded_config_hash)
        requires = self._cfg_dict["CONFIG"].get("requires")
        if requires and self._recorder is not None:
            _check_upstream(self._task_path, self._context, requires)
        elif requires:
            logger.warning(
                "CONFIG.requires of %s was not checked: run records are off. Set the job "
                "parameter ubunye_run_records (or $UBUNYE_RUN_RECORDS) to the store the "
                "upstream task records into.",
                self._task_identity,
            )

        # --- Phase E: load Task class and register as transform ---
        self._added_to_path = self._task_dir_str not in sys.path
        if self._added_to_path:
            sys.path.insert(0, self._task_dir_str)

        transform_cfg = self._cfg.CONFIG.transform
        if transform_cfg.type is None or transform_cfg.type == "noop":
            task_cls = _load_task_class(self._task_path)
            task_obj = task_cls(config=self._cfg_dict)
            task_obj.variables = dict(self._variables)  # dt and --var for the Task
            task_obj.setup()

            reg = Registry.from_entrypoints()
            reg.register_transform(_USER_TASK_TRANSFORM_KEY, _make_user_task_transform(task_obj))
            self._cfg_dict = {
                **self._cfg_dict,
                "CONFIG": {
                    **self._cfg_dict["CONFIG"],
                    "transform": {"type": _USER_TASK_TRANSFORM_KEY},
                },
            }
        else:
            reg = Registry.from_entrypoints()

        # --- Phase F: build engine ---
        lineage_hooks = [MonitorHook(self._recorder)] if self._recorder is not None else []
        self._engine = Engine(
            backend=self._backend,
            registry=reg,
            context=self._context,
            extra_hooks=[*(hooks or []), *lineage_hooks],
            manage_backend=False,
        )

        self._last_sources: Optional[Dict[str, Any]] = None
        self._last_outputs: Optional[Dict[str, Any]] = None

    def _new_context(self, config_hash: Optional[str] = None) -> EngineContext:
        """The run's EngineContext: its id, task, batch, store and identity."""
        choice = self._choice
        fields: Dict[str, Any] = {
            "run_id": self._run_id,
            "profile": self._mode,
            "task_name": self._task_identity,
            "variables": dict(self._variables),
            "config_hash": config_hash,
            "task_dir": self._task_dir_str,
            "lineage_dir": choice.lineage_dir(self._usecase_dir) if choice else None,
            "run_records": self._recorder.shared_store() if self._recorder else None,
        }
        return EngineContext(**auto.with_identity(fields, self._identity))

    # ---------- one run: with ubunye.notebook(...) as run ----------

    def __enter__(self) -> "NotebookContext":
        """Open the run: read, transform, write and named steps inside are one run."""
        manager = self._engine.open_run(self._cfg_dict)
        self._run_state = manager.__enter__()
        self._open_run = manager
        self._run_started = time.perf_counter()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Close the run: a success, or a failure in the stage it was in."""
        manager, self._open_run = self._open_run, None
        try:
            if manager is None:
                return
            state = self._run_state
            if exc is not None and state.get("stage") is None:
                state["stage"] = auto.NOTEBOOK_STAGE  # code between the steps
            stage = state.get("stage")
            try:
                manager.__exit__(exc_type, exc, tb)
            finally:
                took = _secs(self._run_started)
                if exc is None:
                    self._say(
                        f"run {self._run_id[:8]} success in {took}; run record: {self._where()}"
                    )
                else:
                    self._say(
                        f"run {self._run_id[:8]} FAILED at {stage} after {took}; "
                        f"run record: {self._where()}"
                    )
        finally:
            self.close()

    @contextmanager
    def step(self, name: str) -> Iterator[None]:
        """A step of the notebook's own (``with run.step("score"):``): timed, printed,
        and the stage a failure in it is recorded under."""
        with self._report(name), self._engine.step(name, self._cfg_dict):
            yield

    # ---------- step-by-step API ----------

    def read(self) -> Dict[str, Any]:
        """Read all inputs defined in config.yaml. Returns ``{name: DataFrame}``."""
        with self._report("read") as note:
            sources = self._engine.read_inputs(self._cfg_dict)
            note(_frames(sources, "input"))
        self._last_sources = sources
        return sources

    def transform(self, sources: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Apply the task's transform logic.

        If *sources* is ``None``, uses the result of the last :meth:`read` call
        (or reads automatically if :meth:`read` was never called).
        """
        if sources is None:
            sources = self._last_sources
            if sources is None:
                sources = self.read()
        with self._report("transform") as note:
            outputs = self._engine.apply_transforms(sources, self._cfg_dict)
            note(_frames(outputs, "output"))
        self._last_outputs = outputs
        return outputs

    def write(self, outputs: Optional[Dict[str, Any]] = None) -> None:
        """Write outputs to configured sinks, and record the run.

        If *outputs* is ``None``, uses the result of the last :meth:`transform`
        call. Inside ``with`` the write is part of the one run; without it, every
        write leaves a run record when records are on, exactly as
        ``ubunye.run_task`` does.
        """
        if outputs is None:
            outputs = self._last_outputs
            if outputs is None:
                raise ValueError(
                    "No outputs to write. Call transform() first or pass outputs explicitly."
                )
        # The engine kept what the transform got (read or passed in), checked and
        # counted before it ran; a reconcile compares with that.
        with self._report("write") as note:
            self._engine.write_outputs(outputs, self._cfg_dict, as_run=True)
            written = (self._cfg_dict.get("CONFIG") or {}).get("outputs") or {}
            note(_frames({n: None for n in written}, "output"))

    def run(self) -> Dict[str, Any]:
        """Read, transform, and write in one call (same as ``ubunye.run_task``)."""
        sources = self.read()
        outputs = self.transform(sources)
        self.write(outputs)
        return outputs

    # ---------- introspection ----------

    @property
    def config(self) -> UbunyeConfig:
        """The fully resolved config object."""
        return self._cfg

    @property
    def config_dict(self) -> Dict[str, Any]:
        """The config as a plain dict."""
        return dict(self._cfg_dict)

    @property
    def env_vars(self) -> Dict[str, str]:
        """The env vars that were auto-resolved and injected."""
        return dict(self._resolved_env)

    @property
    def task_name(self) -> str:
        """The task directory name."""
        return self._task_path.name

    @property
    def run_id(self) -> str:
        """This run's id, as its run record has it."""
        return self._run_id

    @property
    def run_records(self) -> Optional[str]:
        """Where this run's record goes (``"delta main.ops.runs"``), or None when off."""
        return self._choice.describe() if self._choice else None

    @property
    def backend(self) -> Any:
        """The active backend."""
        return self._backend

    @property
    def spark(self) -> Any:
        """Shortcut to the active SparkSession."""
        return getattr(self._backend, "spark", None)

    # ---------- cleanup ----------

    def close(self) -> None:
        """Restore env vars and remove task dir from sys.path."""
        if self._added_to_path and self._task_dir_str in sys.path:
            sys.path.remove(self._task_dir_str)
        self._added_to_path = False
        for key, original in self._original_env.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original
        self._original_env = {}

    # ---------- internal ----------

    def _inject_env(self, resolved: Dict[str, str]) -> None:
        for key, val in resolved.items():
            self._original_env[key] = os.environ.get(key)
            os.environ[key] = val

    def _where(self) -> str:
        if self._choice is None:
            return "off (set the job parameter ubunye_run_records to keep one)"
        return self._choice.describe()

    def _say(self, line: str) -> None:
        """One summary line, on standard output (``quiet=True`` silences it)."""
        if not self._quiet:
            print(f"ubunye: {line}", flush=True)

    @contextmanager
    def _report(self, what: str) -> Iterator[Callable[[str], None]]:
        """Time a step and print its one line: what it did, or why it failed."""
        started = time.perf_counter()
        said: list = []
        try:
            yield said.append
        except Exception as exc:
            self._say(f"{what} FAILED after {_secs(started)}: {_oneline(exc)}")
            raise
        detail = " ".join(said)
        self._say(
            f"{what} {detail} in {_secs(started)}" if detail else f"{what} in {_secs(started)}"
        )


def _secs(started: float) -> str:
    return f"{time.perf_counter() - started:.2f}s"


def _oneline(exc: BaseException) -> str:
    """An exception as one short line: its type and the first line of its message."""
    text = str(exc).strip().splitlines()
    first = text[0] if text else ""
    if len(first) > 200:
        first = first[:197] + "..."
    return f"{type(exc).__name__}: {first}" if first else type(exc).__name__


def _frames(frames: Mapping[str, Any], kind: str) -> str:
    """``2 inputs (claims, policies)``: how many, and their names (no rows are counted)."""
    names = sorted(frames)
    noun = kind if len(names) == 1 else f"{kind}s"
    return f"{len(names)} {noun} ({', '.join(names)})" if names else f"0 {noun}"


def notebook(
    task_dir: str,
    *,
    mode: Optional[str] = None,
    dt: Optional[str] = None,
    dtf: Optional[str] = None,
    spark: Optional[Any] = None,
    backend: BackendChoice = None,
    variables: Optional[Mapping[str, Any]] = None,
    env: Optional[Dict[str, str]] = None,
    secrets_scope: Optional[str] = None,
    secrets_map: Optional[Dict[str, str]] = None,
    lineage: Optional[bool] = None,
    lineage_dir: Optional[str] = None,
    profile: Optional[str] = None,
    auto_env: bool = True,
    hooks: Optional[Iterable[Hook]] = None,
    lineage_backend: Optional[str] = None,
    run_records: Optional[str] = None,
    quiet: bool = False,
) -> NotebookContext:
    """A task run step by step from a notebook; use it with ``with`` for one recorded run.

    ::

        with ubunye.notebook(task_dir, dt=dt) as run:
            sources = run.read()
            outputs = run.transform(sources)
            run.write(outputs)

    All ``{{ env.VAR }}`` references in the task's ``config.yaml`` are
    auto-resolved from Databricks widgets and secrets: no manual
    ``os.environ`` setup needed.

    Run records are on when a location is configured: ``run_records=`` (a table
    name goes to the Delta store, a path to files), else the job parameter
    ``ubunye_run_records``, else ``$UBUNYE_RUN_RECORDS``. ``lineage=True`` with
    ``lineage_dir=`` still works as before; ``lineage=False`` turns records off.
    ``dt`` and ``mode`` given as ``""`` or None come from the job parameters
    ``dt`` / ``ubunye_mode`` (``$UBUNYE_DT`` / ``$UBUNYE_MODE``); mode defaults to
    ``DEV``. ``quiet=True`` stops the one-line-per-step summary.

    ``backend`` picks the engine as in :func:`ubunye.run_task`: a registered
    name (``"pandas"``) or an instance; by default the notebook's session.
    ``lineage_backend`` picks the run record store as in :func:`ubunye.run_task`.
    """
    return NotebookContext(
        task_dir,
        mode=mode,
        dt=dt,
        dtf=dtf,
        spark=spark,
        backend=backend,
        variables=variables,
        env=env,
        secrets_scope=secrets_scope,
        secrets_map=secrets_map,
        lineage=lineage,
        lineage_dir=lineage_dir,
        profile=profile,
        auto_env=auto_env,
        hooks=hooks,
        lineage_backend=lineage_backend,
        run_records=run_records,
        quiet=quiet,
    )
