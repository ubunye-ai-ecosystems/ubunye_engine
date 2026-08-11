"""Interactive notebook API for step-by-step Ubunye task execution.

Usage (Databricks notebook)::

    import ubunye

    ctx = ubunye.notebook("/Workspace/pipelines/claims/claim_etl",
                          mode="PROD", dt="2026-01-01")

    sources = ctx.read()
    sources["raw_claims"].display()

    outputs = ctx.transform(sources)
    outputs["bronze_claims"].show()

    ctx.write(outputs)
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set

from ubunye.adapters.spark.catalog import set_catalog_and_schema
from ubunye.api import _detect_backend
from ubunye.config import load_config
from ubunye.config.resolver import extract_env_references
from ubunye.config.schema import UbunyeConfig
from ubunye.core.hooks import Hook
from ubunye.core.runtime import Engine, EngineContext, Registry
from ubunye.core.task_runner import (
    _USER_TASK_TRANSFORM_KEY,
    _load_task_class,
    _make_user_task_transform,
)

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
    """Stateful notebook session for step-by-step Ubunye task execution."""

    def __init__(
        self,
        task_dir: str,
        *,
        mode: str = "DEV",
        dt: Optional[str] = None,
        dtf: Optional[str] = None,
        spark: Optional[Any] = None,
        env: Optional[Dict[str, str]] = None,
        secrets_scope: Optional[str] = None,
        secrets_map: Optional[Dict[str, str]] = None,
        lineage: bool = False,
        lineage_dir: str = ".ubunye/lineage",
        profile: Optional[str] = None,
        auto_env: bool = True,
        hooks: Optional[Iterable[Hook]] = None,
    ) -> None:
        self._task_path = Path(task_dir).resolve()
        self._mode = mode
        self._original_env: Dict[str, Optional[str]] = {}
        self._resolved_env: Dict[str, str] = {}

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
        variables = {"dt": dt, "dtf": dtf, "mode": mode}
        self._cfg = load_config(str(self._task_path), variables=variables, profile=profile)

        # --- Phase C: start backend ---
        spark_conf = self._cfg.merged_spark_conf(mode)
        parts = self._task_path.parts
        task_name = parts[-1] if len(parts) >= 1 else None
        package_name = parts[-2] if len(parts) >= 2 else None
        usecase_name = parts[-3] if len(parts) >= 3 else None
        app_parts = [p for p in (usecase_name, package_name, task_name) if p]
        app_name = f"ubunye:{'.'.join(app_parts)}" if app_parts else "ubunye"

        self._backend = _detect_backend(spark=spark, spark_conf=spark_conf, app_name=app_name)
        self._backend.start()
        set_catalog_and_schema(
            self._backend,
            catalog=self._cfg.resolved_catalog(mode),
            schema=self._cfg.resolved_schema(mode),
        )

        # --- Phase D: load Task class and register as transform ---
        self._cfg_dict = self._cfg.model_dump(mode="json")
        self._task_dir_str = str(self._task_path)
        self._added_to_path = self._task_dir_str not in sys.path
        if self._added_to_path:
            sys.path.insert(0, self._task_dir_str)

        transform_cfg = self._cfg.CONFIG.transform
        if transform_cfg.type is None or transform_cfg.type == "noop":
            task_cls = _load_task_class(self._task_path)
            task_obj = task_cls(config=self._cfg_dict)
            task_obj.setup()
            self._task_obj = task_obj

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
            self._task_obj = None
            reg = Registry.from_entrypoints()

        # --- Phase E: build engine ---
        run_id = str(uuid.uuid4())
        lineage_hooks = self._build_lineage_hooks(lineage, lineage_dir)
        self._context = EngineContext(run_id=run_id, profile=mode, task_name=self._task_path.name)
        self._engine = Engine(
            backend=self._backend,
            registry=reg,
            context=self._context,
            hooks=list(hooks) if hooks is not None else None,
            extra_hooks=lineage_hooks,
            manage_backend=False,
        )

        self._last_sources: Optional[Dict[str, Any]] = None
        self._last_outputs: Optional[Dict[str, Any]] = None

    # ---------- step-by-step API ----------

    def read(self) -> Dict[str, Any]:
        """Read all inputs defined in config.yaml. Returns ``{name: DataFrame}``."""
        sources = self._engine.read_inputs(self._cfg_dict)
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
        outputs = self._engine.apply_transforms(sources, self._cfg_dict)
        self._last_outputs = outputs
        return outputs

    def write(self, outputs: Optional[Dict[str, Any]] = None) -> None:
        """Write outputs to configured sinks.

        If *outputs* is ``None``, uses the result of the last :meth:`transform`
        call.
        """
        if outputs is None:
            outputs = self._last_outputs
            if outputs is None:
                raise ValueError(
                    "No outputs to write. Call transform() first or pass outputs explicitly."
                )
        self._engine.write_outputs(outputs, self._cfg_dict)

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
        for key, original in self._original_env.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original

    # ---------- internal ----------

    def _inject_env(self, resolved: Dict[str, str]) -> None:
        for key, val in resolved.items():
            self._original_env[key] = os.environ.get(key)
            os.environ[key] = val

    def _build_lineage_hooks(self, lineage: bool, lineage_dir: str) -> list:
        if not lineage:
            return []
        from ubunye.lineage.recorder import LineageRecorder
        from ubunye.telemetry.hooks import MonitorHook

        recorder = LineageRecorder(
            store="filesystem",
            base_dir=str(self._task_path.parent / lineage_dir),
        )
        return [MonitorHook(recorder)]


def notebook(
    task_dir: str,
    *,
    mode: str = "DEV",
    dt: Optional[str] = None,
    dtf: Optional[str] = None,
    spark: Optional[Any] = None,
    env: Optional[Dict[str, str]] = None,
    secrets_scope: Optional[str] = None,
    secrets_map: Optional[Dict[str, str]] = None,
    lineage: bool = False,
    lineage_dir: str = ".ubunye/lineage",
    profile: Optional[str] = None,
    auto_env: bool = True,
    hooks: Optional[Iterable[Hook]] = None,
) -> NotebookContext:
    """Create an interactive notebook context for step-by-step task execution.

    All ``{{ env.VAR }}`` references in the task's ``config.yaml`` are
    auto-resolved from Databricks widgets and secrets — no manual
    ``os.environ`` setup needed.
    """
    return NotebookContext(
        task_dir,
        mode=mode,
        dt=dt,
        dtf=dtf,
        spark=spark,
        env=env,
        secrets_scope=secrets_scope,
        secrets_map=secrets_map,
        lineage=lineage,
        lineage_dir=lineage_dir,
        profile=profile,
        auto_env=auto_env,
        hooks=hooks,
    )
