"""Engine runtime and plugin registry."""
from __future__ import annotations

import importlib.metadata as md
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from ubunye.backends.spark_backend import SparkBackend  # default backend
from ubunye.core.interfaces import Backend, Reader, Transform, Writer

# --- Optional telemetry (no-ops if libs not present / flag disabled) ---
from ubunye.telemetry.events import EventLogger
from ubunye.telemetry.monitors import load_monitors, safe_call
from ubunye.telemetry.otel import init_tracer, span
from ubunye.telemetry.prometheus import (
    observe_step,
    observe_task,
    start_prometheus_http_server,
)


@dataclass(frozen=True)
class EngineContext:
    """Lightweight context passed around for observability and debugging."""
    run_id: str
    profile: Optional[str] = None
    task_name: Optional[str] = None  # e.g., "fraud_detection/claims/claim_etl"


class Registry:
    """Discovers plugins via Python entry points."""

    def __init__(self) -> None:
        self.readers: Dict[str, type[Reader]] = {}
        self.writers: Dict[str, type[Writer]] = {}
        self.transforms: Dict[str, type[Transform]] = {}

    @staticmethod
    def _load(group: str) -> Dict[str, Any]:
        eps = md.entry_points()
        # Handle dict (Python <3.10) or SelectableGroups (Python >=3.10)
        group_eps = eps.get(group, []) if hasattr(eps, "get") else eps.select(group=group)
        return {ep.name: ep.load() for ep in group_eps}

    @classmethod
    def from_entrypoints(cls) -> "Registry":
        reg = cls()
        reg.readers = reg._load("ubunye.readers")
        reg.writers = reg._load("ubunye.writers")
        reg.transforms = reg._load("ubunye.transforms")
        return reg

    # Nice for tests or dynamic registration:
    def register_reader(self, name: str, cls_: type[Reader]) -> None:
        self.readers[name] = cls_

    def register_writer(self, name: str, cls_: type[Writer]) -> None:
        self.writers[name] = cls_

    def register_transform(self, name: str, cls_: type[Transform]) -> None:
        self.transforms[name] = cls_


# ---------------- Telemetry toggles & init helpers ----------------
_TELEMETRY_ENABLED = os.getenv("UBUNYE_TELEMETRY", "0") not in ("0", "", "false", "False")


def _maybe_init_telemetry() -> None:
    """Initialize optional telemetry backends if enabled via env flag."""
    if not _TELEMETRY_ENABLED:
        return
    try:
        init_tracer(service_name="ubunye")  # safe to call more than once
    except Exception:
        pass
    # Optional Prometheus endpoint for dev/local debugging
    prom_port = os.getenv("UBUNYE_PROM_PORT")
    if prom_port:
        try:
            start_prometheus_http_server(int(prom_port))
        except Exception:
            pass


class Engine:
    """
    Executes a task by reading inputs, applying one or more transforms, and writing outputs.

    Minimal required config structure:
      cfg["CONFIG"]["inputs"]   : mapping input_name -> reader cfg (must include "format")
      cfg["CONFIG"]["outputs"]  : mapping output_name -> writer cfg (must include "format")
      cfg["CONFIG"]["transform"]: EITHER a single transform dict with "type",
                                  OR a list of transform dicts to form a pipeline.

    Notes
    -----
    - This class is intentionally small; orchestration, telemetry, and retries
      can be layered around it without changing plugin contracts.
    """

    def __init__(
        self,
        backend: Optional[Backend] = None,
        registry: Optional[Registry] = None,
        context: Optional[EngineContext] = None,
    ) -> None:
        self.backend = backend or SparkBackend(app_name="ubunye")
        self.registry = registry or Registry.from_entrypoints()
        self.context = context or EngineContext(run_id=str(uuid.uuid4()))

    # ---------- public API ----------

    def run(self, cfg: dict, *, dry_run: bool = False) -> Optional[Dict[str, Any]]:
        """
        Run a task using the provided config mapping.

        Parameters
        ----------
        cfg : dict
            Parsed task configuration (already rendered/validated).
        dry_run : bool, default False
            If True, validates and returns without executing readers/writers.

        Returns
        -------
        Optional[Dict[str, Any]]
            Optionally returns the outputs map (None if dry_run).
        """
        inputs_cfg = cfg.get("CONFIG", {}).get("inputs", {}) or {}
        outputs_cfg = cfg.get("CONFIG", {}).get("outputs", {}) or {}
        transform_cfg = cfg.get("CONFIG", {}).get("transform", {"type": "noop"}) or {"type": "noop"}

        # Preflight validation
        self._validate_io_configs(inputs_cfg, outputs_cfg)
        transforms = self._normalize_transforms(transform_cfg)
        self._validate_transforms_exist(transforms)

        # Resolve context for telemetry
        task_name = (self.context.task_name
                     or cfg.get("TASK_NAME")
                     or "unknown_task")
        profile = (self.context.profile
                   or cfg.get("ENGINE", {}).get("active_profile")
                   or "default")

        # Init telemetry (no-op if disabled)
        _maybe_init_telemetry()
        logger = EventLogger(task=task_name, profile=profile, run_id=self.context.run_id)
        if _TELEMETRY_ENABLED:
            logger.task_start()
        monitors = load_monitors(cfg)
        for monitor in monitors:
            safe_call(monitor, "task_start", context=self.context, config=cfg)

        if dry_run:
            # Intentionally do not start backend on dry runs
            for monitor in monitors:
                safe_call(
                    monitor,
                    "task_end",
                    context=self.context,
                    config=cfg,
                    outputs=None,
                    status="success",
                    duration_sec=0.0,
                )
            if _TELEMETRY_ENABLED:
                logger.task_end(status="success", duration_sec=0.0)
                observe_task(task=task_name, profile=profile, status="success")
            return None

        # Execute
        self.backend.start()
        task_start = time.perf_counter()
        try:
            # -------- READ --------
            sources: Dict[str, Any] = {}
            for name in sorted(inputs_cfg):  # deterministic order
                icfg = inputs_cfg[name]
                rtype = icfg["format"]
                reader_cls = self.registry.readers.get(rtype)
                if not reader_cls:
                    raise KeyError(
                        f"Reader plugin '{rtype}' not found for input '{name}'. "
                        f"Installed: {sorted(self.registry.readers)}"
                    )
                step = f"Reader:{rtype}"
                t0 = time.perf_counter()
                if _TELEMETRY_ENABLED:
                    logger.step_start(step, extra={"input": name})
                try:
                    with span(step, {"task": task_name, "profile": profile}):
                        df = reader_cls().read(icfg, self.backend)
                    sources[name] = df
                    dur = time.perf_counter() - t0
                    if _TELEMETRY_ENABLED:
                        observe_step(task=task_name, profile=profile, step=step,
                                     status="success", duration_sec=dur)
                        logger.step_end(step, status="success", duration_sec=dur)
                except Exception as e:
                    dur = time.perf_counter() - t0
                    if _TELEMETRY_ENABLED:
                        observe_step(task=task_name, profile=profile, step=step,
                                     status="error", duration_sec=dur)
                        logger.step_end(step, status="error", duration_sec=dur, extra={"error": repr(e)})
                    raise

            # -------- TRANSFORM(S) --------
            outputs_map = dict(sources)
            for tcfg in transforms:
                ttype = tcfg["type"]
                tcls = self.registry.transforms[ttype]
                step = f"Transform:{ttype}"
                t0 = time.perf_counter()
                if _TELEMETRY_ENABLED:
                    logger.step_start(step)
                try:
                    with span(step, {"task": task_name, "profile": profile}):
                        outputs_map = tcls().apply(outputs_map, tcfg, self.backend)
                    if not isinstance(outputs_map, dict):
                        raise TypeError(
                            f"Transform '{ttype}' must return a dict[str, DataFrame], got {type(outputs_map)}"
                        )
                    dur = time.perf_counter() - t0
                    if _TELEMETRY_ENABLED:
                        observe_step(task=task_name, profile=profile, step=step,
                                     status="success", duration_sec=dur)
                        logger.step_end(step, status="success", duration_sec=dur)
                except Exception as e:
                    dur = time.perf_counter() - t0
                    if _TELEMETRY_ENABLED:
                        observe_step(task=task_name, profile=profile, step=step,
                                     status="error", duration_sec=dur)
                        logger.step_end(step, status="error", duration_sec=dur, extra={"error": repr(e)})
                    raise

            # -------- WRITE --------
            for name in sorted(outputs_cfg):  # deterministic order
                ocfg = outputs_cfg[name]
                wtype = ocfg["format"]
                writer_cls = self.registry.writers.get(wtype)
                if not writer_cls:
                    raise KeyError(
                        f"Writer plugin '{wtype}' not found for output '{name}'. "
                        f"Installed: {sorted(self.registry.writers)}"
                    )
                if name not in outputs_map:
                    raise KeyError(f"Transform did not return output '{name}' expected by config.")
                step = f"Writer:{wtype}"
                t0 = time.perf_counter()
                if _TELEMETRY_ENABLED:
                    logger.step_start(step, extra={"output": name})
                try:
                    with span(step, {"task": task_name, "profile": profile}):
                        writer_cls().write(outputs_map[name], ocfg, self.backend)
                    dur = time.perf_counter() - t0
                    if _TELEMETRY_ENABLED:
                        observe_step(task=task_name, profile=profile, step=step,
                                     status="success", duration_sec=dur)
                        logger.step_end(step, status="success", duration_sec=dur)
                except Exception as e:
                    dur = time.perf_counter() - t0
                    if _TELEMETRY_ENABLED:
                        observe_step(task=task_name, profile=profile, step=step,
                                     status="error", duration_sec=dur)
                        logger.step_end(step, status="error", duration_sec=dur, extra={"error": repr(e)})
                    raise

            # Success
            if _TELEMETRY_ENABLED:
                observe_task(task=task_name, profile=profile, status="success")
                logger.task_end(status="success")
            duration = time.perf_counter() - task_start
            for monitor in monitors:
                safe_call(
                    monitor,
                    "task_end",
                    context=self.context,
                    config=cfg,
                    outputs=outputs_map,
                    status="success",
                    duration_sec=duration,
                )
            return outputs_map

        except Exception:
            duration = time.perf_counter() - task_start
            for monitor in monitors:
                safe_call(
                    monitor,
                    "task_end",
                    context=self.context,
                    config=cfg,
                    outputs=None,
                    status="error",
                    duration_sec=duration,
                )
            if _TELEMETRY_ENABLED:
                observe_task(task=task_name, profile=profile, status="error")
                # task_end(status="error") already emitted inside failing step block,
                # but call again defensively if failure was outside step boundaries:
                logger.task_end(status="error")
            raise
        finally:
            self.backend.stop()

    # ---------- internal helpers ----------

    def _validate_io_configs(self, inputs: Dict[str, Any], outputs: Dict[str, Any]) -> None:
        missing_in = [k for k, v in inputs.items() if not v.get("format")]
        if missing_in:
            raise ValueError(f"Inputs missing 'format': {missing_in}")

        missing_out = [k for k, v in outputs.items() if not v.get("format")]
        if missing_out:
            raise ValueError(f"Outputs missing 'format': {missing_out}")

    def _normalize_transforms(self, tcfg: Any) -> List[Dict[str, Any]]:
        # Accept a single object {"type": "..."} or a list of such objects.
        if isinstance(tcfg, dict):
            return [tcfg]
        if isinstance(tcfg, list):
            return tcfg
        raise TypeError("CONFIG.transform must be a dict or a list of dicts")

    def _validate_transforms_exist(self, transforms: Iterable[Dict[str, Any]]) -> None:
        missing = []
        for t in transforms:
            ttype = t.get("type")
            if ttype not in self.registry.transforms:
                missing.append(ttype)
        if missing:
            raise KeyError(
                f"Transform plugin(s) not found: {missing}. "
                f"Installed: {sorted(self.registry.transforms)}"
            )
