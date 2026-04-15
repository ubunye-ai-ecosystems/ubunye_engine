"""Engine runtime and plugin registry."""

from __future__ import annotations

import importlib.metadata as md
import os
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from ubunye.backends.spark_backend import SparkBackend  # default backend
from ubunye.core.hooks import Hook, HookChain
from ubunye.core.interfaces import Backend, Reader, Transform, Writer


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


# ---------------- Default hook assembly ----------------
_TELEMETRY_ENABLED = os.getenv("UBUNYE_TELEMETRY", "0") not in ("0", "", "false", "False")


def _discover_hooks() -> List[type[Hook]]:
    """Load Hook classes from the ``ubunye.hooks`` entry point group."""
    eps = md.entry_points()
    group_eps = eps.get("ubunye.hooks", []) if hasattr(eps, "get") else eps.select(group="ubunye.hooks")
    classes: List[type[Hook]] = []
    for ep in group_eps:
        try:
            classes.append(ep.load())
        except Exception:
            # A broken third-party hook must not prevent the task from running.
            pass
    return classes


def _default_hooks(cfg: Dict[str, Any]) -> List[Hook]:
    """Build the default hook list honoring ``UBUNYE_TELEMETRY`` and config monitors.

    Discovery model:

    - When ``UBUNYE_TELEMETRY`` is set, all hooks registered under the
      ``ubunye.hooks`` entry point group are instantiated with no arguments.
      Third-party packages can ship their own hooks (Slack, Datadog, audit
      logs, drift detectors) and Ubunye will pick them up automatically.
    - ``LegacyMonitorsHook`` is always appended — it reads ``CONFIG.monitors``
      and runs user-declared monitors (MLflow, lineage recorders, etc.),
      preserving the pre-hook behavior.

    Hooks that need constructor arguments should be passed explicitly via
    ``Engine(hooks=[...])``.
    """
    from ubunye.telemetry.hooks import LegacyMonitorsHook

    hooks: List[Hook] = []
    if _TELEMETRY_ENABLED:
        for hook_cls in _discover_hooks():
            try:
                hooks.append(hook_cls())
            except Exception:
                # Hook __init__ errors shouldn't fail the run.
                pass
    hooks.append(LegacyMonitorsHook(cfg))
    return hooks


class Engine:
    """
    Executes a task by reading inputs, applying one or more transforms, and writing outputs.

    Minimal required config structure:
      cfg[``CONFIG``][``inputs``]   : mapping input_name -> reader cfg (must include 'format')
      cfg[``CONFIG``][``outputs``]  : mapping output_name -> writer cfg (must include 'format')
      cfg[``CONFIG``][``transform``]: EITHER a single transform dict with 'type',
                                  OR a list of transform dicts to form a pipeline.

    Observation (logging, metrics, tracing, user monitors) is delegated to
    :class:`ubunye.core.hooks.Hook` instances. Pass ``hooks=`` to override the
    default set.
    """

    def __init__(
        self,
        backend: Optional[Backend] = None,
        registry: Optional[Registry] = None,
        context: Optional[EngineContext] = None,
        hooks: Optional[Iterable[Hook]] = None,
        extra_hooks: Optional[Iterable[Hook]] = None,
        manage_backend: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        hooks : iterable of Hook, optional
            Replace the default hook set entirely.
        extra_hooks : iterable of Hook, optional
            Append these hooks to the default set. Ignored when ``hooks`` is
            also given.
        manage_backend : bool, default True
            If True (default), the engine calls ``backend.start()`` and
            ``backend.stop()``. Set to False when the caller owns the backend
            lifecycle (e.g. Python API running multiple tasks on one session).
        """
        self.backend = backend or SparkBackend(app_name="ubunye")
        self.registry = registry or Registry.from_entrypoints()
        self.context = context or EngineContext(run_id=str(uuid.uuid4()))
        self._hooks_override = list(hooks) if hooks is not None else None
        self._extra_hooks = list(extra_hooks) if extra_hooks else []
        self._manage_backend = manage_backend

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

        # Resolve context for observability
        task_name = self.context.task_name or cfg.get("TASK_NAME") or "unknown_task"
        profile = self.context.profile or cfg.get("ENGINE", {}).get("active_profile") or "default"
        ctx = EngineContext(run_id=self.context.run_id, profile=profile, task_name=task_name)

        if self._hooks_override is not None:
            hook_list = self._hooks_override
        else:
            hook_list = _default_hooks(cfg) + self._extra_hooks
        chain = HookChain(hook_list)
        state: Dict[str, Any] = {"outputs": None}

        if dry_run:
            with chain.task(ctx, cfg, state):
                pass
            return None

        with chain.task(ctx, cfg, state):
            if self._manage_backend:
                self.backend.start()
            try:
                sources = self._read_inputs(ctx, chain, inputs_cfg)
                outputs_map = self._apply_transforms(ctx, chain, sources, transforms)
                self._write_outputs(ctx, chain, outputs_cfg, outputs_map)
                state["outputs"] = outputs_map
                return outputs_map
            finally:
                if self._manage_backend:
                    self.backend.stop()

    # ---------- pipeline stages ----------

    def _read_inputs(
        self,
        ctx: EngineContext,
        chain: HookChain,
        inputs_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        sources: Dict[str, Any] = {}
        for name in sorted(inputs_cfg):
            icfg = inputs_cfg[name]
            rtype = icfg["format"]
            reader_cls = self.registry.readers.get(rtype)
            if not reader_cls:
                raise KeyError(
                    f"Reader plugin '{rtype}' not found for input '{name}'. "
                    f"Installed: {sorted(self.registry.readers)}"
                )
            with chain.step(ctx, f"Reader:{rtype}", {"input": name}):
                sources[name] = reader_cls().read(icfg, self.backend)
        return sources

    def _apply_transforms(
        self,
        ctx: EngineContext,
        chain: HookChain,
        sources: Dict[str, Any],
        transforms: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        outputs_map: Dict[str, Any] = dict(sources)
        for tcfg in transforms:
            ttype = tcfg["type"]
            tcls = self.registry.transforms[ttype]
            with chain.step(ctx, f"Transform:{ttype}", None):
                outputs_map = tcls().apply(outputs_map, tcfg, self.backend)
            if not isinstance(outputs_map, dict):
                raise TypeError(
                    f"Transform '{ttype}' must return a dict[str, DataFrame], "
                    f"got {type(outputs_map)}"
                )
        return outputs_map

    def _write_outputs(
        self,
        ctx: EngineContext,
        chain: HookChain,
        outputs_cfg: Dict[str, Any],
        outputs_map: Dict[str, Any],
    ) -> None:
        for name in sorted(outputs_cfg):
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
            with chain.step(ctx, f"Writer:{wtype}", {"output": name}):
                writer_cls().write(outputs_map[name], ocfg, self.backend)

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
