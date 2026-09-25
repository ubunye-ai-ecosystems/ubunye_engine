"""Engine runtime and plugin registry."""

from __future__ import annotations

import importlib.metadata as md
import logging
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional

from ubunye.core.capabilities import Capabilities, check_task
from ubunye.core.errors import (
    BackendCapabilityError,
    ReaderNotFoundError,
    TransformNotFoundError,
    TransformOutputError,
    WriterNotFoundError,
)
from ubunye.core.hooks import Hook, HookChain
from ubunye.core.interfaces import Backend, Reader, Transform, Writer
from ubunye.core.secrets import SecretResolver

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EngineContext:
    """Lightweight context passed around for observability and debugging."""

    run_id: str
    profile: Optional[str] = None
    task_name: Optional[str] = None  # e.g., "fraud_detection/claims/claim_etl"
    #: The backend running the task ("spark", "pandas", ...); filled in by the engine.
    backend: Optional[str] = None
    #: The template variables the run was given (dt, dtf, mode, ...).
    variables: Dict[str, Any] = field(default_factory=dict)
    #: The hash of the resolved config as loaded (ubunye.config.hashing), the
    #: same one ``ubunye plan`` shows; set before the engine rewrites the config.
    config_hash: Optional[str] = None
    #: The task folder, so the run record can hash the task's code.
    task_dir: Optional[str] = None


class Registry:
    """Discovers plugins via Python entry points."""

    def __init__(self) -> None:
        self.readers: Dict[str, type[Reader]] = {}
        self.writers: Dict[str, type[Writer]] = {}
        self.transforms: Dict[str, type[Transform]] = {}

    @staticmethod
    def _load(group: str) -> Dict[str, Any]:
        return {ep.name: ep.load() for ep in md.entry_points(group=group)}

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
def _telemetry_enabled() -> bool:
    """``UBUNYE_TELEMETRY``, read when a run starts.

    It was read once at import, so setting it in a notebook (or anywhere after
    ``import ubunye``) did nothing.
    """
    return os.getenv("UBUNYE_TELEMETRY", "0").strip().lower() not in ("0", "", "false", "no", "off")


def _discover_hooks() -> List[type[Hook]]:
    """Load Hook classes from the ``ubunye.hooks`` entry point group."""
    classes: List[type[Hook]] = []
    for ep in md.entry_points(group="ubunye.hooks"):
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
    if _telemetry_enabled():
        for hook_cls in _discover_hooks():
            try:
                hooks.append(hook_cls())
            except Exception:
                # Hook __init__ errors shouldn't fail the run.
                pass
    hooks.append(LegacyMonitorsHook(cfg))
    return hooks


def _unwrap(frame: Any) -> Any:
    """A frame wrapper as the frame it wraps (ADR 005).

    A transform written with Narwhals may return the Narwhals frame; it offers
    ``to_native()``, and so does any wrapper that follows the same protocol. The
    engine unwraps by that method alone and never imports the wrapper's library.
    """
    to_native = getattr(type(frame), "to_native", None)
    return frame.to_native() if callable(to_native) else frame


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
        # With no backend given, one is resolved like any run (the platform's
        # session, else the default) when it is first needed, so an Engine can be
        # built, and a config checked, where no engine is installed. The core
        # names no engine (ADR 001 and 003).
        self._backend: Optional[Backend] = backend
        self.registry = registry or Registry.from_entrypoints()
        self.context = context or EngineContext(run_id=str(uuid.uuid4()))
        self._hooks_override = list(hooks) if hooks is not None else None
        self._extra_hooks = list(extra_hooks) if extra_hooks else []
        # One per engine, so each secret is fetched once per run.
        self._secrets = SecretResolver()
        # Per-step timings of the current run, for the run record.
        self._timings: List[Dict[str, Any]] = []
        self._manage_backend = manage_backend

    @property
    def backend(self) -> Any:
        """The backend running this engine's tasks, resolved on first use."""
        if self._backend is None:
            from ubunye.core import backends

            self._backend = backends.resolve(None, app_name="ubunye")
        return self._backend

    @backend.setter
    def backend(self, value: Any) -> None:
        self._backend = value

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
        transform_cfg = cfg.get("CONFIG", {}).get("transform") or {}

        # Preflight validation
        self._validate_io_configs(inputs_cfg, outputs_cfg)
        transforms = self._normalize_transforms(transform_cfg)
        self._warn_deprecated_noop(transforms)
        self._validate_transforms_exist(transforms)
        self._check_backend_can_run(cfg)

        ctx = self._resolve_context(cfg)
        chain = self._build_hook_chain(cfg)
        state: Dict[str, Any] = {"outputs": None}

        if dry_run:
            with chain.task(ctx, cfg, state):
                pass
            return None

        self._timings = []
        state["timings"] = self._timings
        state["llm_calls"] = []
        with chain.task(ctx, cfg, state):
            if self._manage_backend:
                self.backend.start()
            try:
                sources = self._read_inputs(ctx, chain, inputs_cfg)
                state["inputs"] = self._to_ports(sources)
                from ubunye import llm

                budget = llm.budget.Budget.from_env()
                try:
                    with llm.recording(state["llm_calls"], task_dir=ctx.task_dir, budget=budget):
                        outputs_map = self._apply_transforms(ctx, chain, sources, transforms)
                finally:
                    state["llm_budget"] = budget.summary() if budget.limited else {}
                outputs_map = self._check_expectations(cfg, outputs_map, state)
                ports = self._to_ports(outputs_map)
                self._write_outputs(ctx, chain, outputs_cfg, ports)
                # Hooks (lineage, monitors) get the port; the caller gets native frames.
                state["outputs"] = ports
                return outputs_map
            finally:
                if self._manage_backend:
                    self.backend.stop()

    def read_inputs(self, cfg: dict) -> Dict[str, Any]:
        """Read all inputs defined in ``CONFIG.inputs``.

        Returns a dict mapping input name to the backend's own frame type (a
        ``pandas.DataFrame`` on pandas) — suitable for interactive inspection
        before calling :meth:`apply_transforms`.
        """
        inputs_cfg = cfg.get("CONFIG", {}).get("inputs", {}) or {}
        outputs_cfg = cfg.get("CONFIG", {}).get("outputs", {}) or {}
        self._validate_io_configs(inputs_cfg, outputs_cfg)
        ctx = self._resolve_context(cfg)
        chain = self._build_hook_chain(cfg)
        return self._to_natives(self._read_inputs(ctx, chain, inputs_cfg))

    def apply_transforms(self, sources: Dict[str, Any], cfg: dict) -> Dict[str, Any]:
        """Apply configured transforms to *sources*.

        Returns a dict mapping output name to the backend's own frame type.
        """
        transform_cfg = cfg.get("CONFIG", {}).get("transform") or {}
        transforms = self._normalize_transforms(transform_cfg)
        self._validate_transforms_exist(transforms)
        ctx = self._resolve_context(cfg)
        chain = self._build_hook_chain(cfg)
        return self._apply_transforms(ctx, chain, sources, transforms)

    def write_outputs(self, outputs: Dict[str, Any], cfg: dict, *, as_run: bool = False) -> None:
        """Write *outputs* to the sinks defined in ``CONFIG.outputs``.

        With ``as_run=True`` the write is wrapped as a whole task for the hooks,
        so lineage and monitors record it exactly as they record ``run()``. This
        is how a notebook that reads, transforms and writes step by step still
        leaves a run record.
        """
        outputs_cfg = cfg.get("CONFIG", {}).get("outputs", {}) or {}
        ctx = self._resolve_context(cfg)
        chain = self._build_hook_chain(cfg)
        state: Dict[str, Any] = {"outputs": None}
        if not as_run:
            outputs = self._check_expectations(cfg, outputs, state)
            self._write_outputs(ctx, chain, outputs_cfg, self._to_ports(outputs))
            return
        with chain.task(ctx, cfg, state):
            outputs = self._check_expectations(cfg, outputs, state)
            ports = self._to_ports(outputs)
            self._write_outputs(ctx, chain, outputs_cfg, ports)
            state["outputs"] = ports

    def _check_expectations(
        self, cfg: dict, outputs: Dict[str, Any], state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """``CONFIG.expectations``: check every output before any is written.

        Returns the frames to write: clean rows, plus the quarantined rows under
        their quarantine output. Raises ``ExpectationError`` (nothing written)
        when a ``fail`` rule is broken. Every rule's result goes into
        ``state["expectations"]`` for the hooks.
        """
        raw = (cfg.get("CONFIG") or {}).get("expectations") or {}
        if not raw:
            return outputs
        from ubunye.config.schema import ExpectationSet
        from ubunye.core import expectations

        specs = {name: ExpectationSet.model_validate(spec) for name, spec in raw.items()}
        from ubunye.core.errors import ExpectationError

        try:
            checked, results = expectations.apply(self._to_natives(outputs), specs)
        except ExpectationError as exc:
            state["expectations"] = list(exc.results)
            raise
        state["expectations"] = [r.as_dict() for r in results]
        return checked

    @contextmanager
    def _step(
        self, chain: HookChain, ctx: EngineContext, label: str, meta: Optional[Dict[str, Any]]
    ) -> Iterator[None]:
        """A hook step that is also timed, for the run record."""
        t0 = time.perf_counter()
        with chain.step(ctx, label, meta):
            yield
        entry: Dict[str, Any] = {"step": label}
        entry.update(meta or {})
        entry["seconds"] = round(time.perf_counter() - t0, 6)
        self._timings.append(entry)

    # ---------- the frame boundary (ADR 004) ----------

    def _to_natives(self, frames: Dict[str, Any]) -> Dict[str, Any]:
        """Frames as a transform sees them: the backend's own type."""
        frames = {name: _unwrap(frame) for name, frame in frames.items()}
        if not isinstance(self.backend, Backend):
            return frames  # a test double or pre-0.7 object: pass through
        return {name: self.backend.to_native(frame) for name, frame in frames.items()}

    def _to_ports(self, frames: Dict[str, Any]) -> Dict[str, Any]:
        """Frames as the engine sees them: behind the DataFramePort."""
        frames = {name: _unwrap(frame) for name, frame in frames.items()}
        if not isinstance(self.backend, Backend):
            return frames
        return {name: self.backend.to_port(frame) for name, frame in frames.items()}

    # ---------- shared helpers ----------

    def _resolve_context(self, cfg: dict) -> EngineContext:
        task_name = self.context.task_name or cfg.get("TASK_NAME") or "unknown_task"
        profile = self.context.profile or cfg.get("ENGINE", {}).get("active_profile") or "default"
        backend = self.context.backend or getattr(self.backend, "name", "") or None
        return EngineContext(
            run_id=self.context.run_id,
            profile=profile,
            task_name=task_name,
            backend=backend if isinstance(backend, str) else None,
            variables=dict(self.context.variables),
            config_hash=self.context.config_hash,
            task_dir=self.context.task_dir,
        )

    def _build_hook_chain(self, cfg: dict) -> HookChain:
        if self._hooks_override is not None:
            return HookChain(self._hooks_override)
        return HookChain(_default_hooks(cfg) + self._extra_hooks)

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
                raise ReaderNotFoundError(
                    f"Reader plugin '{rtype}' not found.",
                    context={
                        "Format": rtype,
                        "Input": name,
                        "Installed": sorted(self.registry.readers),
                    },
                    hint=f"Check the 'format' field in CONFIG.inputs.{name}. "
                    f"Installed reader plugins: {', '.join(sorted(self.registry.readers))}",
                )
            with self._step(chain, ctx, f"Reader:{rtype}", {"input": name}):
                # Secrets are swapped in only here, in the connector's copy.
                sources[name] = reader_cls().read(self._secrets.resolve(icfg), self.backend)
        return sources

    def _apply_transforms(
        self,
        ctx: EngineContext,
        chain: HookChain,
        sources: Dict[str, Any],
        transforms: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        # Transforms see native frames (ADR 004); between transforms too, so one
        # that hands back a port still gives the next a native frame.
        outputs_map: Dict[str, Any] = self._to_natives(sources)
        for tcfg in transforms:
            ttype = tcfg.get("type")
            if ttype is None:
                continue
            tcls = self.registry.transforms[ttype]
            with self._step(chain, ctx, f"Transform:{ttype}", None):
                outputs_map = tcls().apply(outputs_map, tcfg, self.backend)
            if not isinstance(outputs_map, dict):
                raise TransformOutputError(
                    f"Transform '{ttype}' must return a dict[str, DataFrame].",
                    context={"Transform": ttype, "Actual type": type(outputs_map).__name__},
                    hint="The apply() method must return a dict mapping output names to DataFrames.",
                )
            outputs_map = self._to_natives(outputs_map)
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
                raise WriterNotFoundError(
                    f"Writer plugin '{wtype}' not found.",
                    context={
                        "Format": wtype,
                        "Output": name,
                        "Installed": sorted(self.registry.writers),
                    },
                    hint=f"Check the 'format' field in CONFIG.outputs.{name}. "
                    f"Installed writer plugins: {', '.join(sorted(self.registry.writers))}",
                )
            if name not in outputs_map:
                raise TransformOutputError(
                    f"Transform did not return output '{name}' expected by config.",
                    context={
                        "Missing output": name,
                        "Available outputs": sorted(outputs_map.keys()),
                    },
                    hint="Ensure your transform returns a dict with keys matching CONFIG.outputs.",
                )
            with self._step(chain, ctx, f"Writer:{wtype}", {"output": name}):
                writer_cls().write(outputs_map[name], self._secrets.resolve(ocfg), self.backend)

    # ---------- internal helpers ----------

    def _check_backend_can_run(self, cfg: dict) -> None:
        """Refuse, before anything starts, a task the backend has said it cannot run."""
        caps = getattr(self.backend, "capabilities", None)
        if not isinstance(caps, Capabilities):
            return  # a backend (or test double) that declares nothing is not pre-checked
        name = getattr(self.backend, "name", "") or type(self.backend).__name__
        io_check = self.backend.check_io if isinstance(self.backend, Backend) else None
        problems = check_task(caps, cfg, self.registry, backend_name=name, io_check=io_check)
        if problems:
            raise BackendCapabilityError(
                f"This task cannot run on the {name} backend:\n"
                + "\n".join(f"  - {p}" for p in problems),
                context={"Backend": name},
                hint="Run it on a backend that can (--backend spark), or change the "
                "inputs and outputs listed above.",
            )

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

    @staticmethod
    def _warn_deprecated_noop(transforms: Iterable[Dict[str, Any]]) -> None:
        for t in transforms:
            ttype = t.get("type")
            if ttype == "noop":
                import warnings

                warnings.warn(
                    "transform.type: noop is deprecated and can be removed. "
                    "When type is omitted the engine defaults to the Task class "
                    "in transformations.py. Remove the 'type: noop' line from "
                    "your config.yaml.",
                    DeprecationWarning,
                    stacklevel=2,
                )

    def _validate_transforms_exist(self, transforms: Iterable[Dict[str, Any]]) -> None:
        missing = []
        for t in transforms:
            ttype = t.get("type")
            if ttype is None:
                continue
            if ttype not in self.registry.transforms:
                missing.append(ttype)
        if missing:
            raise TransformNotFoundError(
                f"Transform plugin(s) not found: {missing}.",
                context={"Missing": missing, "Installed": sorted(self.registry.transforms)},
                hint="Check the 'type' field in CONFIG.transform. "
                f"Installed transform plugins: {', '.join(sorted(self.registry.transforms))}",
            )
