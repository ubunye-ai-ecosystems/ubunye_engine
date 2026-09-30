"""Engine runtime and plugin registry."""

from __future__ import annotations

import importlib.metadata as md
import logging
import os
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from ubunye.core import runs
from ubunye.core.capabilities import SELF_OVERWRITE_MARK, Capabilities, check_task
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

# The run a transform is part of. Transforms are called as
# apply(inputs, cfg, backend), so this is how one learns its run id.
_CURRENT_CONTEXT: ContextVar[Optional["EngineContext"]] = ContextVar(
    "ubunye_current_context", default=None
)


def current_run_id() -> Optional[str]:
    """The run id of the task running now, or None outside a run."""
    ctx = _CURRENT_CONTEXT.get()
    return ctx.run_id if ctx is not None else None


def _backend_time_zone(backend: Any) -> Optional[str]:
    """The zone a backend cuts time in, if it says (``timezone``); never raises."""
    try:
        zone = getattr(backend, "timezone", None)
        return zone if isinstance(zone, str) else None
    except Exception:  # noqa: BLE001
        return None


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
    #: The session time zone the backend cuts time in (ADR 007).
    time_zone: Optional[str] = None
    #: Where run records are kept, when recorded: a dead run's record is marked
    #: ``interrupted`` there by the run that takes over its lease (ADR 008).
    lineage_dir: Optional[str] = None
    #: Replace a batch a finished run already appended, rather than refuse (F-031).
    rerun: bool = False


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


def _materialise_enabled() -> bool:
    """``UBUNYE_MATERIALISE_OUTPUTS``, read when a run starts; on unless 0 (ADR 009)."""
    value = os.getenv("UBUNYE_MATERIALISE_OUTPUTS", "1").strip().lower()
    return value not in ("0", "false", "no", "off")


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
        # The notebook path: the frames last inspected before a transform (read or
        # passed to apply_transforms), their contract results and reconcile counts.
        self._inspected: Optional[Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]] = (
            None
        )
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
        held: List[Any] = []
        try:
            with chain.task(ctx, cfg, state):
                if self._manage_backend:
                    self.backend.start()
                try:
                    versions: Optional[Dict[str, Any]] = None
                    if any(getattr(h, "reads_inputs", False) for h in chain.hooks):
                        versions = state["source_versions"] = {}
                    sources = self._read_inputs(ctx, chain, inputs_cfg, versions)
                    state["inputs"] = self._to_ports(sources)
                    # Contracts and reconcile counts before the transform: it may
                    # change its inputs in place.
                    measured = self._inspect_inputs(cfg, sources, state)
                    from ubunye import llm

                    budget = llm.budget.Budget.from_env()
                    try:
                        with llm.recording(
                            state["llm_calls"], task_dir=ctx.task_dir, budget=budget
                        ):
                            outputs_map = self._apply_transforms(ctx, chain, sources, transforms)
                    finally:
                        state["llm_budget"] = budget.summary() if budget.limited else {}
                    once = self._hold_outputs(cfg, ctx, chain, outputs_map, state, held)
                    checked = self._check_expectations(cfg, once, state, measured=measured)
                    ports = self._to_ports(checked)
                    self._write_outputs(ctx, chain, outputs_cfg, ports)
                    # Hooks (lineage, monitors) get the port; the caller gets native frames.
                    state["outputs"] = ports
                    return self._hand_back(cfg, outputs_map, once, checked, state)
                finally:
                    if self._manage_backend:
                        self.backend.stop()
        finally:
            # After the hooks: the run record hashes the held frames at task end.
            self._release(held)

    def read_inputs(self, cfg: dict) -> Dict[str, Any]:
        """Read all inputs defined in ``CONFIG.inputs``.

        Returns a dict mapping input name to the backend's own frame type (a
        ``pandas.DataFrame`` on pandas) — suitable for interactive inspection
        before calling :meth:`apply_transforms`. The inputs' expectations (input
        contracts) are checked here, as in :meth:`run`, and the inputs a
        ``reconcile`` names are counted; both are kept for a later
        :meth:`write_outputs`.
        """
        inputs_cfg = cfg.get("CONFIG", {}).get("inputs", {}) or {}
        outputs_cfg = cfg.get("CONFIG", {}).get("outputs", {}) or {}
        self._validate_io_configs(inputs_cfg, outputs_cfg)
        ctx = self._resolve_context(cfg)
        chain = self._build_hook_chain(cfg)
        natives = self._to_natives(self._read_inputs(ctx, chain, inputs_cfg))
        self._inspect_for_write(cfg, natives)
        return natives

    def _inspect_for_write(self, cfg: dict, natives: Dict[str, Any]) -> None:
        """Check and count the frames a transform is about to get, for write_outputs.

        The notebook path reads, transforms and writes in separate calls, so what
        :meth:`run` keeps in local variables is kept here: the frames inspected,
        the contract results and the reconcile counts.
        """
        self._inspected = None
        state: Dict[str, Any] = {}
        measured = self._inspect_inputs(cfg, natives, state)
        self._inspected = (dict(natives), list(state.get("expectations") or []), measured)

    def apply_transforms(self, sources: Dict[str, Any], cfg: dict) -> Dict[str, Any]:
        """Apply configured transforms to *sources*.

        Returns a dict mapping output name to the backend's own frame type.
        Frames that did not come from :meth:`read_inputs` (a notebook's own
        sample, say) are checked against the input contracts and counted for a
        ``reconcile`` first, like read ones: a reconcile compares an output with
        the frames the transform actually got.
        """
        transform_cfg = cfg.get("CONFIG", {}).get("transform") or {}
        transforms = self._normalize_transforms(transform_cfg)
        self._validate_transforms_exist(transforms)
        ctx = self._resolve_context(cfg)
        chain = self._build_hook_chain(cfg)
        if not self._is_inspected(sources):
            self._inspect_for_write(cfg, self._to_natives(sources))
        return self._apply_transforms(ctx, chain, sources, transforms)

    def _is_inspected(self, sources: Dict[str, Any]) -> bool:
        if self._inspected is None:
            return False
        seen = self._inspected[0]
        natives = self._to_natives(sources)
        return set(natives) == set(seen) and all(natives[n] is seen[n] for n in seen)

    def write_outputs(
        self,
        outputs: Dict[str, Any],
        cfg: dict,
        *,
        as_run: bool = False,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write *outputs* to the sinks defined in ``CONFIG.outputs``.

        With ``as_run=True`` the write is wrapped as a whole task for the hooks,
        so lineage and monitors record it exactly as they record ``run()``. This
        is how a notebook that reads, transforms and writes step by step still
        leaves a run record. A ``reconcile`` compares with the inputs counted
        before the transform (by :meth:`read_inputs` or :meth:`apply_transforms`).
        *inputs* given here that were not counted then are counted now, after
        the transform: a transform that changed them in place hides its loss.
        """
        outputs_cfg = cfg.get("CONFIG", {}).get("outputs", {}) or {}
        # The notebook path writes here without run(): the same pre-checks apply,
        # or an overwrite of its own input deletes the source (F-047).
        self._check_backend_can_run(cfg)
        ctx = self._resolve_context(cfg)
        chain = self._build_hook_chain(cfg)
        state: Dict[str, Any] = {"outputs": None}
        measured: Optional[Dict[str, Any]] = None
        if self._inspected is not None and (inputs is None or self._is_inspected(inputs)):
            _, checks, measured = self._inspected
            if checks:
                state["expectations"] = list(checks)
        held: List[Any] = []
        try:
            if not as_run:
                once = self._hold_outputs(cfg, ctx, chain, outputs, state, held, task=False)
                outputs = self._check_expectations(cfg, once, state, inputs, measured)
                self._write_outputs(ctx, chain, outputs_cfg, self._to_ports(outputs))
                return
            with chain.task(ctx, cfg, state):
                once = self._hold_outputs(cfg, ctx, chain, outputs, state, held)
                outputs = self._check_expectations(cfg, once, state, inputs, measured)
                ports = self._to_ports(outputs)
                self._write_outputs(ctx, chain, outputs_cfg, ports)
                state["outputs"] = ports
        finally:
            self._release(held)

    @staticmethod
    def _expectation_specs(cfg: dict, side: str) -> Dict[str, Any]:
        """``CONFIG.expectations`` for the inputs (input contracts) or the outputs."""
        section = cfg.get("CONFIG") or {}
        raw = section.get("expectations") or {}
        if not raw:
            return {}
        from ubunye.config.schema import ExpectationSet

        inputs = section.get("inputs") or {}
        return {
            name: ExpectationSet.model_validate(spec)
            for name, spec in raw.items()
            if (name in inputs) == (side == "inputs")
        }

    def _inspect_inputs(
        self, cfg: dict, sources: Dict[str, Any], state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Before the transform: the input contracts, then the reconcile counts.

        Contracts (F-018) stop a source that changed shape before the transform
        can compute something wrong from it; results go into
        ``state["expectations"]`` and a broken ``fail`` rule raises
        ExpectationError. Then every input a ``reconcile`` names is counted
        (F-017), here and not after the transform, which may change its inputs
        in place (a pandas ``drop(inplace=True)``) and so hide the loss.
        """
        from ubunye.core import expectations
        from ubunye.core.errors import ExpectationError

        natives = self._to_natives(sources)
        specs = self._expectation_specs(cfg, "inputs")
        if specs:
            try:
                results = expectations.check_inputs(natives, specs)
            except ExpectationError as exc:
                state["expectations"] = list(exc.results)
                raise
            state["expectations"] = [r.as_dict() for r in results]
        outputs = {n: s for n, s in self._expectation_specs(cfg, "outputs").items() if s.reconcile}
        return expectations.measure_inputs(natives, outputs) if outputs else {}

    def _check_expectations(
        self,
        cfg: dict,
        outputs: Dict[str, Any],
        state: Dict[str, Any],
        inputs: Optional[Dict[str, Any]] = None,
        measured: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """``CONFIG.expectations``: check every output before any is written.

        Returns the frames to write: clean rows, plus the quarantined rows under
        their quarantine output. Raises ``ExpectationError`` (nothing written)
        when a ``fail`` rule is broken. Every rule's result goes into
        ``state["expectations"]`` for the hooks. A ``reconcile`` compares with
        *measured* (counted before the transform), else counts *inputs* now.
        """
        specs = self._expectation_specs(cfg, "outputs")
        if not specs:
            return outputs
        from ubunye.core import expectations
        from ubunye.core.errors import ExpectationError

        # The input contracts' results, checked before the transform, come first.
        before = list(state.get("expectations") or [])
        try:
            checked, results = expectations.apply(
                self._to_natives(outputs),
                specs,
                self._to_natives(inputs) if inputs is not None else None,
                measured,
            )
        except ExpectationError as exc:
            state["expectations"] = before + list(exc.results)
            raise
        state["expectations"] = before + [r.as_dict() for r in results]
        # A quarantine output is cut from its output's rows: it has the same basis.
        basis = state.get("hash_basis")
        if isinstance(basis, dict):
            for name, spec in specs.items():
                if spec.quarantine and name in basis:
                    basis[spec.quarantine] = basis[name]
        return checked

    # ---------- one computation per output (ADR 009) ----------

    def _hold_outputs(
        self,
        cfg: dict,
        ctx: EngineContext,
        chain: HookChain,
        outputs: Dict[str, Any],
        state: Dict[str, Any],
        held: List[Any],
        task: bool = True,
    ) -> Dict[str, Any]:
        """Each output that more than one consumer acts on, computed once.

        An output's consumers are its expectation checks, its writer, and the
        run record's hash at task end. On a lazy backend each is a new
        computation of the plan, so a value that differs per computation made
        the record hash rows that were not written (F-040), and every check and
        the write paid for the transform again (F-039, F-043). An output is held
        (``Backend.materialise``) when the run is recorded, when it has
        expectations, or when it is written under two names; a plain run is left
        exactly as it was. Every consumer then gets the held frame.

        ``state["hash_basis"]`` says, per output, whether the frame the record
        will hash is the rows written (``"materialised"``) or will be computed
        again (``"recomputed"``). What is held is added to ``held``, for
        :meth:`_release` once the hooks are done. Holding is a timed step
        (``Materialise``), so its cost is in the record's timings. ``task`` is
        False for a write outside a task, where no hook sees the outputs.
        """
        section = cfg.get("CONFIG") or {}
        outputs_cfg = section.get("outputs") or {}
        expected = set((section.get("expectations") or {}).keys())
        names = [n for n in sorted(outputs_cfg) if n in outputs]
        recorded = task and any(getattr(h, "reads_outputs", False) for h in chain.hooks)
        uses: Dict[int, int] = {}
        for name in names:
            uses[id(outputs[name])] = uses.get(id(outputs[name]), 0) + 1

        in_memory = self._frames_in_memory()
        # Only a real backend is asked: a test double or pre-0.7 object is left alone.
        is_backend = isinstance(self.backend, Backend)
        allowed = is_backend and not in_memory and _materialise_enabled()
        result = dict(outputs)
        done: Dict[int, Any] = {}  # one computation per frame, however many names
        basis: Dict[str, str] = {}
        for name in names:
            frame = outputs[name]
            needed = recorded or name in expected or uses[id(frame)] > 1
            if needed and allowed:
                if id(frame) not in done:
                    # An error here is the job failing while it computes the
                    # output; it propagates, as it would from the writer. Falling
                    # back would compute the same failing plan again (ADR 009).
                    with self._step(chain, ctx, "Materialise", {"output": name}):
                        raw = _unwrap(frame)
                        once = self.backend.materialise(raw)
                    if once is raw:  # handed back unheld: it would recompute
                        once = None
                    done[id(frame)] = once
                    if once is not None:
                        held.append(once)
                if done.get(id(frame)) is not None:
                    result[name] = done[id(frame)]
                    basis[name] = "materialised"
                    continue
            if in_memory is not None:
                basis[name] = "materialised" if in_memory else "recomputed"
        state["hash_basis"] = basis
        return result

    def _frames_in_memory(self) -> Optional[bool]:
        """True when the backend's frames are the rows themselves (not lazy).

        ``None`` when the backend has not said (a test double): the run record
        then judges by the frame's type.
        """
        caps = getattr(self.backend, "capabilities", None)
        if not isinstance(caps, Capabilities) or not caps.declared:
            return None
        return not caps.lazy

    def _hand_back(
        self,
        cfg: dict,
        original: Dict[str, Any],
        once: Dict[str, Any],
        checked: Dict[str, Any],
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        """The frames ``run`` returns: never a held frame, which is freed at task end.

        A held frame, and anything cut from it (the clean and quarantined rows),
        stops working when it is released. So the caller gets the frames the
        transform returned, cut again by the same rule results where
        expectations quarantined rows. They are lazy, as they always were.
        """
        swapped = [n for n in once if once[n] is not original.get(n)]
        if not swapped:
            return checked
        back = dict(checked)
        for name in swapped:
            if back.get(name) is once[name]:
                back[name] = original[name]
        raw = (cfg.get("CONFIG") or {}).get("expectations") or {}
        cut = {n: spec for n, spec in raw.items() if n in swapped}
        if cut:
            from ubunye.config.schema import ExpectationSet
            from ubunye.core import expectations

            specs = {n: ExpectationSet.model_validate(spec) for n, spec in cut.items()}
            natives = self._to_natives({n: original[n] for n in specs})
            back.update(expectations.cut(natives, specs, state.get("expectations") or []))
        return back

    def _release(self, held: List[Any]) -> None:
        """Free every held frame. Never raises: the run's result stands either way."""
        while held:
            frame = held.pop()
            try:
                self.backend.release(frame)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Releasing a held output failed: %s", exc)

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
            time_zone=self.context.time_zone or _backend_time_zone(self._backend),
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
        versions: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Read every input. With ``versions`` (a recorded run), each source's
        version is taken right after its read, for the run record (F-046)."""
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
            if versions is not None:
                from ubunye.lineage.source_version import capture

                versions[name] = capture(_unwrap(sources[name]), icfg)
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
        token = _CURRENT_CONTEXT.set(ctx)
        try:
            return self._apply_each(ctx, chain, outputs_map, transforms)
        finally:
            _CURRENT_CONTEXT.reset(token)

    def _apply_each(
        self,
        ctx: EngineContext,
        chain: HookChain,
        outputs_map: Dict[str, Any],
        transforms: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
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
            # Rerun safety (ADR 008): the lease knows which output is being written. A
            # backend that claims its files (pandas) has its appends taken back exactly
            # if the run fails or dies; other appends are named, never guessed at. A
            # missing mode counts as append: most writers default to it.
            runs.writing(
                name,
                appends=str(ocfg.get("mode") or "append").lower() == "append",
                exact=bool(getattr(self.backend, "claims_appends", False)),
            )
            with self._step(chain, ctx, f"Writer:{wtype}", {"output": name}):
                writer_cls().write(outputs_map[name], self._secrets.resolve(ocfg), self.backend)
            runs.written(name)

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
            if all(SELF_OVERWRITE_MARK in p for p in problems):
                hint = "Write each output listed above to a new path, or use Delta."
            else:
                hint = (
                    "Run it on a backend that can (--backend spark), or change the "
                    "inputs and outputs listed above."
                )
            raise BackendCapabilityError(
                f"This task cannot run on the {name} backend:\n"
                + "\n".join(f"  - {p}" for p in problems),
                context={"Backend": name},
                hint=hint,
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
