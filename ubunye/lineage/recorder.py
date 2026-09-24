"""LineageRecorder — a Monitor plugin that captures run lineage.

``LineageRecorder`` implements the ``Monitor`` protocol defined in
``ubunye.telemetry.monitors`` so it integrates transparently with the existing
monitor chain. It can be:

1. **Enabled via CLI flag** — ``ubunye run --lineage`` injects it automatically.
2. **Enabled via config** — add to ``CONFIG.monitors`` in ``config.yaml``:

   .. code-block:: yaml

       CONFIG:
         monitors:
           - type: lineage
             params:
               store: filesystem
               base_dir: .ubunye/lineage

3. **Enabled as entry-point** — registered under ``ubunye.monitors`` so users
   can reference it by name without importing.

The recorder creates a ``"running"`` record at ``task_start``, then updates it
with final status, duration, and per-step hashes at ``task_end``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ubunye.lineage import evidence
from ubunye.lineage.context import RunContext, StepRecord
from ubunye.lineage.storage import FileSystemLineageStore, LineageStore, S3LineageStore


def _utcnow() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _hash_config(config: dict) -> str:
    """The config hash, computed as ``ubunye plan`` computes it."""
    from ubunye.config.hashing import config_hash

    return config_hash(config)


def _engine_version() -> str:
    try:
        from importlib.metadata import version

        return version("ubunye-engine")
    except Exception:
        return "unknown"


def _make_store(store: str, base_dir: str) -> LineageStore:
    if store == "s3":
        return S3LineageStore(base_dir)
    return FileSystemLineageStore(base_dir)


def _fingerprint_into(step: StepRecord, frame: Any) -> None:
    """Every row, one pass, the same method on every engine (ADR 006).

    A failure is recorded as a failure: no data_hash and the reason, never the
    schema hash standing in for the data.
    """
    from ubunye.lineage.content_hash import fingerprint

    print_ = fingerprint(frame)
    step.schema_hash = print_.schema_hash
    step.data_hash = print_.data_hash
    step.row_count = print_.row_count
    step.hash_method = print_.method
    step.hash_error = print_.error


class LineageRecorder:
    """Monitor plugin that persists run lineage as structured JSON.

    Parameters
    ----------
    store:
        Backend type — ``"filesystem"`` (default) or ``"s3"`` (stub).
    base_dir:
        Root directory for the ``FileSystemLineageStore``.
    sample_fraction:
        Ignored since 0.6.0 and kept so existing configs still load: every row is
        hashed now (the ``rows-v1`` content hash), so there is nothing to sample.
    """

    def __init__(
        self,
        store: str = "filesystem",
        base_dir: str = ".ubunye/lineage",
        sample_fraction: float = 0.01,
        hash_inputs: bool = True,
    ) -> None:
        self._store: LineageStore = _make_store(store, base_dir)
        # Inputs are hashed like outputs (every row). It costs a pass over each
        # input; turn it off for inputs too large to read twice.
        self._hash_inputs = hash_inputs
        self._sample_fraction = sample_fraction
        # In-flight run contexts keyed by run_id (supports concurrent tasks)
        self._runs: Dict[str, RunContext] = {}

    # ------------------------------------------------------------------
    # Monitor protocol
    # ------------------------------------------------------------------

    def task_start(self, *, context: Any, config: dict) -> None:  # noqa: ANN001
        """Create a ``"running"`` lineage record and persist it immediately."""
        run_id = context.run_id
        task_name = context.task_name or "unknown"
        profile = context.profile or "default"

        # Derive usecase / package / task from task_name or EngineContext
        # task_name may be "fraud_detection/ingestion/claim_etl" or plain "claim_etl"
        parts = task_name.replace("\\", "/").strip("/").split("/")
        if len(parts) >= 3:
            usecase, package, task = parts[-3], parts[-2], parts[-1]
        elif len(parts) == 2:
            usecase, package, task = parts[0], parts[1], parts[1]
        else:
            usecase = package = task = task_name

        task_path = f"{usecase}/{package}/{task}"

        top_cfg = config or {}
        model = top_cfg.get("MODEL", "")
        version = top_cfg.get("VERSION", "")

        env = evidence.environment()
        ctx = RunContext(
            run_id=run_id,
            task_path=task_path,
            usecase=usecase,
            package=package,
            task_name=task,
            profile=profile,
            model=model,
            version=version,
            # The hash of the config as loaded, set by the entry point before the
            # engine rewrites it; hashing what arrives here would not match the plan.
            config_hash=getattr(context, "config_hash", None) or _hash_config(top_cfg),
            started_at=_utcnow(),
            engine_version=_engine_version(),
            backend=getattr(context, "backend", None) or "",
            code_hash=evidence.code_hash(getattr(context, "task_dir", None)),
            environment=env,
            environment_hash=evidence.environment_hash(env),
            variables={
                k: v
                for k, v in dict(getattr(context, "variables", {}) or {}).items()
                if v is not None
            },
        )
        self._runs[run_id] = ctx
        try:
            self._store.save(ctx)
        except Exception:
            pass  # Never break the task due to lineage recording failure

    def task_end(
        self,
        *,
        context: Any,
        config: dict,
        outputs: Optional[Dict[str, Any]],
        status: str,
        duration_sec: float,
        inputs: Optional[Dict[str, Any]] = None,
        expectations: Optional[List[Dict[str, Any]]] = None,
        timings: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Update the run record with final status, duration, and step hashes."""
        run_id = context.run_id
        ctx = self._runs.get(run_id)
        if ctx is None:
            return  # task_start was not called (e.g. safe_call swallowed it)

        ctx.ended_at = _utcnow()
        ctx.duration_sec = duration_sec
        ctx.status = status

        cfg_section = (config or {}).get("CONFIG", {})
        inputs_cfg: Dict[str, Any] = cfg_section.get("inputs", {}) or {}
        outputs_cfg: Dict[str, Any] = cfg_section.get("outputs", {}) or {}

        ctx.timings = list(timings or [])
        ctx.expectations = list(expectations or [])

        # --- Input StepRecords, hashed like outputs when the frames are given ---
        ctx.inputs = []
        for name, io_cfg in inputs_cfg.items():
            step = StepRecord.from_io_cfg(name, "input", io_cfg)
            if self._hash_inputs and inputs and inputs.get(name) is not None:
                _fingerprint_into(step, inputs[name])
            ctx.inputs.append(step)

        # --- Build output StepRecords with optional DataFrame hashes ---
        step_outputs: list[StepRecord] = []
        for name, io_cfg in outputs_cfg.items():
            step = StepRecord.from_io_cfg(name, "output", io_cfg)
            if outputs and name in outputs and outputs[name] is not None:
                _fingerprint_into(step, outputs[name])
            step_outputs.append(step)
        ctx.outputs = step_outputs

        try:
            self._store.save(ctx)
        except Exception:
            pass

        # Clean up in-flight state
        self._runs.pop(run_id, None)
