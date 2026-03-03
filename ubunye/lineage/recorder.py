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
               sample_fraction: 0.01

3. **Enabled as entry-point** — registered under ``ubunye.monitors`` so users
   can reference it by name without importing.

The recorder creates a ``"running"`` record at ``task_start``, then updates it
with final status, duration, and per-step hashes at ``task_end``.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ubunye.lineage.context import RunContext, StepRecord
from ubunye.lineage.storage import FileSystemLineageStore, LineageStore, S3LineageStore


def _utcnow() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _hash_config(config: dict) -> str:
    """Return a sha256 of the JSON-serialised config dict."""
    payload = json.dumps(config, sort_keys=True, default=str).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _make_store(store: str, base_dir: str) -> LineageStore:
    if store == "s3":
        return S3LineageStore(base_dir)
    return FileSystemLineageStore(base_dir)


class LineageRecorder:
    """Monitor plugin that persists run lineage as structured JSON.

    Parameters
    ----------
    store:
        Backend type — ``"filesystem"`` (default) or ``"s3"`` (stub).
    base_dir:
        Root directory for the ``FileSystemLineageStore``.
    sample_fraction:
        Fraction of rows sampled when hashing DataFrames (0 < value ≤ 1).
    """

    def __init__(
        self,
        store: str = "filesystem",
        base_dir: str = ".ubunye/lineage",
        sample_fraction: float = 0.01,
    ) -> None:
        self._store: LineageStore = _make_store(store, base_dir)
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

        ctx = RunContext(
            run_id=run_id,
            task_path=task_path,
            usecase=usecase,
            package=package,
            task_name=task,
            profile=profile,
            model=model,
            version=version,
            config_hash=_hash_config(top_cfg),
            started_at=_utcnow(),
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

        # --- Build input StepRecords (no DataFrame hashing — inputs were read) ---
        ctx.inputs = [
            StepRecord.from_io_cfg(name, "input", io_cfg)
            for name, io_cfg in inputs_cfg.items()
        ]

        # --- Build output StepRecords with optional DataFrame hashes ---
        step_outputs: list[StepRecord] = []
        for name, io_cfg in outputs_cfg.items():
            step = StepRecord.from_io_cfg(name, "output", io_cfg)
            if outputs and name in outputs:
                df = outputs[name]
                if df is not None:
                    try:
                        from ubunye.lineage.hasher import hash_dataframe, hash_schema
                        step.schema_hash = hash_schema(df)
                        step.data_hash = hash_dataframe(df, sample_fraction=self._sample_fraction)
                        step.row_count = int(df.count())
                    except Exception:
                        pass  # Hashing is best-effort
            step_outputs.append(step)
        ctx.outputs = step_outputs

        try:
            self._store.save(ctx)
        except Exception:
            pass

        # Clean up in-flight state
        self._runs.pop(run_id, None)
