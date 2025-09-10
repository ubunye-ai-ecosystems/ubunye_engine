"""Engine runtime and plugin registry."""
from __future__ import annotations

import importlib.metadata as md
import uuid
from dataclasses import dataclass
from typing import Dict, Any, Iterable, List, Optional

from ubunye.core.interfaces import Reader, Writer, Transform, Backend
from ubunye.backends.spark_backend import SparkBackend  # default backend


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
        # On Python 3.10+, md.entry_points(group=...) returns mapping for that group.
        # If running earlier, adapt accordingly.
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

    def __init__(self,
                 backend: Optional[Backend] = None,
                 registry: Optional[Registry] = None,
                 context: Optional[EngineContext] = None) -> None:
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
            If True, validates and prints the plan but does not hit sources/sinks.

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

        if dry_run:
            # No backend start here; just report plan
            # (Real CLI prints this; engine just returns None.)
            return None

        # Execute
        self.backend.start()
        try:
            sources = self._read_all(inputs_cfg)
            outputs = self._apply_transforms_chain(sources, transforms)
            self._write_all(outputs, outputs_cfg)
            return outputs
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

    def _read_all(self, inputs_cfg: Dict[str, Any]) -> Dict[str, Any]:
        sources: Dict[str, Any] = {}
        for name in sorted(inputs_cfg):  # deterministic
            icfg = inputs_cfg[name]
            rtype = icfg["format"]
            reader_cls = self.registry.readers.get(rtype)
            if not reader_cls:
                raise KeyError(
                    f"Reader plugin '{rtype}' not found for input '{name}'. "
                    f"Installed: {sorted(self.registry.readers)}"
                )
            df = reader_cls().read(icfg, self.backend)
            sources[name] = df
        return sources

    def _apply_transforms_chain(self,
                                initial: Dict[str, Any],
                                transforms: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        outputs = dict(initial)
        for tcfg in transforms:
            ttype = tcfg["type"]
            tcls = self.registry.transforms[ttype]
            outputs = tcls().apply(outputs, tcfg, self.backend)
            if not isinstance(outputs, dict):
                raise TypeError(f"Transform '{ttype}' must return a dict[str, DataFrame], got {type(outputs)}")
        return outputs

    def _write_all(self, outputs: Dict[str, Any], outputs_cfg: Dict[str, Any]) -> None:
        for name in sorted(outputs_cfg):  # deterministic
            ocfg = outputs_cfg[name]
            wtype = ocfg["format"]
            writer_cls = self.registry.writers.get(wtype)
            if not writer_cls:
                raise KeyError(
                    f"Writer plugin '{wtype}' not found for output '{name}'. "
                    f"Installed: {sorted(self.registry.writers)}"
                )
            if name not in outputs:
                raise KeyError(f"Transform did not return output '{name}' expected by config.")
            writer_cls().write(outputs[name], ocfg, self.backend)
