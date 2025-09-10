"""Engine runtime and plugin registry."""
from __future__ import annotations
from typing import Dict, Any
import importlib.metadata as md

from ubunye.core.interfaces import Reader, Writer, Transform, Backend
from ubunye.backends.spark_backend import SparkBackend  # default backend


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


class Engine:
    """Executes a single task by reading inputs, applying transform, and writing outputs."""

    def __init__(self, backend: Backend | None = None, registry: Registry | None = None) -> None:
        self.backend = backend or SparkBackend(app_name="ubunye")
        self.registry = registry or Registry.from_entrypoints()

    def run(self, cfg: dict) -> None:
        """Run a task using the provided config mapping.

        Expected config structure:
          cfg["CONFIG"]["inputs"]:  mapping name -> reader cfg (must include "format")
          cfg["CONFIG"]["outputs"]: mapping name -> writer cfg (must include "format")
          cfg["CONFIG"]["transform"]: transform cfg (must include "type"; defaults to "noop")
        """
        # Start backend
        self.backend.start()
        try:
            # Read inputs
            inputs_cfg = cfg.get("CONFIG", {}).get("inputs", {}) or {}
            sources: Dict[str, Any] = {}
            for name, icfg in inputs_cfg.items():
                rtype = icfg.get("format")
                if not rtype:
                    raise ValueError(f"Input '{name}' missing 'format'")
                reader_cls = self.registry.readers.get(rtype)
                if not reader_cls:
                    raise KeyError(f"Reader plugin '{rtype}' not found. Installed: {list(self.registry.readers)}")
                df = reader_cls().read(icfg, self.backend)
                sources[name] = df

            # Transform
            tcfg = cfg.get("CONFIG", {}).get("transform", {"type": "noop"}) or {"type": "noop"}
            ttype = tcfg.get("type", "noop")
            tcls = self.registry.transforms.get(ttype)
            if not tcls:
                raise KeyError(f"Transform plugin '{ttype}' not found. Installed: {list(self.registry.transforms)}")
            outputs_map = tcls().apply(sources, tcfg, self.backend)

            # Write outputs
            outputs_cfg = cfg.get("CONFIG", {}).get("outputs", {}) or {}
            for name, ocfg in outputs_cfg.items():
                wtype = ocfg.get("format")
                if not wtype:
                    raise ValueError(f"Output '{name}' missing 'format'")
                writer_cls = self.registry.writers.get(wtype)
                if not writer_cls:
                    raise KeyError(f"Writer plugin '{wtype}' not found. Installed: {list(self.registry.writers)}")
                df = outputs_map.get(name)
                if df is None:
                    raise KeyError(f"Transform did not return output '{name}' expected by config.")
                writer_cls().write(df, ocfg, self.backend)
        finally:
            self.backend.stop()
