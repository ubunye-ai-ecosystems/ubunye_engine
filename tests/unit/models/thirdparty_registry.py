"""A registry backend written the way a third party would: no Ubunye internals.

It implements only the six :class:`~ubunye.interfaces.RegistryBackend` methods,
keeps one JSON file per model, and names its stages with aliases of its own
(``champion`` for production, ``challenger`` for staging), mapping them to the
public stage names at the edge. The tests register it through a real entry point
(a ``.dist-info`` folder on ``sys.path``) to prove a registry needs no edit to
Ubunye.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ubunye.core.errors import VersionNotFoundError
from ubunye.interfaces.registry import ModelVersionInfo
from ubunye.models.lifecycle import check_demote

_TO_ALIAS = {
    "development": "dev",
    "staging": "challenger",
    "production": "champion",
    "archived": "retired",
}
_FROM_ALIAS = {v: k for k, v in _TO_ALIAS.items()}


class JsonRegistryBackend:
    def __init__(self, store: str) -> None:
        self.root = Path(store)

    def _index_path(self, use_case: str, model_name: str) -> Path:
        return self.root / use_case / model_name / "index.json"

    def _load(self, use_case: str, model_name: str) -> Dict[str, Any]:
        path = self._index_path(use_case, model_name)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _save(self, use_case: str, model_name: str, index: Dict[str, Any]) -> None:
        path = self._index_path(use_case, model_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(index), encoding="utf-8")

    def _info(self, model_name: str, entry: Dict[str, Any]) -> ModelVersionInfo:
        return ModelVersionInfo(
            name=model_name,
            version=entry["version"],
            stage=_FROM_ALIAS[entry["alias"]],
            created_at=entry["created_at"],
            engine_version="thirdparty",
            artifact_path=entry["path"],
            metrics=dict(entry["metrics"]),
            metadata=dict(entry["metadata"]),
        )

    def _entry(self, use_case: str, model_name: str, version: str) -> Dict[str, Any]:
        entry = self._load(use_case, model_name).get(version)
        if entry is None:
            raise VersionNotFoundError(f"No version '{version}' of {use_case}/{model_name}.")
        return entry

    def register(
        self,
        *,
        use_case: str,
        model_name: str,
        version: Optional[str],
        model: Any,
        metrics: Dict[str, Any],
        metadata: Optional[Dict[str, str]] = None,
        lineage_run_id: Optional[str] = None,
    ) -> ModelVersionInfo:
        if version is None:
            raise ValueError("This backend needs an explicit version.")
        path = self.root / use_case / model_name / version
        model.save(str(path))
        meta = dict(metadata or {})
        if lineage_run_id:
            meta["lineage_run_id"] = lineage_run_id
        entry = {
            "version": version,
            "alias": "dev",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "path": str(path),
            "metrics": metrics,
            "metadata": meta,
        }
        index = self._load(use_case, model_name)
        index[version] = entry
        self._save(use_case, model_name, index)
        return self._info(model_name, entry)

    def get(
        self,
        *,
        use_case: str,
        model_name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> Tuple[str, ModelVersionInfo]:
        if version is not None:
            entry = self._entry(use_case, model_name, version)
        else:
            alias = _TO_ALIAS[stage or "production"]
            found = [e for e in self._load(use_case, model_name).values() if e["alias"] == alias]
            if not found:
                raise VersionNotFoundError(f"No version of {model_name} is '{stage}'.")
            entry = found[0]
        return entry["path"], self._info(model_name, entry)

    def list_versions(self, *, use_case: str, model_name: str) -> List[ModelVersionInfo]:
        entries = sorted(
            self._load(use_case, model_name).values(),
            key=lambda e: e["created_at"],
            reverse=True,
        )
        return [self._info(model_name, e) for e in entries]

    def _move(self, use_case: str, model_name: str, version: str, stage: str) -> ModelVersionInfo:
        index = self._load(use_case, model_name)
        if version not in index:
            raise VersionNotFoundError(f"No version '{version}' of {use_case}/{model_name}.")
        index[version]["alias"] = _TO_ALIAS[stage]
        self._save(use_case, model_name, index)
        return self._info(model_name, index[version])

    def promote(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
        gates: Optional[Dict[str, Any]] = None,
        promoted_by: Optional[str] = None,
    ) -> ModelVersionInfo:
        return self._move(use_case, model_name, version, to_stage)

    def demote(
        self, *, use_case: str, model_name: str, version: str, to_stage: str
    ) -> ModelVersionInfo:
        current = _FROM_ALIAS[self._entry(use_case, model_name, version)["alias"]]
        check_demote(version, current, to_stage)  # demote never moves a version up
        return self._move(use_case, model_name, version, to_stage)

    def delete(self, *, use_case: str, model_name: str, version: str) -> None:
        index = self._load(use_case, model_name)
        index.pop(version, None)
        self._save(use_case, model_name, index)
