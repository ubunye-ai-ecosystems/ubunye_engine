"""Observations: one environment's evidence about one workload, as a JSON file."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

FORMAT = 1

#: executed: a run record exists (the run itself may have failed).
#: failed_to_launch: the environment was tried and no run happened (a submit error).
#: not_run: nobody ran it here (no credentials, not scheduled, skipped on purpose).
#: unsupported: the environment cannot run this workload, by design.
STATUSES = ("executed", "failed_to_launch", "not_run", "unsupported")

#: Where a cost figure came from. Never present an estimate as a bill.
COST_BASES = ("actual", "provider_estimate", "ubunye_estimate", "local", "unknown")


@dataclass
class Observation:
    workload: str
    environment: str
    status: str
    record: Optional[Dict[str, Any]] = None
    reason: str = ""
    #: kind (local, cloud, managed), provider, runtime, region, anything else known.
    platform: Dict[str, Any] = field(default_factory=dict)
    #: amount, currency, basis (one of COST_BASES), source (how it was obtained).
    cost: Dict[str, Any] = field(default_factory=lambda: {"basis": "unknown"})
    #: run_url, artifact, observed_at: where a reader can check this observation.
    provenance: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}, not {self.status!r}")
        basis = (self.cost or {}).get("basis", "unknown")
        if basis not in COST_BASES:
            raise ValueError(f"cost basis must be one of {COST_BASES}, not {basis!r}")
        if self.status == "executed" and not self.record:
            raise ValueError("an executed observation needs its run record")

    def to_dict(self) -> Dict[str, Any]:
        return {"ubunye_observation": FORMAT, **asdict(self)}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Observation":
        if d.get("ubunye_observation") != FORMAT:
            raise ValueError("not a Ubunye observation (ubunye_observation: 1 missing)")
        fields = {k: v for k, v in d.items() if k != "ubunye_observation"}
        return Observation(**fields)

    def save(self, folder: Union[str, Path]) -> Path:
        """Write ``<folder>/<workload>/<environment>.json`` and return the path."""
        path = Path(folder) / self.workload / f"{self.environment}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        return path


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def observe_record(
    record: Dict[str, Any],
    *,
    workload: str,
    environment: str,
    platform: Optional[Dict[str, Any]] = None,
    cost: Optional[Dict[str, Any]] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Observation:
    """An observation from a run record (a dict as the lineage store writes it)."""
    return Observation(
        workload=workload,
        environment=environment,
        status="executed",
        record=record,
        platform=dict(platform or {}),
        cost=dict(cost or {"basis": "unknown"}),
        provenance={"observed_at": _now(), **(provenance or {})},
    )


def skipped(
    *,
    workload: str,
    environment: str,
    status: str,
    reason: str,
    platform: Optional[Dict[str, Any]] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Observation:
    """An environment that did not execute the workload, and why."""
    if status == "executed":
        raise ValueError("a skipped observation cannot be 'executed'")
    return Observation(
        workload=workload,
        environment=environment,
        status=status,
        reason=reason,
        platform=dict(platform or {}),
        provenance={"observed_at": _now(), **(provenance or {})},
    )


def load_observations(folder: Union[str, Path], workload: str) -> List[Observation]:
    """Every observation of ``workload`` under ``folder`` (``<folder>/<workload>/*.json``)."""
    root = Path(folder) / workload
    if not root.is_dir():
        return []
    out = []
    for path in sorted(root.glob("*.json")):
        out.append(Observation.from_dict(json.loads(path.read_text(encoding="utf-8"))))
    return out
