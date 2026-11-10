"""What a model step puts on the run record (ADR 012).

The engine opens :func:`recording` around the transforms of a run; a
:class:`~ubunye.plugins.transforms.model_transform.ModelTransform` adds one
entry per train or predict with :func:`record`. The list ends up in
``RunContext.model_runs``. Outside a run nothing is recorded and nothing fails.

An entry is plain JSON (strings, numbers, lists, dicts), so every run record
store keeps it as it is.
"""

from __future__ import annotations

import contextvars
import math
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

_ACTIVE: contextvars.ContextVar[Optional[List[Dict[str, Any]]]] = contextvars.ContextVar(
    "ubunye_model_runs", default=None
)


@contextmanager
def recording(into: Optional[List[Dict[str, Any]]] = None) -> Iterator[List[Dict[str, Any]]]:
    """Collect every model entry made inside the block; the engine opens one per run."""
    runs: List[Dict[str, Any]] = into if into is not None else []
    token = _ACTIVE.set(runs)
    try:
        yield runs
    finally:
        _ACTIVE.reset(token)


def record(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Add *entry* to the run being recorded, if there is one. Returns it."""
    runs = _ACTIVE.get()
    if runs is not None:
        runs.append(entry)
    return entry


def plain(value: Any) -> Any:
    """*value* as plain JSON: numpy and other numbers become float or int, the rest str."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    for kind in (int, float):
        try:
            if hasattr(value, "item"):  # numpy scalars
                return plain(value.item())
            return plain(kind(value))
        except (TypeError, ValueError):
            continue
    return str(value)


def gate_checks(results: List[Any]) -> List[Dict[str, Any]]:
    """Promotion gate results (:class:`~ubunye.models.gates.GateResult`) as entries."""
    checks = []
    for r in results:
        name = r.gate_name
        metric = name[4:] if name.startswith(("min_", "max_")) else None
        checks.append(
            {
                "gate": name,
                "metric": metric,
                "passed": bool(r.passed),
                "actual": plain(r.actual_value),
                "threshold": plain(r.threshold),
                "message": r.message,
            }
        )
    return checks


def blocked_by(checks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The failed checks, as the gate, the metric, its value and the threshold."""
    return [
        {k: c[k] for k in ("gate", "metric", "actual", "threshold")}
        for c in checks
        if not c["passed"]
    ]
