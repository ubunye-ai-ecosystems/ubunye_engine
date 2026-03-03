"""Promotion gates — configurable metric thresholds for model stage transitions.

Gates are checked before a model version is promoted to a higher stage. If any
gate fails, promotion is blocked and a descriptive error is raised.

Gate config keys (used in config.yaml and passed to ModelRegistry.promote()):
    min_<metric>: float       — actual metric value must be >= threshold
    max_<metric>: float       — actual metric value must be <= threshold
    require_drift_check: bool — metadata["drift_check_passed"] must be True

Example::

    promotion_gates:
      min_auc: 0.85
      min_f1: 0.80
      max_loss: 0.5
      require_drift_check: true
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class GateResult:
    """Result of evaluating a single promotion gate."""

    gate_name: str
    passed: bool
    message: str
    actual_value: Any = None
    threshold: Any = None


class PromotionGate:
    """Evaluates a set of metric thresholds before allowing model promotion.

    Args:
        gate_config: Dict of gate rules. Supported keys:
            - ``min_<metric>``: metric must be >= value
            - ``max_<metric>``: metric must be <= value
            - ``require_drift_check``: boolean; checks ``metadata["drift_check_passed"]``
    """

    def __init__(self, gate_config: Dict[str, Any]):
        self.config = gate_config or {}

    def evaluate(
        self,
        metrics: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[GateResult]:
        """Evaluate all configured gates against the provided metrics.

        Args:
            metrics: Model metrics dict from :meth:`UbunyeModel.train`.
            metadata: Optional model metadata dict from :meth:`UbunyeModel.metadata`.

        Returns:
            List of :class:`GateResult` — one per configured gate.
            Promotion is allowed only if all results have ``passed=True``.
        """
        results: List[GateResult] = []

        for gate_name, threshold in self.config.items():
            if gate_name.startswith("min_"):
                metric_name = gate_name[4:]
                actual = metrics.get(metric_name)
                if actual is None:
                    results.append(GateResult(
                        gate_name=gate_name,
                        passed=False,
                        message=f"Metric '{metric_name}' not found in model metrics.",
                        actual_value=None,
                        threshold=threshold,
                    ))
                else:
                    passed = float(actual) >= float(threshold)
                    results.append(GateResult(
                        gate_name=gate_name,
                        passed=passed,
                        message=(
                            f"{metric_name}: {actual:.4f} "
                            f"{'≥' if passed else '<'} {threshold} (min threshold)"
                        ),
                        actual_value=actual,
                        threshold=threshold,
                    ))

            elif gate_name.startswith("max_"):
                metric_name = gate_name[4:]
                actual = metrics.get(metric_name)
                if actual is None:
                    results.append(GateResult(
                        gate_name=gate_name,
                        passed=False,
                        message=f"Metric '{metric_name}' not found in model metrics.",
                        actual_value=None,
                        threshold=threshold,
                    ))
                else:
                    passed = float(actual) <= float(threshold)
                    results.append(GateResult(
                        gate_name=gate_name,
                        passed=passed,
                        message=(
                            f"{metric_name}: {actual:.4f} "
                            f"{'≤' if passed else '>'} {threshold} (max threshold)"
                        ),
                        actual_value=actual,
                        threshold=threshold,
                    ))

            elif gate_name == "require_drift_check":
                if threshold:
                    drift_passed = (metadata or {}).get("drift_check_passed", False)
                    results.append(GateResult(
                        gate_name=gate_name,
                        passed=bool(drift_passed),
                        message=(
                            "Drift check: passed"
                            if drift_passed
                            else "Drift check: not found or failed in metadata."
                        ),
                    ))
                # if threshold is False, gate is disabled — skip silently

        return results

    def all_passed(
        self,
        metrics: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Return ``True`` only if every configured gate passes.

        Args:
            metrics: Model metrics from :meth:`UbunyeModel.train`.
            metadata: Optional model metadata from :meth:`UbunyeModel.metadata`.
        """
        return all(r.passed for r in self.evaluate(metrics, metadata))

    def failed_gates(
        self,
        metrics: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[GateResult]:
        """Return only the gates that did not pass."""
        return [r for r in self.evaluate(metrics, metadata) if not r.passed]
