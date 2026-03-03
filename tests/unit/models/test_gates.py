"""Unit tests for PromotionGate and GateResult.

All tests are pure Python — no filesystem or Spark dependency.
"""
from ubunye.models.gates import GateResult, PromotionGate


class TestPromotionGate:

    def test_empty_config_always_passes(self):
        gate = PromotionGate({})
        assert gate.all_passed({"any_metric": 0.5}) is True

    def test_all_min_gates_pass(self):
        gate = PromotionGate({"min_auc": 0.85, "min_f1": 0.80})
        assert gate.all_passed({"auc": 0.90, "f1": 0.87}) is True

    def test_min_gate_fails_below_threshold(self):
        gate = PromotionGate({"min_auc": 0.85})
        assert gate.all_passed({"auc": 0.80}) is False

    def test_min_gate_passes_at_exact_threshold(self):
        gate = PromotionGate({"min_auc": 0.85})
        assert gate.all_passed({"auc": 0.85}) is True

    def test_missing_metric_fails_gate(self):
        gate = PromotionGate({"min_auc": 0.85})
        assert gate.all_passed({"f1": 0.87}) is False  # auc missing

    def test_max_gate_passes_below_threshold(self):
        gate = PromotionGate({"max_loss": 0.5})
        assert gate.all_passed({"loss": 0.3}) is True

    def test_max_gate_fails_above_threshold(self):
        gate = PromotionGate({"max_loss": 0.5})
        assert gate.all_passed({"loss": 0.7}) is False

    def test_max_gate_passes_at_exact_threshold(self):
        gate = PromotionGate({"max_loss": 0.5})
        assert gate.all_passed({"loss": 0.5}) is True

    def test_drift_check_gate_passes_with_metadata(self):
        gate = PromotionGate({"require_drift_check": True})
        assert gate.all_passed({}, {"drift_check_passed": True}) is True

    def test_drift_check_gate_fails_when_false(self):
        gate = PromotionGate({"require_drift_check": True})
        assert gate.all_passed({}, {"drift_check_passed": False}) is False

    def test_drift_check_gate_fails_when_missing(self):
        gate = PromotionGate({"require_drift_check": True})
        assert gate.all_passed({}, {}) is False

    def test_drift_check_disabled_skips_gate(self):
        """require_drift_check: false means the gate is disabled."""
        gate = PromotionGate({"require_drift_check": False})
        assert gate.all_passed({}, {}) is True

    def test_evaluate_returns_list_of_gate_results(self):
        gate = PromotionGate({"min_auc": 0.85, "min_f1": 0.80})
        results = gate.evaluate({"auc": 0.90, "f1": 0.70})
        assert len(results) == 2
        assert all(isinstance(r, GateResult) for r in results)

    def test_evaluate_correct_pass_fail_per_gate(self):
        gate = PromotionGate({"min_auc": 0.85, "min_f1": 0.80})
        results = gate.evaluate({"auc": 0.90, "f1": 0.70})
        by_name = {r.gate_name: r for r in results}
        assert by_name["min_auc"].passed is True
        assert by_name["min_f1"].passed is False

    def test_gate_result_contains_actual_and_threshold(self):
        gate = PromotionGate({"min_auc": 0.85})
        results = gate.evaluate({"auc": 0.90})
        r = results[0]
        assert r.actual_value == 0.90
        assert r.threshold == 0.85

    def test_failed_gates_returns_only_failing(self):
        gate = PromotionGate({"min_auc": 0.85, "min_f1": 0.80})
        failed = gate.failed_gates({"auc": 0.90, "f1": 0.70})
        assert len(failed) == 1
        assert failed[0].gate_name == "min_f1"

    def test_mixed_min_max_gates(self):
        gate = PromotionGate({"min_auc": 0.85, "max_loss": 0.3})
        assert gate.all_passed({"auc": 0.90, "loss": 0.2}) is True
        assert gate.all_passed({"auc": 0.90, "loss": 0.5}) is False

    def test_no_metadata_for_drift_check_fails(self):
        gate = PromotionGate({"require_drift_check": True})
        assert gate.all_passed({}, None) is False
