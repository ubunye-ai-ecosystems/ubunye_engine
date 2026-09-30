"""A failed expectation's hint names only what that rule can do (F-057).

Every failure said "change the rule's severity to quarantine or warn", but a
reconcile, `unique`, `row_count` and `columns` cannot quarantine: the config refuses
it. A reader who followed the hint met a config error next.
"""

from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

from ubunye.config.schema import ExpectationSet  # noqa: E402
from ubunye.core import expectations  # noqa: E402
from ubunye.core.errors import ExpectationError  # noqa: E402

ORDERS = pd.DataFrame({"order_id": [1, 2, 3], "amount": [10, 0, 5]})


def _hint(spec, out, measured=None):
    with pytest.raises(ExpectationError) as err:
        expectations.apply({"out": out}, {"out": spec}, measured=measured)
    return err.value.hint


def test_a_reconcile_failure_does_not_suggest_quarantine():
    spec = ExpectationSet(reconcile=[{"input": "orders", "rows": {"max_lost": 0}}])
    measured = {"orders": {"rows": 3, "sums": {}}}
    hint = _hint(spec, ORDERS.head(2), measured)
    assert "quarantine or warn" not in hint
    assert "max_lost" in hint and "cannot quarantine" in hint


def test_a_unique_failure_does_not_suggest_quarantine():
    spec = ExpectationSet(rules=[{"unique": "order_id"}])
    hint = _hint(spec, pd.concat([ORDERS, ORDERS]))
    assert "quarantine or warn" not in hint
    assert "severity: warn" in hint and "cannot quarantine" in hint


def test_a_row_rule_failure_still_suggests_quarantine_or_warn():
    spec = ExpectationSet(rules=[{"between": {"column": "amount", "min": 1}}])
    hint = _hint(spec, ORDERS)
    assert "quarantine or warn" in hint
    assert "max_lost" not in hint


def test_too_much_quarantined_says_the_source_changed():
    spec = ExpectationSet(
        quarantine="bad",
        max_quarantine_rate=0.1,
        rules=[{"between": {"column": "amount", "min": 1}, "severity": "quarantine"}],
    )
    hint = _hint(spec, ORDERS)
    assert "max_quarantine_rate" in hint
