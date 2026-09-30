# F-057: a failed reconcile's hint suggests a severity it cannot have

**Status:** fixed on example/r2-olist
**Severity:** minor (wrong advice in an error message)
**Source:** example-author, writing Tutorial 2 (Olist), 2026-09-30

## What happens
When a reconcile fails (Tutorial 2, step 6: an inner join loses an order), the error
ends with the hint every failed output rule gets:

```text
  orders_fact: rows_from_orders (reconcile): 60 rows read from orders, 59 reached orders_fact: 1 lost, ...
  Hint: Fix the data or the source, or change the rule's severity to quarantine
  or warn if this is expected.
```

A reconcile cannot be `quarantine` (the config refuses it: a reconcile is about the
whole output). A reader who follows the hint gets a config error next.

## Repro
Tutorial 2, step 6, or `tests/unit/examples/test_olist_example.py::test_a_join_that_loses_orders_stops_the_run_and_writes_nothing`
(the message is in `err.value`).

## Expected
When only reconciles (or `unique` / `row_count`, which cannot quarantine either)
failed, the hint names what is possible: fix the transform or the source, allow a
share (`max_lost: "1%"`), or `severity: warn`.

## Where
The hint is built in `ubunye/core/expectations.py` (`apply`), one text for every
failure.

## Fix
`expectations._failure_hint` builds the hint from the kinds of the `fail` rules that
broke: quarantine or warn for a rule on rows (`not_null`, `between`, `one_of`,
`matches`); for a reconcile, look for a join or filter that loses rows, allow a share
or warn, and "a reconcile cannot quarantine"; warn for `unique`, `row_count` and
`columns`; and "the source probably changed" when `max_quarantine_rate` is broken.
Tutorial 2, step 6 now prints:

```text
  Hint: A reconcile: look for a join or filter that loses rows, or a source that changed; if the loss is expected, allow a share (max_lost: "1%") or set severity: warn. A reconcile cannot quarantine.
```

Test: `tests/unit/core/test_expectation_hint.py` (4; 3 fail before, the row-rule one
passes before and after, as it should).
