# F-057: a failed reconcile's hint suggests a severity it cannot have

**Status:** open
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
