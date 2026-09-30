# F-055: a broken expectation looks like a crash in `ubunye run`

**Status:** fixed on example/r2-olist
**Severity:** minor (right exit code and message, wrong impression)
**Source:** example-author, writing Tutorial 2 (Olist), 2026-09-30
**Promise:** a newcomer can tell what went wrong

## What happens
Run a task whose input breaks its contract (Tutorial 2: one purchase time written as
`18/02/2018 10:59`). `ubunye run` prints the right message:

```text
[ERROR] Run failed for clean: An input broke its expectations, so the transform did not run and nothing was written:
  raw_orders: columns (columns): order_purchase_timestamp: expected timestamp, found string
```

then 60 more lines: a boxed traceback through `cli/main.py`, `core/task_runner.py`,
`core/runtime.py` and `core/expectations.py`, ending in the same message again. For a
beginner that reads as "Ubunye crashed", when the engine did exactly what it was asked.
A refused run (`RunLeaseHeld`) already exits without a traceback.

## Repro
`tests/unit/cli/test_run_expectation_stop.py`, or Tutorial 2, step 5.

## Fix
`ubunye run` catches `ExpectationError` (a failed rule, input contract or reconcile),
prints `[ERROR] Run stopped for <task>: <message>` and exits 1. Every other error keeps
its traceback (a bug in a transform needs one). The Python API still raises the
`ExpectationError` with its results, unchanged.

Test: `tests/unit/cli/test_run_expectation_stop.py` (the first test fails before: the
exception was the `ExpectationError`, not a clean exit; the second pins that other
errors still propagate).

## Skeptic review (2026-09-30)

The first fix caught every `ExpectationError`, but two are raised for engine-side
problems, not bad data: narwhals missing (`expectations._nw`) and a reconcile given no
input frames (a notebook or Engine caller bug). Those lost their traceback. Now only
an `ExpectationError` that carries rule results (a verdict on the data: failed rules,
an input contract, a reconcile, too much quarantined) stops quietly; any other keeps
its traceback. Test:
`tests/unit/cli/test_run_expectation_stop.py::test_an_expectation_error_about_the_engine_keeps_its_traceback`
(fails before, passes after).
