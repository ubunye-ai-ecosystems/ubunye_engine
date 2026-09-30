# F-054: a failed run's record does not say why it failed

**Status:** fixed on example/r2-olist
**Severity:** major (the receipt of a failed run is silent on the one thing asked)
**Source:** example-author, building R2 (Olist), 2026-09-30
**Promise:** the run record is the receipt

## What happens
Run a task with `--lineage` whose transform raises. The record says
`"status": "error"` and `"error": null`. The field exists (`RunContext.error`), the
OpenLineage FAIL event would send it as `errorMessage` (the docs promise it), and
`ubunye prove` uses it as the reason a run failed. Nothing filled it, except for the
lease case (ADR 008). So a failed cloud run in the proving ground, where the log may be
gone, shows no reason at all.

## Repro
Any failing task, pandas backend:

```python
import ubunye
ubunye.run_task("pipelines/olist/sales/clean", backend="pandas", lineage=True)  # raises
# pipelines/.ubunye/lineage/olist/sales/clean/<run>.json: "status": "error", "error": null
```

Seen building R2: a Narwhals `AttributeError` in the transform left a record with no
error text.

## Cause
`MonitorHook` (`ubunye/telemetry/hooks/monitors.py`) called `task_end(status="error")`
without the exception, and `LineageRecorder.task_end` had no parameter for it.

## Fix
The hook passes `error="<Type>: <message>"` to a monitor whose `task_end` takes
`error` (older monitors are called as before). The recorder keeps it, with the values
of secret-looking variables masked by the same `scrub` that masks step locations
(F-016, F-050). `lineage trace` prints it, and `--json` carries it.

Test: `tests/unit/lineage/test_record_error.py` (fails before: `assert None is not
None`; passes after), including a secret variable's value in the message that the
record must not keep.
