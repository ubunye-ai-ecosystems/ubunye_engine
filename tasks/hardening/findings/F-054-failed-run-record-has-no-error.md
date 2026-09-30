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

## Skeptic review (2026-09-30)

The first fix leaked. Its masking knew only secret-looking `--var` values, word for
word. The skeptic's probe (`p1_error_leaks.py`) put secrets in a transform's error:

| Case | before (45d435c) | after |
|---|---|---|
| `--var db_password`, word for word | masked | masked |
| env var templated into `jdbc:...;password=` | kept | `password=***` |
| `postgresql://admin:pw@db` (literal) | kept | `admin:***@db` |
| `?api_key=` (literal) | kept | `api_key=***` |
| `Authorization: Bearer x` (literal) | kept | `Bearer ***` |
| a resolved `secret://env/...` value | kept | `***` |
| `--var` secret URL-encoded | kept | `***` |
| `--var` secret in base64 of `u:<secret>` | kept | `dTp***Q==` |
| `--var` secret broken across a line | kept | `***` |
| a 3 character `--var` value | kept | kept (documented: too short to mask word for word) |

Fix: one function, `ubunye.core.secrets.mask_text(text, values)`, used by the run
recorder, by the monitor hook (so every monitor's `task_end(error=...)` gets masked
text) and by the REST connector's F-050 `redact`. The values it is given
(`known_secrets`): secret-looking variables, every value a `SecretResolver` returned in
the process, the config's literal secrets, and environment variables with
secret-looking names. It masks each value, its URL-encoded and base64 forms (the part
of the base64 that does not depend on the bytes around it), and the value split by
whitespace; plus URL userinfo, secret-named URL and JDBC parameters (including `key`
and `sig`), and `Authorization` / `Proxy-Authorization` / `X-Api-Key` header values.

Tests: `tests/unit/core/test_mask_text.py` (27; every row above, on `mask_text` and
through a real run). Before the fix the file does not import (`mask_text` missing).

Also from the review: a 5,000,000 character message made a 5,002,957 byte record.
The error is now cut to 4,096 characters plus `... (N more characters)` (masked
first, so a cut cannot show half a secret); the probe's record is now a few KB.
Tests: `tests/unit/lineage/test_record_error.py::test_a_huge_error_is_cut_to_about_4_kb_in_the_record_and_openlineage`
and `::test_the_cut_never_shows_half_a_secret` (both fail before, pass after).
