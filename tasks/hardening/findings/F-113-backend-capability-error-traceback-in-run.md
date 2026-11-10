# F-113: a task the backend cannot run prints a traceback box in `ubunye run`

**Status:** fixed (0.9.1)
**Severity:** minor (the message is right, but buried under a traceback)
**Source:** review (0.9.1 CLI pass)
**Promise:** an expected error is one `[ERROR]` line (F-096)

## What happens
`ubunye run --backend pandas` on a task with a `hive` input stops before anything runs
with `BackendCapabilityError` ("This task cannot run on the pandas backend: ..."). The
run command printed `[ERROR] Run failed ...` and then re-raised it, so Rich printed a
full traceback box. The error says what, where and what to try: it is not a bug.

## Repro
```bash
ubunye run -d pipelines -u uc -p pkg -t copy --backend pandas   # copy reads hive
```

## Fix
`run` treats `BackendCapabilityError` as the refusal it is, with the run lease's
refusal: `fail()` from `ubunye/cli/output.py` prints `[ERROR] Run refused for <task>:
...` with the hint, exit 1.

## Evidence
`tests/unit/cli/test_backend_choice.py::TestRun::test_a_task_the_backend_cannot_run_stops_before_it_starts`
now asks for exit 1, a `SystemExit`, the `[ERROR]` line and its hint, no traceback.
It failed before (the `BackendCapabilityError` escaped) and passes after.

## Also seen, not fixed here
`TaskClassMissingError` (no `Task` subclass in `transformations.py`) still prints a
traceback box in `run`; it goes through the generic `except Exception` branch.
