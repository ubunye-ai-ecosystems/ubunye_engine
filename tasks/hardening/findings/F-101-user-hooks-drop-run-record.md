# F-101: `run_task(..., hooks=[...])` wrote no run record

**Status:** fixed (0.9.1)
**Severity:** major (a run with `lineage=True` left no record and no error)
**Source:** docs audit (0.9.1)
**Promise:** every run with `lineage=True` writes a run record

## What happened
`ubunye.run_task`, `run_pipeline` and `notebook` passed the caller's `hooks=` to the
engine as `Engine(hooks=...)`, which replaces the default hook set and ignores
`extra_hooks`. The run record recorder is an extra hook, so it was dropped, and so
was the `CONFIG.monitors` hook. The run finished with no record.

## Fix
The three Python entry points now pass the caller's hooks as extra hooks, before the
run record hook. They are added to the engine's own hooks, never in place of them.
`Engine(hooks=...)` itself is unchanged.

## Evidence
`tests/unit/test_user_hooks_keep_engine_hooks.py` (run_task, run_pipeline, notebook,
each with `hooks=[...]` and `lineage=True`): all three fail before the fix (no run
record) and pass after.
