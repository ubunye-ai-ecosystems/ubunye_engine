# F-098: `--resume` never skips an overwrite-only task that finished its batch

**Status:** open (target 0.9.1)
**Severity:** minor (the task runs again; the result is the same, the time is not)
**Source:** review (full CLI test run before 0.9.0)
**Promise:** none

## What happens
`docs/cli.md` says that with `--resume`, "tasks that finished the batch are skipped
with `[SKIP]`, the rest run". Only a task with append outputs raises `BatchFinished`
(`ubunye/core/runs.py`), which is what `--resume` turns into a skip. A task that only
overwrites never raises it, so `--resume` runs it again.

## Repro
Run a pipeline of two overwrite tasks for one `-dt`, then
`ubunye run ... --all -dt <same> --resume`: both run again, no `[SKIP]`.

## Expected
Either finished overwrite tasks are skipped too, or the docs say `--resume` skips
only finished tasks that append.

## Why not fixed here
Out of scope for the 0.9.0 release blockers; it changes when a task is considered
finished for every write mode. Planned for 0.9.1.

## Evidence
The 0.9.0 CLI test run; `ubunye/core/runs.py` (BatchFinished raised for appends only).
