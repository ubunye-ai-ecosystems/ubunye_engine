# F-013: a killed run's record says "running" forever

**Status:** fixed on branch fix/rerun-safety (ADR 008)
**Severity:** major
**Source:** experiment E-01 (2026-09-28)
**Promise:** 5

## What happens
The run record is saved as `running` at the start and rewritten at the end. A killed
run never reaches the end, so its record stays `running` for ever. `lineage list`
cannot tell a run in progress from one that died, and nothing tells the next run that
its predecessor was interrupted (which is exactly when F-011 bites).

## Repro
Kill any `--lineage` run; `ubunye lineage list`.

## Expected
The next run of the task (or `lineage list`) can tell an interrupted run from a live
one, marks it `interrupted`, and says so, naming what it may have written.

## Evidence
All 16 killed trials in E-01 left a `running` record next to the rerun's `success`.

## Fix (2026-09-29)
The run that takes over a dead run's lease marks its record `interrupted`, naming itself, and says which outputs may hold part of a batch (ADR 008). E-01 rerun: no `running` record left in 20 trials.
