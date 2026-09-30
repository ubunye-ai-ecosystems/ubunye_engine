# F-031: a second run of a finished batch appends it again

**Status:** fixed on hardening/real-world (ADR 008 addendum, 2026-09-29)
**Severity:** major
**Source:** experiment E-01 rerun (2026-09-29)
**Promise:** 5 (nothing is lost or doubled silently)

## What happens
A run appends dt=2 and finishes: record `success`, lease released. Running dt=2 again
appends the batch a second time. The lease (ADR 008) cannot help: it only exists while
a run lives.

## Repro
E-01 harness, trial 17 of 20: the kill landed at 12.61 s, after the run had finished
(its record already said `success`). The "rerun" was a second full run; `events` held
dt=2 twice.

## Expected
Unclear, and that is the point: skip a finished batch (Pramen's default), replace it
(`overwrite_partitions`, F-012), or refuse unless `--force`. Each is reasonable; the
engine should pick one and say so.

## Options
1. Refuse: a finished (task, variables) with `success` in the lineage is refused unless
   `--rerun`. Cheap, needs lineage on.
2. Replace: route appends through partition overwrite by batch (F-012). Most correct,
   most work.
3. Document only: say appends are at least once for deliberate reruns.

## Decision and fix
Refuse by default, `--rerun` replaces (ADR 008 addendum, which says why not skip).
Before: the probe (a pandas append task, `dt=2024-01-02` run twice) ended with 4 rows.
After: the second run is refused (`BatchFinished`), 2 rows; with `rerun=True`, 2 rows
and none of the first run's files. Tests: `tests/unit/core/test_finished_batch.py`.

Two skeptic rounds shaped it. Round 5 proved a --rerun taken over at commit deleted
both runs' files (commit now bails before touching anything once the lease is lost);
an overwrite deleting the note (overwrites are now noted); pipelines that could not
resume. Round 6 proved a locked note led a later --rerun to double (the lease is now
kept and nothing replaced until the note is written); skip-by-default dropping an hourly
pipeline's rows with exit 0 (resume is now `--resume`, asked for by name); dropping
`dtf` from the batch key stranded old leases (reverted). Each has a test.

Measured again (2026-09-29, 1M rows, pandas): E-01 20/20 right (18 killed and rerun
once, 2 had finished and were refused on rerun); E-02 0/10 doubled.
