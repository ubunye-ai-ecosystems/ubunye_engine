# F-031: a second run of a finished batch appends it again

**Status:** open (product decision)
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
