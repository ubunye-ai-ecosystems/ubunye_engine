# F-011: a killed run's append is appended again by the rerun

**Status:** fixed on branch fix/rerun-safety (ADR 008)
**Severity:** blocker
**Source:** experiment E-01 (2026-09-28)
**Promise:** 5 (nothing is lost or doubled silently)

## What happens
A run that appends a batch is killed after the append is committed but before the
run is recorded as finished. The rerun cannot tell the batch is already there and
appends it again: the output holds the batch twice, and nothing says so.

## Repro
E-01 harness (tests/experiments/e01_crash.py): 1,000,000 rows, one `overwrite` and one `append` output,
pandas backend, `--lineage`. Start from dt=1 written once; start dt=2, hard-kill it at
a random moment, rerun dt=2.

## Expected
After any kill plus one rerun, `events` holds dt=1 once and dt=2 once.

## Evidence
16 of 16 trials (kills between 2.4 s and 11.7 s into a 12.8 s run) ended with dt=2
twice: 2,000,000 rows instead of 1,000,000. In every trial the append was already
committed at the kill. The window is most of the run because the run spends about
90% of its time hashing for the run record after the writes (F-014). The `overwrite`
output was correct in all 16. Plain Spark `append` has the same property by design;
the difference is that Spark users have `overwrite_partitions` and pandas users do not
(F-012).

## Fix (2026-09-29)
A lease per batch. The pandas backend claims each part file in the lease before it lands; the run that takes over a killed run removes exactly the claimed files, then runs the batch once (ADR 008). Spark appends are named in the record, not repaired: on Spark use overwrite, overwrite_partitions or Delta. (Since F-070, 2026-09-30, a Spark append to a path on a local or shared disk is claimed and taken back too; Delta, JDBC and object storage are still named only: F-071, F-072, F-073.) E-01 rerun: all 18 trials that were killed came back right; the 2 wrong trials were never killed (the run had finished), which is F-031.
