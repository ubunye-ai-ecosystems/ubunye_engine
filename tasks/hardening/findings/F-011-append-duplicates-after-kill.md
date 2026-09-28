# F-011: a killed run's append is appended again by the rerun

**Status:** open
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
