# F-014: `--lineage` makes a 1M-row run 8.5 times slower

**Status:** open
**Severity:** major
**Source:** experiment E-01 (2026-09-28)
**Promise:** 7 (the core stays small) and the scale requirement (E-06)

## What happens
The same task, 1,000,000 rows, one input and two outputs, pandas backend, on the dev
box: 1.61 s without `--lineage`, 13.62 s with it (12.89 s with telemetry off). The run
record itself says the task took 0.70 s (read 0.35, transform 0.01, two writes of 0.16);
the rest is hashing inputs and outputs for the record, about 4 s per million rows, in
one Python process, after the writes. That time is not in the record, and it is the
window in which F-011 happens.

## Repro
`tests/experiments/timing.py` against the E-01 task.

## Expected
Hashing costs a small fraction of the run, is itself timed in the record, and does not
grow on one machine when the backend is distributed. Measure on Spark too (E-06).

## Evidence
Timings above. Earlier: 0.78 s per 60,000 rows (0.6.0), 2.4 times faster in 0.7.0.
