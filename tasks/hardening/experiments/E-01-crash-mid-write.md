# E-01: A power cut or kill in the middle of writing: is the output half written, does the run record say so, and does a rerun repair it or make it worse?

**Status:** planned
**Why:** Pramen (AbsaOSS) writes its bookkeeping last and repairs offsets from what was actually written, because partial writes and reruns corrupted tables in production. Load shedding makes this the first question for users here.

## Method
Kill the process at random points during write, for each writer (parquet folder, csv, delta, jdbc) and each mode (overwrite, append, merge, overwrite_partitions); then rerun. Compare with a clean run.

## Pass means
After any kill plus one rerun, the output equals a clean run's output, and no run record claims success for a run that did not finish.

## Result
Not run yet.
