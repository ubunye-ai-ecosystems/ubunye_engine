# E-01: A power cut or kill in the middle of writing: is the output half written, does the run record say so, and does a rerun repair it or make it worse?

**Status:** answered (2026-09-28): fails
**Why:** Pramen (AbsaOSS) writes its bookkeeping last and repairs offsets from what was actually written, because partial writes and reruns corrupted tables in production. Load shedding makes this the first question for users here.

## Method
Kill the process at random points during write, for each writer (parquet folder, csv, delta, jdbc) and each mode (overwrite, append, merge, overwrite_partitions); then rerun. Compare with a clean run.

## Pass means
After any kill plus one rerun, the output equals a clean run's output, and no run record claims success for a run that did not finish.

## Result
Run on the pandas backend, parquet, 1,000,000 rows, hard kills (taskkill /F /T) at
uniform random moments, 16 trials completed (the 40-trial run was stopped by the dev
box's memory limit; 16 of 16 agree).

| Output mode | Correct after kill + rerun | Missing right after kill | Debris left |
|---|---|---|---|
| overwrite (`snapshot`) | 16 of 16 | 0 | none |
| append (`events`) | 0 of 16: batch doubled | 0 | none |

Every killed run left a `running` record. The rerun-safe mode (`overwrite_partitions`)
is refused on pandas. The run spends about 90% of its time hashing after the writes.

Findings: F-011 (append doubled), F-012 (no rerun-safe incremental write on pandas),
F-013 (killed run stays `running`), F-014 (run record hashing cost).

Not yet covered: Spark backend, delta, jdbc, merge; a kill inside the overwrite swap
(its window is two renames; not hit in 16 trials); an operating-system crash (this
kills the process, the disk cache survives).

## Rerun after the fix (2026-09-29, branch fix/rerun-safety, claim design)
20 trials, same harness. 18 were killed; all 18 came back exactly right (snapshot and
events), no debris, every killed run's record `interrupted`, none left `running`. The
2 trials the harness could not kill (the run finished first) were then run a second
time and appended dt=2 again: a finished batch, not a crash (F-031). An earlier
design scored the same on this harness but deleted other runs' files in a shared
folder (adversarial review); passing E-01 is not evidence of that safety, the unit
tests are. Output in the session scratchpad (e01-v3.txt).

Final run on the shipped design (after four adversarial reviews): 20 of 20 trials
killed, 20 of 20 exactly right, no debris, no `running` record (e01-v5.txt). E-02 on the
same build: 0 of 10 doubled (e02-v5.txt).

## `events` with `overwrite_partitions` on pandas (2026-09-29, F-012)
Same harness, `EVENTS=partitions`: `events` written with `mode: overwrite_partitions`,
`partitionBy: [batch]`, which the pandas backend can do since F-012. 10 trials, 10
killed, 10 of 10 exactly right after the rerun (snapshot and events: batch 1 and
batch 2 once each), no debris, no `running` record left. The rerun is not refused
(an overwrite is already rerun safe) and replaces `batch=2`. Every kill in this run
landed after the events write (in the run record hashing, about 90% of the run), so
a kill inside the partition swap itself was not hit; that path is covered by a unit
test that fails the second leaf's swap and checks every partition is put back.
Output in the session scratchpad (e01-partitions.txt).
