# F-039: On Spark, `--lineage` costs 1.7 times the job at 1M rows and 4.4 times at 50M, and computes every output twice

**Status:** open. Items 1 and 2 fixed (ADR 009, F-041); the 1.5x target is not met. Measured 2026-09-30 below; the next step is experiment E-07 (a native SHA-256 kernel).
**Severity:** major
**Source:** scale-runner, experiment E-06 (2026-09-29)
**Promise:** 7 (the core stays small) and the scale requirement; ADR 006 states a smaller cost than the real one

## What happens
E-06 job, local Spark 4.x (`local[*]`, driver memory 10g), GitHub `ubuntu-latest`
(4 vCPU, 16 GB), Java 21, median of 3 cold runs (E-06 run 36583035813):

| rows | plain s | Ubunye s | `--lineage` s | `--lineage` / plain | record `hash_seconds` |
|---|---|---|---|---|---|
| 1,000,000 | 13.21 | 13.44 | 22.51 | 1.70x | 9.37 |
| 5,000,000 | 17.32 | 17.56 | 38.15 | 2.20x | 20.87 |
| 20,000,000 | 21.59 | 21.76 | 75.82 | 3.51x | 54.45 |
| 50,000,000 | 27.87 | 28.03 | 123.16 | 4.42x | 95.64 |

Ubunye without `--lineage` is within 2% of plain PySpark at every size. With it, the
hash grows about 6 times as fast as the job: the job costs about 0.3 s per extra
million rows, the hash about 1.8 s (at 50M: `events` input 48 s, `detail` output
42 s, `summary` 4.7 s). Nothing is pulled to the driver (E-06); this is time spent
on the executors.

Where the hash time goes:

1. **Every output is computed a second time, and every input is read again.** The
   recorder hashes at `task_end`, after the writes, with `fingerprint_spark` on the
   lazy DataFrames the transform returned; nothing is persisted. Spark's event log:

   | rows | Spark jobs, plain / Ubunye / `--lineage` | input records read, plain / Ubunye / `--lineage` |
   |---|---|---|
   | 1,000,000 | 7 / 7 / 18 | 2,001,800 / 2,001,800 / 5,004,500 |
   | 5,000,000 | 7 / 7 / 18 | 10,001,800 / 10,001,800 / 25,004,500 |
   | 20,000,000 | 7 / 7 / 18 | 40,001,800 / 40,001,800 / 100,004,500 |
   | 50,000,000 | 7 / 7 / 18 | 100,001,800 / 100,001,800 / 250,004,500 |

   `events` is read twice by a plain run (once per output) and five times with
   `--lineage`: once more to hash the input, once more for each output, whose
   filter and join are redone. ADR 006 says "one extra scan of each output". For
   this job the recompute is cheap next to the hash (dev box, 5M rows, median of 3:
   hashing `detail` from its lazy plan 3.70 s, from cached rows 3.31 s); for a plan
   with a wide shuffle, a UDF, a model call or a slow source, the second run costs
   what the first did. It is also the cause of F-040.
2. **Decimal lane arithmetic,** about 40% of the hash itself (F-041).
3. **`to_json` and `sha2` per row,** the rest: the `rows-v1` definition (ADR 006).
   Inherent to that digest, not to Ubunye.

## Repro
`python tests/experiments/e06_scale.py --backend spark --rows 5000000 --repeats 3`
(needs Java 17+ and `pyspark>=4`), or dispatch `.github/workflows/scale-ladder.yml`.
Each JSON line holds `spark.jobs` (per job: call site, time, records read, bytes to
the driver). `tasks/hardening/experiments/e06/e06_spark_hash_parts.py DATA_DIR`
splits recompute from hash.

## Expected
Recording a run costs a bounded share of the run, flat with size. A bound the data
supports: `--lineage` within 1.5 times the plain job at 5M rows and above. Toward
it: hash each output once, from the rows the writer writes (persist for the write
and the hash, or hash in the write's own pass), which also closes F-040; then
F-041. At the least, ADR 006 states the cost as it is.

## After item 1 (ADR 009)
A recorded output is computed once and held; the writer and the hash read the held
rows. Dev box (Windows, 16 GB, Spark 4.2 local, `local[*]`), E-06 job at 5,000,000
rows, 3 runs each, `devbox-f040-before.jsonl` and `devbox-f040-after.jsonl`:

| | before | after |
|---|---|---|
| Spark jobs, `--lineage` | 18 | 17 |
| `events` (5M rows) read from the source, `--lineage` | 5 times | 3 times (the writes: 2, the input hash: 1) |
| wall s, plain, median (min to max) | 15.46 (14.68 to 16.09) | 17.47 (15.68 to 18.08) |
| wall s, `--lineage` | 35.88 (28.09 to 51.29) | 31.93 (31.72 to 34.69) |
| `--lineage` / plain | 2.32x | 1.83x |
| record `hash_seconds` (inputs + outputs) | 12.27, 14.55, 20.96 | 12.36, 11.53, 11.89 |

(The "input records read" total in the JSON lines counts reads of the held blocks
too, so the source scans are counted per job: a job that read 5,000,000 records.)

The target of 1.5x at 5M is not met. What is left is the input hash, a second read of
`events` (F-046), and the hash's own cost per row (F-041). Every output digest is the
same before and after (`sha256:b412fee5...`, `sha256:82f89588...`). The box was noisy:
one old Ubunye run took 26.85 s against 14.95 and 17.61; read the job and scan counts
as the result and the times as a direction.

## Measured after the 2026-09-30 fixes
Scale ladder on GitHub `ubuntu-latest` (4 vCPU), median of 3, at `hardening/real-world`
11823f9 (run 36673389425), against 29 September (run 36632756146, before F-041). Ratio =
`--lineage` wall time over the plain job's.

| backend | rows | 29 Sep | now | record `hash_seconds`, 29 Sep | now |
|---|---|---|---|---|---|
| Spark | 1,000,000 | 1.74x | 1.63x | 8.0 | 6.0 |
| Spark | 5,000,000 | 2.52x | **1.92x** | 16.7 | 12.8 |
| Spark | 20,000,000 | 4.08x | 2.86x | 62.1 | 25.6 |
| Spark | 50,000,000 | 4.80x | **3.78x** | 125.1 | 80.2 |
| pandas | 1,000,000 | 5.72x | 5.70x | 3.2 | 3.3 |
| pandas | 5,000,000 | 9.81x | **6.35x** | 15.7 | 9.6 |
| pandas | 20,000,000 | 9.58x | 6.47x | 51.3 | 32.3 |
| pandas | 50,000,000 | 9.67x | **4.90x** | 156.1 | 71.9 |

Ubunye without `--lineage` is within 0.92x to 1.46x of plain at every size (the small
sizes are start up).

What moved it: outputs computed once (ADR 009), long half lane sums on Spark (F-041),
hashing across cores on pandas (F-038). What is left is the `rows-v1` definition itself:
`to_json` of every row and one SHA-256 per row, plus reading each input again to hash
it (F-046 made that honest, not cheaper).

## Next
Experiment E-07: a native per row SHA-256 kernel that gives the same digest, as an
optional extra (the pure Python path stays the default and the reference), measured
on this ladder. Two cheaper options to measure first: keeping the pandas hash helpers
alive for the whole run, and an opt-in that skips the input content hash when a pinned
Delta version names the input exactly (it changes ADR 006's promise, so opt-in only).

