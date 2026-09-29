# F-039: On Spark, `--lineage` costs 1.7 times the job at 1M rows and 4.4 times at 50M, and computes every output twice

**Status:** open
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
