# E-06: Does Ubunye's overhead stay small as data grows, and does anything pull data to one machine?

**Status:** answered (2026-09-29): without `--lineage` passes; with `--lineage` fails on both backends; nothing pulls data to the driver
**Why:** The owner's first requirement: scalable, efficient, distributed. The run record's content hash is the first suspect (pandas: about 0.78 s per 60,000 rows).

## Method
One ETL job, written once (`tests/experiments/e06_logic.py`) and run three ways, each
in a fresh process:

- **plain:** the job in plain pandas or plain PySpark, nothing of Ubunye imported;
- **ubunye:** `ubunye run` of a task whose `transformations.py` calls the same
  function, no `--lineage`;
- **lineage:** the same with `--lineage`;
- **expect** (Spark only, fewer sizes): `ubunye run` of a copy of the task with
  `CONFIG.expectations`, every rule passing, no `--lineage`.

The job: read `events` (N rows: `id`, `region`, `cat`, `amount`, `qty`; `cat` is a
string, the rest integers) and `regions` (900 rows); keep `qty > 0`; derive `revenue` and `big`;
inner join to `regions`; write `detail` (the joined rows, about 0.78 N, append) and
`summary` (18,000 rows grouped by region and category, overwrite). Data is generated
from fixed seeds, the same bytes on every machine. Every number is an integer, so
every run of every variant on every backend must give the same rows. Sizes: 1M, 5M,
20M and 50M rows on both backends.

Compute: GitHub `ubuntu-latest` (4 vCPU, 16 GB), Python 3.12.14, pandas 3.0.6,
pyarrow 25.0.1, numpy 2.5.3, narwhals 2.26.0; Spark: pyspark 4.2.0 `local[*]`,
Temurin Java 21, driver memory 10g, UI off, event log on. One job per backend and
size (`.github/workflows/scale-ladder.yml`), so each rung has a fresh machine;
variants interleaved, 3 repeats each. Runs 36583035813 and 36584681630. The
`expect` variant also ran on the dev box (Windows, Spark 4.2, 2 repeats) at 1M
and 5M.

Measured per run (`tests/experiments/e06_scale.py`): wall time; peak resident memory
of the process tree, Python and JVM apart (psutil, every 50 ms); for `--lineage` the
run record's step timings and `hash_seconds`; on Spark, Spark's own event log (jobs,
their call sites, records read, and the bytes every task sent back to the driver).
Every run's outputs are counted, and the first run of each variant is hashed in full
(`rows-v1`), so the three variants are known to write the same rows.

## Pass means
Overhead under an agreed bound at every size, flat or falling with size; no step
whose time or memory grows on one machine while the backend is distributed.

Bound proposed from the data:
- **without `--lineage`:** within 10% of plain, plus a fixed 0.5 s of start up;
- **with `--lineage`:** within 1.5 times plain at 5M rows and above. One pass over
  each input and output is the least a record of every row can cost; on this job a
  pass that hashes as fast as Spark or pandas writes parquet would cost about half
  the job.

## Result
Median of 3 (min to max), seconds; peak memory of the process tree, MB.

| backend | rows | plain s | Ubunye s | `--lineage` s | Ubunye / plain | `--lineage` / plain | record `hash_seconds` | peak MB plain / Ubunye / `--lineage` |
|---|---|---|---|---|---|---|---|---|
| pandas | 1,000,000 | 0.81 (0.80-0.87) | 1.10 (1.08-1.12) | 4.39 (4.37-4.40) | 1.36x | 5.43x | 3.28 | 407 / 376 / 382 |
| pandas | 5,000,000 | 1.88 (1.88-1.89) | 2.36 (2.36-2.36) | 18.22 (18.11-18.31) | 1.25x | 9.67x | 15.87 | 1,363 / 1,166 / 1,169 |
| pandas | 20,000,000 | 6.09 (6.04-6.12) | 6.99 (6.92-7.05) | 70.14 (69.15-70.17) | 1.15x | 11.52x | 63.15 | 4,609 / 3,815 / 3,683 |
| pandas | 50,000,000 | 16.09 (15.63-18.40) | 16.73 (16.48-16.92) | 171.78 (171.53-172.57) | 1.04x | 10.67x | 155.48 | 12,189 / 9,083 / 9,200 |
| spark | 1,000,000 | 13.21 (13.14-16.25) | 13.44 (13.42-13.49) | 22.51 (22.49-22.86) | 1.02x | 1.70x | 9.37 | 1,098 / 1,112 / 1,446 |
| spark | 5,000,000 | 17.32 (17.21-17.48) | 17.56 (16.51-18.01) | 38.15 (37.05-38.60) | 1.01x | 2.20x | 20.87 | 1,890 / 1,956 / 3,082 |
| spark | 20,000,000 | 21.59 (21.52-22.31) | 21.76 (21.74-23.26) | 75.82 (75.81-77.33) | 1.01x | 3.51x | 54.45 | 2,229 / 1,958 / 2,331 |
| spark | 50,000,000 | 27.87 (27.34-38.70) | 28.03 (28.03-29.52) | 123.16 (122.78-124.66) | 1.01x | 4.42x | 95.64 | 2,380 / 2,050 / 2,533 |

(pandas 50M from run 36584681630, the rest from run 36583035813.)

What reached the driver on Spark, per run (Spark's event log: every task's result;
the Python driver's and the JVM's peak memory):

| rows | task results to the driver, plain / Ubunye / `--lineage` | largest single job | Python driver peak MB | JVM peak MB (driver and executors) |
|---|---|---|---|---|
| 1,000,000 | 57 / 57 / 131 kB | 13.7 kB | 120 / 137 / 138 | 849 to 1,415 |
| 5,000,000 | 65 / 65 / 147 kB | 15.5 kB | 119 / 137 / 138 | 1,769 to 3,001 |
| 20,000,000 | 65 / 64 / 147 kB | 15.5 kB | 121 / 137 / 139 | 1,817 to 2,238 |
| 50,000,000 | 65 / 64 / 147 kB | 15.5 kB | 119 / 137 / 138 | 1,896 to 2,813 |

All three variants wrote the same rows at every size (row counts every run, full
`rows-v1` hash of the first run of each), and pandas and Spark gave the same digests
for the same size (`detail` at 50M: `sha256:8b4a5124...` on both). On both backends
the `--lineage` record's output digests equal the digests of the written files (this
job is deterministic; F-040 is the case where they do not).

**Without `--lineage`: pass.** Spark is within 2% of plain PySpark at every size.
pandas is 1.36x at 1M, 1.25x at 5M, 1.15x at 20M, 1.04x at 50M: about 0.25 s of
start up plus a little per row, which is a slower `merge` on the Arrow backed frames
the pandas backend hands the transform (F-042). Falling with size, inside the bound.
Ubunye's pandas runs use less memory than plain pandas (9.1 GB against 12.2 GB at 50M).

**With `--lineage`: fail, and the overhead grows with size on both backends.**
pandas: 5.4x at 1M, 9.7x at 5M, then about 11x (11.5x at 20M, 10.7x at 50M). The
hash runs in one Python thread and grows about 3.1 s per million input rows (inputs
and outputs hashed), against 0.3 s per million for the job (F-038). Spark: 1.70x,
2.20x, 3.51x, 4.42x. The hash grows about 1.8 s per million rows against 0.3 s for
the job; each output is computed a second time and
each input read again (records read 2.5 times plain's, F-039), 40% of the hash is
decimal arithmetic (F-041), the rest is `to_json` plus SHA-256 per row, which the
`rows-v1` definition asks for. Because the output is computed again rather than
taken from what was written, the Spark record can hash rows that were never written
(F-040, shown with `current_timestamp()`).

**Does anything pull data to one machine: no.** All task results sent to the driver
in a run: 57 to 65 kB plain and Ubunye, 131 to 147 kB with `--lineage`, the same from
5M to 50M rows (it follows the number of tasks, not rows). The Python driver's peak
memory is flat at every size (about 120 MB plain, 137 MB Ubunye with or without
`--lineage`). The JVM, which in local mode holds the executors too, has no trend
from 5M to 50M rows under a fixed 10g heap. In the code: `fingerprint_spark`
collects one row of three numbers; expectations collect one row of counts per check
(`_scalars`); the OpenTelemetry hook counts rows only on a backend that is not
distributed; the run record is a small JSON file. Expectations on Spark (dev box,
`expect` variant) also sent only task metadata to the driver (154 kB at 1M, 175 kB
at 20M), but each check is another full computation of the output before the write:
1.36x plain at 1M and 1.54x at 5M on the dev box, 1.47x at 20M on GitHub (run
36584681630), with 18 Spark jobs against 7 (F-043).

Findings: F-038 (pandas `--lineage` cost grows with size), F-039 (Spark `--lineage`
cost grows with size; outputs computed twice), F-040 (Spark record can hash rows it
did not write), F-041 (Spark hash: decimal lanes), F-042 (pandas: slow merge on
Arrow backed frames), F-043 (Spark expectations compute the output again).

Raw numbers in `tasks/hardening/experiments/e06/`: `gha-36583035813/` and
`gha-36584681630/` (one JSON line per run, with Spark's jobs), `devbox-expect.jsonl`,
and `e06-runs.csv` (every run, flattened). The probes behind the findings are there
too: `e06_loaded_at.py` (F-040), `e06_spark_hash_parts.py` (F-039),
`e06_lanes.py` (F-041), `e06_pandas_ops.py` (F-042).

Rerun: push a change of `tests/experiments/e06_*.py` or the workflow to an `exp/`
branch (`tests/experiments/e06_only.txt` picks rungs; empty means all), or, once the
workflow is on `main`, `gh workflow run scale-ladder.yml --ref <branch>`.

## Not measured
- **A real cluster.** Local Spark puts driver and executors in one JVM, so JVM memory
  cannot be split between them; the evidence that nothing is collected is Spark's
  own count of bytes sent to the driver. Databricks serverless: not run (no existing
  workflow could be reused cheaply for this job).
- **The ML paths.** `ubunye/models/base.py` and `ubunye/plugins/ml/adapters.py` call
  `toPandas()` on the whole frame; that pulls data to the driver by design and is not
  part of this ETL job.
- **Other formats and writers:** Delta, JDBC, CSV, partitioned writes, `merge`.
- **Telemetry on** (`UBUNYE_TELEMETRY=1`): all runs used the default, off.
- **Out of memory:** no rung ran out. Plain pandas peaked at 12.2 GB of the runner's
  16 GB at 50M rows (Ubunye 9.1 GB, its frames are Arrow backed), so about 60M rows
  is where plain pandas would stop on this runner; not run. Spark at 50M rows took
  124 s with `--lineage` and was not pushed further.
- **Cold caches:** each run is a new process, but the operating system's file cache
  holds the data after generation.
