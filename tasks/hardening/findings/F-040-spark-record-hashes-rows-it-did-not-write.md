# F-040: On Spark, the run record hashes a second computation of an output, not the rows written

**Status:** open
**Severity:** major
**Source:** scale-runner, experiment E-06 (2026-09-29)
**Promise:** 5 (nothing is lost silently) and the run record's purpose (ADR 006: "what was written")

## What happens
On Spark the recorder hashes each output at `task_end`, after the writes, by running
`fingerprint_spark` on the same lazy DataFrame. Nothing is cached, so Spark computes
the output a second time (F-039). When that second computation gives different
rows than the write did, the record carries a digest of rows that were never
written, and says nothing.

Any non-deterministic expression does it. The most common one is an ingest time column:

| `loaded_at` column | record `data_hash` | hash of the written files | same |
|---|---|---|---|
| `current_timestamp()` | `sha256:0294c375...` | `sha256:2afa8ccc...` | **no** |
| `timestamp'2026-09-29 12:00:00'` (control) | `sha256:50cad57f...` | `sha256:50cad57f...` | yes |

(100,000 rows, dev box, Spark 4.2, local. The written files are hashed with the same
`fingerprint_spark` in a fresh session, so the method is identical on both sides.)

Other ways to get there, not run here: a source table that changes between the write
and the hash (another job appends to it, or the task's own `overwrite` output is also
one of its inputs), `rand()` or `uuid()` in a plan Spark re-analyses, or a UDF that
calls a service. On pandas the frame is in memory, so the record hashes exactly what
was written (checked in E-06 at every size: the record's output digest equals the
digest of the written parquet).

## Repro
`e06_loaded_at.py` (in the E-06 folder): a task that adds
`withColumn("loaded_at", F.expr(COLUMN))`, run with `ubunye run --backend spark
--lineage`, then `fingerprint_spark(spark.read.parquet(out))` compared with the
record's `outputs[0].data_hash`.

```
python tasks/hardening/experiments/e06/e06_loaded_at.py /tmp/f040 "current_timestamp()"
python tasks/hardening/experiments/e06/e06_loaded_at.py /tmp/f040 "timestamp'2026-09-29 12:00:00'"
```

## Expected
The record's output hash is the hash of the rows that landed. Two ways there: hash
the frame the writer wrote (persist it for the write and the hash, which also
removes the recompute in F-039), or hash what the writer produced (read back the
files this run wrote; for an append the writer must know its own files, as the
pandas backend does since ADR 008).

## Evidence
Three runs of each on the dev box. Control: the record and the written files agree
in 3 of 3, with the same digest every time (`sha256:50cad57f...`). `current_timestamp()`:
they disagree in 3 of 3, and the record's digest differs from run to run.
