# F-040: On Spark, the run record hashes a second computation of an output, not the rows written

**Status:** fixed on fix/f040-spark-persist (2026-09-29, ADR 009), awaiting skeptic review and merge into hardening/real-world
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

## Fix (ADR 009)
Each output that the record, its expectations or a second output name acts on is
computed once, before the checks, with `localCheckpoint(eager=True)` (memory and
disk), and the checks, the writer and the record's hash all get that frame. It is
released after the hooks, on success and on failure. `persist` was measured and
rejected: after a lost block it recomputes quietly, and when the output overwrites
its own input the write refreshes the cache, so the record hashed rows that were not
written (`e06_materialise.py`, `devbox-materialise-1m.jsonl`).

Where holding is impossible (Spark Connect or serverless refusing it, a streaming
frame), the run goes on as before and the record says so: every step has
`hash_basis`, `materialised` or `recomputed`. `UBUNYE_MATERIALISE_OUTPUTS=0` turns it off.

## After
Same repro, dev box, Spark 4.2 local, three runs each:

| `loaded_at` | record equals written files, before | after |
|---|---|---|
| `current_timestamp()` | 0 of 3 | **3 of 3** (a new digest each run, each equal to its files; `hash_basis: materialised`) |
| `timestamp'2026-09-29 12:00:00'` (control) | 3 of 3, `sha256:50cad57f...` | 3 of 3, the same `sha256:50cad57f...` |
| `current_timestamp()` with `UBUNYE_MATERIALISE_OUTPUTS=0` | | 0 of 1, `hash_basis: recomputed` (the switch says so) |

Pinned by `tests/integration/test_materialise_spark.py` (live Spark: record equals
written files, quarantine too, held rows released, a lost block fails loudly) and
`tests/unit/core/test_materialise_outputs.py` (15 of its 19 tests fail on the old code).

Still open around it: an input's hash is a second read of the source (F-046), and an
output that overwrites its own input deletes it on a plain run (F-047).
