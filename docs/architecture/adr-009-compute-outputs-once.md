# ADR 009: Each output is computed once, and every consumer gets that copy

**Status:** accepted (hardening programme, 2026-09-29)

## Context

On Spark a DataFrame is a plan, not rows. Every action on it runs the plan again.
The engine acts on each output up to three times: the expectation checks, the
write, and the run record's hash at task end. So:

- **The record could hash rows that were never written (F-040).** A task that adds
  `loaded_at = current_timestamp()` got a record digest that matched the written
  files in 0 of 3 runs. Anything that differs per computation does it: a clock, a
  UDF that calls a service or a model, a source another job changes.
- **The checks could pass rows the writer never wrote.** Same cause: the checks were
  one computation, the write another.
- **Every consumer paid for the transform again (F-039, F-043).** With `--lineage` or
  expectations, the E-06 job read its 5 million row source 5 times instead of 2.

On pandas none of this happens: the frame is the rows.

## Decision

**Hold each output that has more than one consumer.** After the transform returns
and before any check runs, the engine asks the backend to compute each such output
once (`Backend.materialise`). The checks, the quarantine split, the writer and the
record's hash all get that one frame. An output is held when:

- the run is recorded (a hook or monitor says `reads_outputs`, as the lineage
  recorder does), or
- the output has `CONFIG.expectations`, or
- the same frame is written under two output names.

Otherwise nothing changes: a plain run, with no record and no expectations, is
exactly what it was (E-06: 1.01x plain Spark). Holding is a timed step
(`Materialise`, one per output) in the record's `timings`, so its cost is not hidden.

**The mechanism is `localCheckpoint(eager=True)`, with `MEMORY_AND_DISK`.** It computes
the rows once and cuts the plan, so a later action reads the held rows and nothing
else. `persist()` was measured against it
(`tasks/hardening/experiments/e06/e06_materialise.py`, one output with
`current_timestamp()`, `rand()` and a Python UDF that answers differently each call,
1,000,000 rows, dev box, Spark 4.2 local):

| Question | `persist` + `count()` | `localCheckpoint(eager=True)` |
|---|---|---|
| Record digest equals the written files | yes | yes |
| A held block is lost before the write (what losing an executor does) | recomputes quietly: the rows checked are **not** the rows written | fails loudly: `CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND` |
| The output overwrites its own input | the write refreshes the cache from the new files: the record hashes rows that were **not** written | correct: record equals written files |
| Seconds to hold, write, hash | 16.5, 1.6, 1.9 | 16.8, 1.0, 1.5 |
| Bytes held on the executors | 43 MB (compressed columns) | 148 MB (rows), 3.4 times more |
| A frame cut from it, used after release | works (recomputes) | fails |

The property this change exists for is that later actions never silently compute
the plan again. `persist` breaks it in two of the cases above; `localCheckpoint` never
does. It costs more executor memory and disk, and a lost executor now fails the run
where `persist` would have gone on with other rows. We take that: a run that fails
says so, a record that describes other rows does not.

**Released after the hooks.** Held frames are released when the task ends, after
the run record is written, on success and on failure (`finally`). The frames the
engine returns to the caller (`run_task`) are never the held ones, which stop
working once released: the caller gets the frames the transform returned, cut again
by the same rule results where expectations quarantined rows. They are lazy, as
before.

**When it cannot be done, the run goes on as before and the record says so.** Each
input and output step in the record has `hash_basis`:

| `hash_basis` | Meaning |
|---|---|
| `materialised` | The digest is of the rows that were written: a held Spark output, or a pandas frame (always in memory) |
| `recomputed` | The frame was computed again for the hash: an output that could not be held, or any Spark input |

A backend that cannot hold a frame (Spark Connect or serverless may refuse, a
streaming frame, a test double) returns `None`, and the run goes on as before. A
backend that hands back the frame it was given has not held it, and the engine
treats it as `None`. A quarantine output has the basis of the output it was cut
from. A record written before this has no `hash_basis`.

**A job that fails while it is held fails the task, once.** Holding is the first
computation of the output, so a failing transform (a UDF that raises, a service that
returns 500) fails there. The backend raises that error and the engine lets it end
the task, as the writer would. It does not fall back: the fallback computes the same
failing plan again, so every side effect of the transform happened twice before the
same error (the skeptic measured 6,000 UDF calls against 3,000 before this). The
Spark backend falls back only on a platform refusal (`NotImplementedError`, or an
error that says `NOT_SUPPORTED`, `NOT_IMPLEMENTED`, `UNSUPPORTED` or "not supported"
and is not a failed job); any other error is raised. It also checks that the plan it
returns is a `LogicalRDD`, the held rows, and returns `None` if not.

**One switch.** `UBUNYE_MATERIALISE_OUTPUTS=0` (also `false`, `no`, `off`) turns
holding off, read when each run starts. Every Spark output is then `recomputed`, and
each consumer computes it, as before this change. Use it when an output is too large to hold
in executor memory and local disk. It is on by default.

**Inputs are not held.** The input hash is still a separate scan of the source, so a
source that changes between the read and the hash gives an input digest of the later
state; every Spark input step says `recomputed` (F-046).

## Measured (E-06 job, 5,000,000 rows, dev box, Spark 4.2 local, 3 runs each)

The job reads `events` (5M rows) and `regions`, writes `detail` (3.9M rows, append)
and `summary` (18,000 rows, overwrite).

| Variant | Spark jobs, before / after | `events` scans, before / after | Wall s, median (min to max), before | after |
|---|---|---|---|---|
| plain PySpark | 7 / 7 | 2 / 2 | 15.46 (14.68 to 16.09) | 17.47 (15.68 to 18.08) |
| Ubunye | 7 / 7 | 2 / 2 | 17.61 (14.95 to 26.85) | 18.73 (17.24 to 19.18) |
| `--lineage` | 18 / 17 | 5 / 3 | 35.88 (28.09 to 51.29) | 31.93 (31.72 to 34.69) |
| expectations | 18 / 16 | 5 / 2 | 26.87 (23.72 to 36.82) | 25.56 (25.48 to 27.43) |

As a share of plain in the same batch: `--lineage` 2.32x before, 1.83x after;
expectations 1.74x before, 1.46x after. The box was noisy (a plain run's spread is
about 2 seconds, an old Ubunye run took 26.85 s), so read the job and scan counts as
the firm result and the times as a direction. The written data and every recorded
output digest are the same before and after. With `--lineage`, the remaining scan of
`events` is the input hash (F-046); the rest of the cost is the hash itself (F-041).

## Consequences

- F-040 is closed where holding works: the `current_timestamp()` repro now matches
  3 of 3 (was 0 of 3). The integration tier checks it on live Spark
  (`tests/integration/test_materialise_spark.py`).
- A recorded or checked output costs its size in executor memory, spilling to local
  disk, from the transform to the end of the task; all such outputs are held at
  once. Plan disk for the largest outputs, or switch it off.
- A lost executor during the write of a held output fails the run
  (`CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND`). Spark retries tasks, not lost checkpoints.
- An output that overwrites its own input now succeeds when held, because the rows
  are computed before Spark deletes the source. Unheld, Spark deletes the source and
  then fails reading it (F-047, open).
