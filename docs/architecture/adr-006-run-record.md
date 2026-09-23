# ADR 006: The run record is correct before it is sold

**Status:** accepted, 0.6.0

## Context

`--lineage` leaves a record of every run: what was read, what was written, and
a data hash per output, so `ubunye lineage compare` can say whether two runs
produced the same data. Before 0.6.0 that hash could not carry the claim:

- it read a **1 percent sample**, so most changes were invisible to it;
- on Spark it depended on **row order**, so a repartition changed it;
- on pandas it failed quietly and recorded the **schema hash as the data
  hash**, so any two frames with the same columns "matched";
- runs from `run_task` and the notebook were stored under a **different name
  and folder** from CLI runs, and the notebook recorded nothing at all.

## Decision

**One hash, `rows-v1`, over every row.** Each row becomes one canonical line:
a JSON object with columns sorted by name, written exactly as Spark's `to_json`
writes it (timestamps as UTC text to the microsecond, doubles the Java way, NaN
as `"NaN"`, nulls left out). The line's SHA-256 is cut into two 64-bit numbers
and each is added up over all rows. Adding does not care about order.

| Promise | How |
|---|---|
| Every row counts | All rows are hashed, in the same pass as the row count |
| Row order does not | The per-row hashes are added, and addition is order free |
| Column order does not | Columns are sorted by name first |
| Any change is seen | One different cell changes one line, so the sums |
| Null is not NaN | `null` is left out; NaN is written `"NaN"` |
| Timezone does not matter | Timestamps are written in UTC |
| Same on every engine | Spark runs it as one aggregation; pandas builds the identical line |

On Spark the work stays on the cluster: one `agg` returns three numbers. The
integration tier checks that Spark and pandas give the same hash for the same
table (nested types, NaN, awkward text, a non-UTC session), and that a whole
task run on both engines leaves identical receipts.

**Honest failure.** If the rows cannot be read, the record has no data hash
and says why (`hash_error`). It never substitutes the schema hash.

**A complete record.** Each record also carries the Ubunye version, the
backend, the template variables (`dt`, `dtf`, `mode`), and each output's
`hash_method`.

**One place.** The CLI, `run_task`, `run_pipeline` and the notebook all store
records under `<usecase_dir>/.ubunye/lineage/<usecase>/<package>/<task>/`, with
the same identity, so `ubunye lineage list` finds every run.

## Consequences

- Hashing every row costs one extra scan of each output. On Spark it runs where
  the data is; only three numbers come back.
- Records written before 0.6.0 have no `hash_method`. `lineage compare` calls
  them "not comparable" with new records rather than "changed", and calls two
  missing hashes "unknown" rather than "unchanged".
- The `sample_fraction` setting is ignored and kept only so old configs load.
