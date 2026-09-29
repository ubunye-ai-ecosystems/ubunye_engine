# F-012: the pandas backend has no rerun-safe way to write incremental data

**Status:** fixed on branch `fix/pandas-partitions` (2026-09-29)
**Severity:** major
**Source:** experiment E-01 (2026-09-28)
**Promise:** 1 (same task anywhere) and 5

## What happens
The rerun-safe pattern for daily data is to replace the day's partition
(`mode: overwrite_partitions`, `partitionBy: [dt]`). On the pandas backend it is
refused: "the pandas backend can do append, errorifexists, ignore, overwrite." So the
backend meant for laptops and small machines, where power cuts are most likely, can
only append (duplicates on a rerun, F-011) or overwrite the whole table.

## Repro
Any task with an output `mode: overwrite_partitions`, `partitionBy: [batch]`, run with
`--backend pandas`.

## Expected
Spark's dynamic partition overwrite on the pandas backend: hive-style folders
(`batch=2/part-...`), replacing only the partitions present in the new data, the same
layout Spark writes (checked with the parity-checker), committed per partition through
the existing staging and rename.

## Evidence
The refusal above (a clear message, which is good). The E-01 rerun with this mode
could not start.

## Fix
`ubunye/adapters/pandas_partitions.py` holds Spark 4.2's rules, ported from its source
(`ExternalCatalogUtils`, `PartitioningUtils`, `FileFormatWriter`,
`HadoopMapReduceCommitProtocol`) and from 44 golden cases probed on live Spark (the
spec is in the session scratchpad, `partspec/SPEC.md`):

- Write, every save mode plus `overwrite_partitions`: `escape(col)=escape(value)`
  folders in `partitionBy` order (Windows extras only on Windows, as Spark), null and
  `""` as `__HIVE_DEFAULT_PARTITION__`, partition columns out of the files, one
  `part-00000-<uuid>.c000<ext>` per leaf, `_SUCCESS` at the root only.
- `overwrite_partitions` is Spark's dynamic overwrite: each leaf folder the data fills
  is staged beside the target, then swapped in one at a time (old aside, new in, old
  deleted); a failed swap puts the swapped leaves back. Siblings, stray files and the
  root `_SUCCESS` are untouched; a new target gets no `_SUCCESS`; an empty frame
  changes nothing.
- A partitioned `append` claims every file before it lands (ADR 008).
- Read: Spark's discovery and inference (int, bigint, decimal, double, timestamp,
  date, else text; widened across folders), partition columns after the data
  columns, Spark's ignored names, and Spark's refusals of conflicting layouts. A data
  file beside partition folders is dropped as Spark drops it, with a warning.
- Refused on write, with the reason: double, decimal, binary, time, interval and
  nested partition columns (Spark writes them but does not read them back as the same
  type), all columns / a missing column / a column named twice (as Spark), a null
  mixed with the literal `__HIVE_DEFAULT_PARTITION__` (Spark's write fails), and on
  Windows a value ending in `.` or values differing only in case.

## Before and after
- Unit: `tests/unit/backends/test_pandas_partitions.py` (89 cases) and
  `tests/unit/core/test_partitioned_rerun.py` (5). On the old code 93 of the 94 fail,
  mostly with "The pandas backend does not write partitioned folders (partition_by)",
  "Path does not exist or holds no data files" (partition folders were not read) and
  "does not support write mode 'overwrite_partitions'", the rest on refusals that did
  not exist; the one that passes guards the old top level read of a plain folder,
  which Spark shares. On the new code all 94 pass.
- Live Spark 4.2 parity: `tests/integration/test_pandas_partitions_parity.py`, 54
  passed (layout per type, csv/json/parquet, append/overwrite/overwrite_partitions over
  two runs with stray files, each engine reading the other's output, hand-built read
  cases).
- E-01 with `EVENTS=partitions` (events written with `overwrite_partitions`): 10
  trials, 10 killed, 10 of 10 right after the rerun, no debris, no `running` record
  (`experiments/E-01-crash-mid-write.md`).

## Not matched, and why
- An all null partition column reads back as all null text; Spark's type is `void`,
  which pandas has no match for.
- A folder value like `10:05:06` reads back as text; Spark 4.2 infers `time`.
- Folder names differing only in case: Spark takes the column's spelling from
  whichever folder its file index lists first, which is not a fixed order (the probe
  got `P`, the parity run got `p`). Values and type match.
- A `basePath` read option is not supported (unknown options are refused, as before).
- A partition emptied by a take back (ADR 008) keeps its empty folder; Spark ignores
  it on read. Removing it would race with another run landing a file there.
