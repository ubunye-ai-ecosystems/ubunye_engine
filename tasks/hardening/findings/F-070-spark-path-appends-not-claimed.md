# F-070: a Spark path append is not taken back after a crash, and --rerun doubles it

**Status:** fixed on branch fix/spark-append-claims (ADR 008 addendum, 2026-09-30)
**Severity:** major
**Source:** ADR 008 named it as a gap; proved on live Spark 4.2 (2026-09-30)
**Promise:** 5 (nothing is lost or doubled silently)

## What happens
Rerun safety takes back a run's appends only if the writer claimed each file in the
run's lease before it landed. The pandas backend did (F-011, F-012). Spark path
appends (`format: s3`, `file_format` parquet/csv/json/orc, `mode: append`) did not,
so on Spark:

- a run killed after its append was committed was appended again by the rerun (the
  batch twice, as E-01 found on pandas);
- a run that failed after its append (a later output failed) left the append, and
  the rerun added the batch again;
- `--rerun` of a finished batch appended it a second time (it can only remove claimed
  files).

## Repro
`tests/integration/test_spark_append_claims.py` on the old code (cb48da1), local
Spark 4.2, three rows per batch:

| case | old code |
|---|---|
| child process killed (`os._exit`) after its append, then rerun | dt=2 holds 6 rows |
| killed after Spark wrote, before the files moved in | the kill point does not exist; the run finishes, the rerun is refused |
| foreign files in the folder, then takeover and `--rerun` | dt=2 holds 7 rows (4 expected) |
| `--rerun` of a finished batch | 6 rows |
| `--rerun` of a partitioned batch, parquet/csv/json/orc | 6 rows each |
| a later output fails the run | the append stays |
| two runs of one batch at once | refused by the lease (already safe) |

9 of 10 fail on the old code.

## Why Spark cannot claim its files directly
Read in Spark 3.5.8 and 4.0.1 sources (identical here) and seen on 4.2:
`HadoopMapReduceCommitProtocol.getFilename` makes `part-NNNNN-<jobId>.c000<ext>`, and
`InsertIntoHadoopFsRelationCommand` makes `jobId` with `UUID.randomUUID()`; nothing
sets it. Files reach the output at job commit with `FileOutputCommitter` v1, at each
task commit with v2 (and a failed v2 job leaves them). So the names are not known
until they have landed.

## Fix
`ubunye/adapters/spark/claimed_append.py`, called from `write_exec.apply` for a path
append. Spark writes into `.<name>.ubunye-<uuid>` beside the output, a folder the run
records in its lease (`runs.staging`) before it exists; the output is then marked
exact. When Spark is done, the data files in that folder (this run's by
construction) are claimed in one lease save (`runs.claim_all`) and moved in, one
rename each, each checked as landed. A same-named file already in the output is
refused before any claim. The staging folder is removed; a failed or dead run's
staging folder is removed with its claims (`_take_back`). The output folder is never
listed.

Only when a lease is held, the format is plain files (parquet, csv, json, orc, avro,
text) and Spark resolves the path to the local file system. Delta, JDBC and object
storage stay as they were (F-071, F-072, F-073). What Spark skips on read is not
moved (`.crc`, `_SUCCESS`, `_temporary`, `._COPYING_`), and neither are Parquet
summary files (`_metadata`, `_common_metadata`), which would describe the staging
folder only; a partition folder starting with `_` (`_part=1`) is data and is moved,
as Spark's `shouldFilterOutPathName` reads it.

## Before and after
- Integration (live Spark 4.2, Windows): old code 9 failed, 1 passed (two runs at
  once, already safe); new code 10 passed.
- Unit: `tests/unit/adapters/spark/test_claimed_append.py` (24, Spark-free): every
  claim is in the lease before the first file moves, a failed run removes only its
  own files, a same-named file is never replaced, the output is exact before Spark
  writes, object storage and HDFS and Spark Connect are not claimed, a dead run's
  staging folder is removed. They import the new module, so all fail on the old code.

## Not matched, and why
- A Parquet summary file (`parquet.summary.metadata.level`, off by default) is not
  updated for the appended files; Spark's own append rewrites it.
- `.crc` checksum files are not moved; Hadoop reads a file without one unchecked.
- A multi-node cluster writing to a `file:` path writes each executor's files to its
  own disk. Spark's own append is broken there too (the job commit on the driver
  sees only the driver's disk). Here, files the driver cannot see are neither
  claimed nor moved. Not tested; use a shared mount or object storage.
