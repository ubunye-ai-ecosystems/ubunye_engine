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
append. Spark writes into `_ubunye-<uuid>` inside the output (beside it in the first
version; see the skeptic review below), a folder the run
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
- An empty output folder is left behind when a first append to a new output fails
  (the folder is made before Spark writes, so the staging folder can go inside it).

## Skeptic review (2026-09-30)
The skeptic reviewed a4beb70 (CI green on Spark 4 and 3.5) with a crash matrix (7
kill points, flat and partitioned, each followed by another batch, a takeover,
`--resume` and `--rerun`), two batches at once, name clashes, foreign files and decoy
staging folders. It found no file deleted or doubled that the run did not own. It
found seven other faults (scripts in the session scratchpad, `skeptic-f070/`); all
are fixed in one commit:

1. **An output on another disk failed.** An output folder that is a junction or
   mount on another drive: the rename from a staging folder beside it failed
   (WinError 17; EXDEV on Linux).
2. **A long output name failed.** `.<name>.ubunye-<id>` beside an output folder named
   with about 235 characters or more passed the 255 character limit (Spark: `Mkdirs
   failed`).
3. **Globs of the parent read the staged batch.** `lake/*`, `lake/*/*.parquet` and
   polars `**` saw the staging folder beside the output.

   Fix for 1 to 3: the staging folder is inside the output, `_ubunye-<12 hex>` (20
   characters), made after the output folder (made if missing, as Spark's append
   makes it). Spark, pyarrow, pandas and Ubunye's reader skip it, partition discovery
   included (a leading `_` and no `=`). A take back still removes only the recorded
   folder. A staging folder left in the output is named by the next append, never
   removed. A job that overwrites the whole output during an append removes the
   staging folder with it, and that append fails and is taken back.
4. **Claiming was quadratic.** `landed()` read the whole lease once per file, and the
   lease grows with each claim. Now one stat of the takeover mark per file (a
   takeover writes it before taking anything back) and one full read after the last
   file (`runs.all_landed`, which also catches a lease displaced without a mark). The
   pandas backend uses the same; its first append to a new folder, which lands every
   file in one rename, now has all of them removed if the run was taken over (it
   removed only the first). Skeptic's `perf_probe.py` on the dev box, 12,000 files in
   partition folders: 76.1 s before, 14.5 s after (14.3 and 14.6 in two runs; plain
   renames of the same files 3.2 s, the rest is the probe's own file writes, the walk
   and the claims).
5. **False warnings.** An output was reported as "may hold part of the batch" when
   its claim list was empty: a name clash refused, a kill before the claims, Spark
   failing. An output written through a staging folder now counts as fully taken
   back with no claims; the finished note marks it `staged`, so `--rerun` does not
   warn either. An exact output without staging and without claims is still named (a
   custom writer on a claiming backend may append without claiming).
6. **A silent fallback.** When the claimed route does not apply (no lease, a format
   that is not plain files, a path that is not local, Spark Connect), one info line
   names the output and the reason.
7. **Ctrl+C during Spark's write.** The JVM could keep writing into the staging
   folder after it was removed. The write now runs in a job group of its own (the
   caller's group is restored after), cancelled before the folder is removed. A task
   already writing when the cancel arrives may still leave a file a moment later; the
   folder is then left in the output, skipped by readers and named by the next
   append. Not fully closed, said plainly.

Before and after (a4beb70, then this commit):

| proof | a4beb70 | now |
|---|---|---|
| `test_spark_append_claims.py` (14 cases) | 4 failed: readers flat and partitioned, output on T: through a junction, 240 character name | 14 passed |
| `test_claimed_append.py` unit (34) | 17 failed | 34 passed |
| `perf_probe.py`, 12,000 files | 76.1 s | 14.5 s |
| crash matrix, 7 kill points x flat and partitioned | no foreign loss, no double | 14 of 14 end with each batch once, no staging folder left, the foreign files and both decoy staging folders (beside and inside) kept |

On a4beb70 the unit failures include tests of the new behaviour (the reason a path
is not local is now raised, not None; the staging folder's place); the faults above
each have at least one test that fails for the fault itself.
