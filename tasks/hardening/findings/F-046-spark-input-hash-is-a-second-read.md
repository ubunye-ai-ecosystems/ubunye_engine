# F-046: On Spark, an input's hash reads the source again, so it can describe a later state

**Status:** fixed on fix/f046-input-source-version (2026-09-30): the record says when an input's digest is not of what was read; skeptic review answered in a second commit (6 bugs fixed, Delta reads pinned); awaiting merge into hardening/real-world
**Severity:** minor for a source nothing else writes; major for a table another job appends to
**Source:** engine-fixer, while fixing F-040 (2026-09-29)
**Promise:** the run record's purpose (ADR 006: "what was read")

## What happens
The run record hashes each input at task end, after the writes, by running
`fingerprint_spark` on the frame the reader returned. On Spark that frame is lazy, so
the hash is a new scan of the source, not of the rows the transform read. The fix
for F-040 (ADR 009) holds the outputs, not the inputs, on purpose: holding every
input would keep a copy of all the data a task reads in executor memory and disk for
the whole run.

So if the source changes between the transform's read and the hash (another job
appends to the table, a file is replaced, the task's own `overwrite` output is its
input), the input digest is of the later state and says nothing. The record now
marks it: every Spark input step has `hash_basis: "recomputed"`. On pandas the input
is in memory, and its step says `materialised`.

## Repro
Not run yet. The shape: a task that reads a parquet folder; between the read and task
end, another process adds a file to the folder (a slow transform or a hook that
sleeps makes the window); compare the record's `inputs[0].data_hash` with the hash
of the folder before the new file.

Spark may narrow the window for a file source (not checked here): a file source lists
its files when the frame is built and keeps that list, so an added file should not be
read by the hash either. A replaced or deleted file, a table (Delta, JDBC, a catalog table) or a
`sql:` input are not protected this way.

## Expected
The record says the digest is of what the transform read, or says plainly that it is
not. Options: hash the input in the same pass the transform reads it (not possible in
general on a lazy engine), hold inputs when the user asks (the memory trade of ADR
009, larger), or record the source's own version where it has one (a Delta version, a
file listing with sizes and modification times) next to the digest.

## What Spark does (measured, dev box, Spark 4.2 local, 2026-09-30)
The guess above was half right. A lazy parquet frame keeps the file list from its
read: after another writer appended a file, the same frame still counted 10 rows, not
15, and its `inputFiles()` still named 2 files (a fresh read named 4). So a file
added to the folder is not read by the hash. A lazy Delta frame is not like that: after
an append it counted 13 rows, not 10. It reads the latest version on each action. A
file rewritten in place is read again by path; in the test below the hash then failed
(`FAILED_READ_FILE`), so the record had no digest, only `hash_error`.

## Fix
The record takes each input's source version at the read and again after its hash,
and says whether they match. Nothing is read from the data for it, and the digest and
the default (hash every input) are unchanged.

- `ubunye/lineage/source_version.py`: `capture(frame, io_cfg)` gives `{"kind": "delta",
  "version", "timestamp"}` (from `DESCRIBE HISTORY ... LIMIT 1`; a pinned read is its
  pinned version), `{"kind": "files", "files", "bytes", "latest_modified",
  "listing_hash"}` (the frame's own `inputFiles()`, with sizes and times from one Hadoop
  listing per folder), or `{"kind": "none", "reason"}` (SQL, JDBC, REST, a catalog table
  that is not Delta, Spark Connect). It never raises.
- The engine takes it right after each read, only in a recorded run (a hook that
  `reads_outputs`). The pandas reader keeps the list of files it read on the frame, so a
  pandas run records the files version too; its input is in memory, so it is not checked
  again.
- The recorder takes it again right after a `recomputed` input's hash, and writes
  `source_version`, `source_version_at_hash`, `source_changed` and `source_note` on the
  step. `hash_basis` keeps its meaning (how the digest was computed); `source_changed`
  says whether the source moved while it was.
- `lineage trace` prints the note; `lineage compare` calls that input's hash "unknown";
  `ubunye gate` warns on it and names it as a possible cause of a changed output, so it
  never says "nothing else changed, the transform is not deterministic". The OpenLineage
  hash facet carries `sourceVersion` and `sourceChanged`.

Evidence:

- `tests/integration/test_source_version_spark.py`, live Spark 4.2 with Delta 4.4 on the
  dev box: 4 passed on the fix, 4 failed on b6ff30b (`KeyError: 'source_version'` or
  `'source_changed'`). Unchanged parquet source: `source_changed: false`, digest equals the
  folder's. A file appended in the transform: `source_changed: false`, 100 rows, digest
  equals the folder's hash before the append. A file rewritten in the transform:
  `source_changed: true`. A Delta table appended in the transform: version 0 at the read,
  1 at the hash, `source_changed: true`, and the digest is of 13 rows.
- `tests/unit/lineage/test_source_version.py`: 17 tests with fakes (versions, the
  recorder, gate, compare, the pandas run, a plain run takes none); cannot even import on
  b6ff30b. Unit tier: 1758 passed, 2 skipped.
- Cost of taking a version (dev box, 3 runs each): 100 files 0.08 s, 1,000 files 0.62 s
  (about 0.6 ms per file, all of it Py4J calls), Delta history 0.16 s (0.9 s the first
  time). A recomputed input pays it twice.

## Cost side: what the input re-scan costs (not changed here)
The input hash is about half of all `hash_seconds`. E-06 (GitHub, before ADR 009): the
`events` input hash was 4.3 s of 9.4 at 1M rows (46%), 10.3 of 20.9 at 5M (49%), 27.8 of
54.5 at 20M (51%), 48.2 of 95.6 at 50M (50%). Dev box after ADR 009, 5M: 6.5 s of 11.9
(55%), about 45% of the whole `--lineage` overhead (31.93 s against 17.47 s plain).
Dropping it would put that run near 1.45x plain; an estimate from the parts, not a
measured run.

A future opt-in, not built: when an input has a strong source version (a Delta version,
or better a pinned one), skip its content hash and record the version as its identity.
That changes ADR 006's promise ("every input hashed like the outputs, every row"), so it
must be the user's choice and the step must say which it has. A second option,
pinning a Delta read to the version taken at the read (`versionAsOf`), was built after
the skeptic review (see below): the lead decided it, since it also fixes outputs that
read two versions.

## Skeptic review (2026-09-30, fd89982, live Spark 4.2 + Delta 4.4)
Six issues confirmed with scripts (`skeptic-f046/p1` to `p6` in the session scratchpad).
Each is fixed in the second commit on this branch, with a test that fails on fd89982 and
passes after (unit: 16 of 30 in `tests/unit/lineage/test_source_version.py` fail on
fd89982; integration: 6 of 8 in `tests/integration/test_source_version_spark.py` fail
on fd89982, the 2 that pass are unchanged file tests).

1. **A same size, same time rewrite passed as "of what the task read".** p4: a CSV
   rewritten (200 to 999, same size, mtime restored); the record said unchanged and "This
   digest is of what the task read", but the digest was not the read one. Fix: the
   listing includes the etag where Hadoop's `FileStatus` gives one (`EtagSource`, S3A and
   ABFS on Hadoop 3.3+); the version says `etags: true|false`. Without etags the note says
   "File names, sizes and modification times are unchanged since the read ... a file
   rewritten with the same size and time cannot be ruled out". p4 after: that note.
   `getFileChecksum` was not used (it reads the file).
2. **A pin in `options` was ignored.** p2a: `options: {versionAsOf: 0}` recorded version 1
   (latest) and then "changed" to 2. Fix: `versionAsOf` / `timestampAsOf` in `options`
   count, in any case, for `format: delta` and `s3` with `file_format: delta`. p2a after:
   version 0, `pinned_by: config`, unchanged, 10 rows.
3. **Two outputs of one Delta input read two versions** (the real consistency bug). p3:
   an append between the two outputs' computations gave `a_out` 10 rows and `b_out` 13,
   and the input digest was of 13. Fix (lead's decision A): a Delta read that names no
   version is pinned by the reader to the version the table is at when it is read
   (`ubunye/adapters/spark/delta_pin.py`, used by the Delta reader and by `s3` with
   `file_format: delta`). The frame carries the pin, so the record says `pinned: true,
   pinned_by: engine`; a pinned read is `source_changed: false` by construction and the
   table's latest version at the hash is recorded as `latest_version`, information only.
   p3 after: 10 and 10, input 10 rows, `latest_version` 1. This changes what a Delta read
   reads; ADR 006, ADR 009 and the connector docs say so.
4. **A listing error read as "every file is missing".** p1: one 503 on `listStatus` made
   `source_changed: true`. Fix: any error other than a definite `FileNotFoundException`
   makes the version `none` ("listing failed: ..."), so the change is unknown. p1 after
   (through the folder listing path, `p1b_listing_path.py`): `none`, `source_changed:
   null`. A definite "not found" still counts as missing: it is an answer, not an error.
5. **Cost.** (i) With `hash_inputs=False` versions were still taken: now only a hook that
   `reads_inputs` (the recorder, when it hashes inputs) asks for them. (ii) One file read
   from a folder of 10,000 listed the whole folder: now up to 1,000 files read are asked
   one by one (`getFileStatus`), more than that are listed by folder. (iii) No bound: now
   `UBUNYE_SOURCE_VERSION_TIMEOUT` (30 s) stops it and records `none` ("took longer
   than"). p4/p5, dev box, local disk, before and after:

   | Case | before (s) | after (s) |
   |---|---|---|
   | one file read from a folder of 10,000 | 2.55 | 0.005 |
   | 1,000 files in 1,000 partition folders | 2.85 | 1.59 |
   | 10,000 files in one folder (listed) | 4.15 | 4.28 |
   | for scale: Spark building that 10,000 file frame | 51.1 | 53.5 |

6. **OPTIMIZE called a change.** p2b: OPTIMIZE between the read and the hash (same rows)
   gave `source_changed: true`. With pinning the read is pinned, so p2b after: unchanged,
   30 rows, `latest_version` 3. For an unpinned Delta table (a catalog table read through
   `hive` or `unity`) the recorder reads the history between the two versions and does
   not call it a change when every commit is one that changes no rows (OPTIMIZE, VACUUM
   START/END, SET/UNSET TBLPROPERTIES, ADD/DROP CONSTRAINT); the operations are recorded
   (`commits_since_read`).

Also: the OpenLineage facet carries `sourceVersionAtHash`, and `docs/schemas/ubunye_hash.json`
types every field of `sourceVersion` and `sourceVersionAtHash`. mypy (`python -m mypy
ubunye`, as CI runs it) failed on fd89982 on `PandasDataFrameAdapter.source_files`; fixed
by declaring it on the class. The 8 errors left locally (runs.py unused ignores,
secrets.py, mcp_server.py) are the same on b6ff30b.

## Open
- A file added to a folder after the read is not reported (it does not change the
  digest). A fresh listing at hash time could say "the source has new files"; not done,
  since it is not about the digest.
- A Delta catalog table read through `hive` or `unity`, and a `sql:` input, are not
  pinned: two outputs of such an input can still see two versions. Only the record's
  check covers them.
- Pinning costs one `DESCRIBE HISTORY` per Delta read (about 0.16 s on the dev box), in
  every run, recorded or not.
- Without etags (local disk, HDFS), a rewrite with the same size and time is not seen;
  the note says so. Etags are not tested on a real object store here.
- Taking a files version over more than 1,000 files lists each folder; not measured on an
  object store.
- The CI integration tier must run `tests/integration/test_source_version_spark.py` on
  Spark 4 and 3.5; the Delta tests skip without Delta's jars.
