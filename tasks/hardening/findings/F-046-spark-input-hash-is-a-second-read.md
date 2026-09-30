# F-046: On Spark, an input's hash reads the source again, so it can describe a later state

**Status:** fixed on fix/f046-input-source-version (2026-09-30): the record says when an input's digest is not of what was read; awaiting skeptic review and merge into hardening/real-world
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
must be the user's choice and the step must say which it has. A second option, also not
built: pin a Delta input's read to the version taken at the read (`versionAsOf`), so the
transform, the writes and the hash all see one snapshot and `source_changed` cannot
happen for Delta; that changes what the reader reads, so it needs its own decision.

## Open
- A file added to a folder after the read is not reported (it does not change the
  digest). A fresh listing at hash time could say "the source has new files"; not done,
  since it is not about the digest.
- For Delta, the version at the read is taken when the frame is built; the transform and
  the outputs compute later and may see a later version too. `source_changed` covers the
  input digest only.
- Taking a files version grows with the file count (about 0.6 ms per file on the dev box,
  twice per input); an input of 100,000 files would spend about 2 minutes. Not measured on
  an object store.
- The CI integration tier must run `tests/integration/test_source_version_spark.py` on
  Spark 4 and 3.5; the Delta test skips without Delta's jars.
