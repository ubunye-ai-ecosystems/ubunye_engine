# F-046: On Spark, an input's hash reads the source again, so it can describe a later state

**Status:** open
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
