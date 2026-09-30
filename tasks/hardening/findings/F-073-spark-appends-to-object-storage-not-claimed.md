# F-073: a Spark append to object storage or HDFS is not taken back after a crash

**Status:** open (not fixed)
**Severity:** major
**Source:** F-070, which claims Spark path appends on the local file system only
(2026-09-30)
**Promise:** 5 (nothing is lost or doubled silently)

## What happens
F-070 claims a Spark path append when Spark resolves the path to the local file
system. A path on `s3a://`, `abfss://`, `gs://`, `dbfs:/` or HDFS (including a path
with no scheme on a cluster whose default file system is HDFS) is written by Spark
directly, as before: a killed run's append is appended again by the rerun, and
`--rerun` appends a finished batch again. Both are named in the log and the dead
run's record.

The lease itself only protects runs that share the usecase folder (ADR 008), which
most cloud jobs do not; that limit comes first.

## Why F-070 stops at the local disk
- A take back runs `os.remove` on claimed paths, possibly in a run that has not
  started Spark yet (the lease is taken before the backend writes). Removing an
  object needs the Hadoop file system of the right scheme, with its credentials.
- On S3 and GCS a rename is a copy and a delete per file, so "moved in one step" no
  longer holds: a kill mid-move leaves the staged copy and a partial target copy.
  Both are claimable, but the claim must be made before the copy starts, and the
  take back must remove both.
- Claims are kept relative to the usecase folder; an object URL is not.

## Options
1. Document (today): on object storage use `overwrite_partitions`, Delta with
   `replace_where`, or `merge`.
2. Extend F-070: take backs through `fsspec`, or through the Hadoop file system when
   Spark is up; claims stored
   as full URLs; moves as copy, then claim is kept until the source is deleted.
   Needs a real bucket to prove (free tier), and a skeptic round.
3. Wait for the object-storage lease (a conditional write), named in ADR 008 as the
   next step: without it, two cloud jobs of one batch are not protected anyway.

## Fix
None. Not in scope of F-070.
