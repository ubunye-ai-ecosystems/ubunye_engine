# F-072: a JDBC append is not taken back after a crash, and --rerun appends it again

**Status:** open (not fixed)
**Severity:** major
**Source:** F-070 review of what ADR 008 still leaves unclaimed (2026-09-30)
**Promise:** 5 (nothing is lost or doubled silently)

## What happens
The `jdbc` writer with `mode: append` inserts rows through Spark's JDBC writer, one
transaction per partition. There are no files to claim. A run killed after some or
all partitions committed is appended again by the rerun (some or all rows twice);
`--rerun` of a finished batch inserts it a second time. ADR 008 names the output in
the log and the dead run's record, and does nothing else.

A kill in the middle is worse than on files: Spark commits each partition's
transaction on its own, so a killed job can leave part of the batch, and nothing
records which part.

## Repro
Not yet run. Any JDBC target (SQLite or Postgres in a container): a task that
appends a batch through `format: jdbc`, killed after the write (as in F-070's
integration test), then rerun with the same `dt`.

## Expected
The batch lands once, or the run refuses and says how to clean up.

## Options
1. Document (today): make JDBC batches idempotent in the database. Stage into a
   table per batch and swap it in, or keep a batch column with a unique key and
   upsert, or delete the batch's rows in a pre-action before the append.
2. A pre-action in the writer: `delete_where: "batch = '{{ dt }}'"` run in the same
   connection before the insert. Still two transactions; a kill between them loses
   the batch until the rerun, which is safe.
3. A staging table named by the lease (the F-070 idea for tables): write the batch
   to `<table>__ubunye_<uuid>`, recorded in the lease, then `INSERT ... SELECT` and
   drop it in one database transaction. The only option that is exactly once, and
   the most work (per-dialect SQL).

## Fix
None. Not in scope of F-070.
