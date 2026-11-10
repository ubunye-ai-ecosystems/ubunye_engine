# F-072: a JDBC append is not taken back after a crash, and --rerun appends it again

**Status:** fixed for appends with `replace_where` on `fix/0.9-delta-jdbc-appends` (2026-11-10); a plain JDBC append is unchanged
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
`tests/integration/test_jdbc_append_reruns.py`, on Apache Derby embedded in memory
(its jar ships with Spark, so no network): a task that appends a batch through
`format: jdbc`, fails after the write, and is rerun with the same `dt`; and
`--rerun` of a finished batch. Both held the batch twice before.

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
Option 3, opt in through `replace_where` (option 2's predicate, without its two
transactions). The batch is written to `ubunye_stage_<12 hex>` beside the target
(named by task, output and batch under a lease, so a retry replaces a staging table
a killed run left), then one transaction runs `DELETE FROM t WHERE <replace_where>`
and `INSERT INTO t (cols) SELECT cols FROM stage`, and the staging table is
dropped. Plain SQL; identifiers quoted by Spark's dialect for the URL. The lease
notes the output as `idempotent: replaces`, so it is not named as unrepaired.

3 of 4 integration cases fail on the old code (the batch twice after a failed run
and after `--rerun`; the move-in seam missing); 4 of 4 pass. The fourth pins that a
JDBC append without `replace_where` is unchanged.

**Not fixed, said plainly:** a plain JDBC append (no `replace_where`). There is no
generic way to know which rows a killed run inserted without a batch column, and a
ledger table in the user's database is more than this needs. The run still names it
after a crash, and the hint names `replace_where`.
