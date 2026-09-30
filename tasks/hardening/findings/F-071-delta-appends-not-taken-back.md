# F-071: a Delta append is not taken back after a crash, and --rerun appends it again

**Status:** open (documented; not fixed)
**Severity:** major
**Source:** F-070 review of what ADR 008 still leaves unclaimed (2026-09-30)
**Promise:** 5 (nothing is lost or doubled silently)

## What happens
A Delta output with `mode: append` is written by Delta's own transaction. ADR 008
cannot claim its files (moving files into a Delta folder behind its log would
corrupt the table, so F-070 does not touch Delta). A run killed after the Delta
commit is appended again by the rerun; `--rerun` of a finished batch appends it a
second time. Both are named in the log and the dead run's record, never repaired.

## Repro
Same as F-070's integration cases with `file_format: delta` (or `format: delta`).
Not yet written as a test.

## Expected
The batch lands once after a kill and a rerun; `--rerun` replaces it.

## Evidence: Delta's idempotent writes, probed (Spark 4.2, Delta 4.4, local)
Delta skips an append whose `txnAppId` has already committed a `txnVersion` at least
as high (`SetTransaction` in the log). Script in the session scratchpad,
`sparkclaims/delta_probe.py`, one row per write:

| step | rows after |
|---|---|
| append, `txnAppId=<task:output:batch>`, `txnVersion=0` | 1 |
| the same again (a rerun after a kill) | 1 (skipped) |
| `DELETE` every row by hand | 0 |
| the same append again (a deliberate rerun) | **0 (skipped, no error)** |
| `txnVersion=1` | 1 |

So a batch-keyed `txnAppId` would make the kill-and-rerun case exactly once for the
cost of two write options. It was not adopted as a default because:

- it skips silently: a user who removed the batch's rows (as the `BatchFinished`
  hint says to) and reruns gets exit 0 and no rows;
- `--rerun` would need a version above the last one, which Ubunye would have to read
  from the table's log or keep somewhere, and a replace still needs the old rows
  gone (`replaceWhere` on the batch column, or a delete);
- `SetTransaction` entries expire with `delta.setTransactionRetentionDuration` when a
  table sets it, after which the rerun appends again.

## Options
1. Document: for Delta batches that must land once, use `overwrite_partitions` with
   `replace_where` on the batch column, or `merge` on a key. Both are already
   supported and already rerun safe. (Done in ADR 008 and the CLI note.)
2. Opt in: a per-output `idempotent: true` that sets `txnAppId` to the task, output
   and batch key, and `txnVersion` to the lease's attempt count for `--rerun` (with
   a `replaceWhere` for the batch). Needs its own design and a skeptic round.

## Fix
None yet. Option 1 is the guidance today.
