# ADR 018: `insert_new` inserts rows with new keys and changes nothing else

**Status:** accepted, 0.9.2

## Context

Some outputs are facts that never change once recorded: trip events, first-seen
dates, an audit trail. `append` duplicates them when a run is repeated, and
`merge` overwrites the first record with a later one. Teams wrote the anti-join
by hand in every transform, and a rerun under it was only as safe as that code.

## Decision

A write mode `insert_new`, with required `merge_keys`: insert the rows whose keys
are not in the target; leave every existing row as it is. The first write of a key
wins, and running the same batch again adds nothing.

- **The contract is in the core** (`ubunye/core/write_modes.py`), next to `merge`.
  It shares `merge`'s rules: keys required, and a target format the writer lists as
  mergeable (Delta by default, `MERGE_FILE_FORMATS` on the writer). `validate` and
  `plan` ask the same resolver, so both refuse a missing key or a parquet target.
- **Spark does it with one Delta `MERGE`** that has only
  `WHEN NOT MATCHED THEN INSERT *`, on a table or a path. Delta runs an insert-only
  merge as an append: no existing file is rewritten (the integration test checks
  the commit's metrics). Keys are compared with `<=>`, so a null key is not inserted
  again on every run.
- **The first run creates the target with `errorifexists`, not `overwrite`.** A path
  that holds files but is not a Delta table is refused before writing: Delta's own
  `errorifexists` only looks for a Delta log and would start a table on top of them.
- **Duplicates inside one batch are inserted**, as Delta does. Dropping them would
  change the data without saying so; the docs tell users to dedupe in the transform.
- **Refused where it cannot be kept:** the pandas backend (its capabilities list no
  such mode, and `execute_write` refuses it as a backstop), `jdbc` (native modes
  only) and `rest_api`. A pandas or plain-file version would be read the keys, then
  append: two overlapping runs could both insert a key, so it could not promise
  "changes nothing". Not built until a backend can do it in one transaction.

## Consequences

- One more mode in `WriteMode` and `LAKEHOUSE_MODES`; every writer that declared
  `ALL_MODES` (delta, unity, s3, hive) gains it, with no writer code changed.
- `plan` no longer reports "already exists" for a mode only because its first-run
  save mode is `errorifexists`; it checks the mode the user asked for.
- A target that exists as a non-Delta table (not a path) fails in Spark's own
  `MERGE` error, not ours.
