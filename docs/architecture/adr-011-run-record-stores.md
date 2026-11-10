# ADR 011: Run records are stored through a plugin, found by name

**Status:** accepted, unreleased (0.9)

## Context

The run record (the receipt, [ADR 006](adr-006-run-record.md)) was always written
to JSON files under `<usecase_dir>/.ubunye/lineage`. The recorder, `ubunye lineage`,
`ubunye gate` and the MCP server each built a `FileSystemLineageStore` directly.
The `ubunye.lineage_backends` entry points (`filesystem`, `delta`) existed, but
nothing that runs used them.

Jobs that share no disk (two Databricks jobs, a cluster and a laptop) could not
see each other's runs. Later work (`ubunye status`, "has the upstream task
finished?") needs one store they all reach.

There were also two contracts. `LineageStore` stores the whole `RunContext`;
everything that runs used it. `LineageBackend` stores a `LineageRecord`, which
keeps a few fields and flattens the rest into strings: steps, hashes, timings,
expectations and the environment do not survive it. The Delta backend only
spoke `LineageBackend`.

## Decision

1. **One contract: `RunRecordStore`** (`ubunye.interfaces.lineage`). A store
   saves a whole `RunContext` and gives it back unchanged. Four methods, no base
   class needed:
   - `save(ctx)`: create or replace the record of `ctx.run_id` for its task;
   - `load(task_path, run_id)`: one record, or `LineageRecordNotFoundError`;
   - `list_runs(task_path, n)`: a task's newest `n` records;
   - `search(task_path, status, since, limit)`: newest first, filtered.

   Newest first means written last first. `LineageBackend` stays for code that
   uses it, but no receipt goes through it.
2. **Stores are plugins** in the `ubunye.lineage_backends` group, built with one
   argument: their location, passed unchanged. `ubunye.lineage.stores` finds
   them, as `ubunye.core.backends` finds backends ([ADR 001](adr-001-backends-are-plugins.md)).
   An unknown name is an error that lists the installed ones.
3. **One order**, like [ADR 003](adr-003-backend-resolution.md):
   `--lineage-backend` / `lineage_backend=` / `LineageRecorder(backend=)` >
   `UBUNYE_LINEAGE_BACKEND` > `filesystem`. Every entry point uses it:
   `ubunye run`, `ubunye test run`, `ubunye lineage *`, `ubunye gate`,
   `ubunye prove`, `ubunye mcp`, `ubunye sync lineage`, `run_task`,
   `run_pipeline` and `notebook`.
4. **The location.** The filesystem store keeps its layout:
   `<usecase_dir>/<lineage_dir>`, so nothing moves. Any other store gets
   `--lineage-dir` exactly as given (`main.ops.ubunye_runs`, `s3://bucket/runs`).
5. **No `config.yaml` key.** Where receipts go is a platform choice, like the
   backend: the same task folder writes to files on a laptop and to a shared
   table in production. The flag, the argument and the variable cover that.
6. **Delta stores the record whole.** One row per save: the record as JSON plus
   columns to find it by (`run_id`, `task_path`, `usecase`, `package`, `task`,
   `status`, `started_at`, `ended_at`, `variables`, `record_version`,
   `written_at`). It only appends, so jobs never conflict; a read takes the last
   row written for each run. Without Spark it does not write local files: a
   record it cannot write goes to the fallback manifest (Decision 2 of the
   metadata contracts) with a warning, and `ubunye sync lineage` replays it.
7. **A conformance suite**, `ubunye.testing.lineage_conformance`, holds every
   store to this: the filesystem store in the unit tier, Delta (a path and a
   named table) on live Spark, and a third party store in a test.

## Consequences

- `UBUNYE_LINEAGE_BACKEND=delta` with `--lineage-dir main.ops.ubunye_runs` puts
  every job's receipts in one table that any job can read.
- Nothing changes for anyone who sets nothing: same files, same places.
- `LineageRecorder(store=...)` still works with a DeprecationWarning; `backend=`
  is the new name.
- `DeltaLineageBackend.search()` now returns run records; the old
  `LineageRecord` search is `search_records()` (keyword arguments `task`,
  `usecase` or `pipeline` still reach it, with a DeprecationWarning).
- Rerun safety ([ADR 008](adr-008-rerun-safety.md)) still reads a dead run's
  status from the filesystem layout. With another store it finds no record and
  treats the dead run as unfinished. Moving that check onto the store is left
  for later.
