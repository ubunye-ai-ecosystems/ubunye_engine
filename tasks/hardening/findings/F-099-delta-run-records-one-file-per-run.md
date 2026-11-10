# F-099: the Delta run record store left one small file per save

**Status:** fixed (0.9.1)
**Severity:** major at scale (reads slow down with every run)
**Source:** review (0.9.1)
**Promise:** none

## What happened
`DeltaLineageBackend` appends one row per save, and a run saves twice. Each append
writes one tiny file. Nothing compacted them (`compact()` runs `OPTIMIZE`, but nothing
calls it), so after thousands of runs a read of one task's history opened thousands
of files.

## Fix
When the store creates its table (path or name), it creates it clustered by
`task_path` (liquid clustering, `clusterBy` in delta-spark 3.2 and later) and with
`delta.autoOptimize.autoCompact = true`. Open-source Delta (4.4 checked) and
Databricks both honour that property: after a write, Delta merges the small files
once enough have piled up. `delta.autoOptimize.optimizeWrite` is not set: a one-row
append writes one file anyway, and an optimized write adds a shuffle to every save.
A table that already exists is used as it is, never altered; `docs/cli.md` shows the
three statements that turn the same on for it. Reads still filter on `task_path`
before the window.

## Evidence
`tests/integration/test_lineage_delta_store.py`:
`test_a_new_table_is_clustered_by_task_and_compacts_itself` (200 saves on two tasks:
clustered by `task_path`, auto compaction on, under 60 files in the table, `list_runs`
newest first for each task) fails before the fix and passes after;
`test_an_existing_table_is_used_as_it_is` shows a 0.9.0 table is not altered.
