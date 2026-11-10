# F-116: each Delta run record save reads the Delta log in 50 tasks

**Status:** fixed (0.9.1)
**Severity:** minor (speed)
**Source:** review (0.9.1 lineage pass)
**Promise:** recording a run costs little next to the run

## What happens
With no active SparkSession (a CLI or pandas run with `--lineage-backend delta`), the
Delta run record store starts a local session. Every save then reads the table's
small Delta log in 50 tasks (Delta's default `spark.databricks.delta.snapshotPartitions`),
about 4.5 s per save on a small machine, and Spark's console progress bars print
into the CLI output.

## Fix
Only in the session the store starts itself (no SparkContext running, so
`getOrCreate` really creates one): `spark.databricks.delta.snapshotPartitions=1`,
`spark.ui.enabled=false`, `spark.ui.showConsoleProgress=false`. An active session,
or a running context whose session `getOrCreate` would hand back, is never changed.

## Evidence
- `tests/unit/lineage/test_delta_store_local_session.py`: the local-session test
  failed before (no `snapshotPartitions` option) and passes after; two guard tests
  pin that a running context and an active session get no options.
- Live, dev box (16 cores, Spark 4.2, delta-spark 4.4.1), 8 saves to a new path table,
  `/tmp/ue091cbd/time_saves.py`:
  before: first save 11.48 s (creates the table), then 1.39 1.37 1.37 1.33 1.29 1.39 1.28 s;
  after: first save 9.38 s, then 0.80 0.73 0.73 0.71 0.70 0.66 0.71 s.
  With fewer cores the 50-task read costs more, so the gain is larger there.
