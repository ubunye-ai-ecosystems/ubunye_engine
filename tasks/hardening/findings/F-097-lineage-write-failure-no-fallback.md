# F-097: a failed filesystem run record write leaves no fallback manifest

**Status:** fixed (0.9.1)
**Severity:** major (a run record is lost, said only in a log line)
**Source:** review (full CLI test run before 0.9.0)
**Promise:** 5 (nothing is lost silently: runs)

## What happens
When the filesystem lineage store cannot write (the lineage folder lies under a path
that is a file), `LineageRecorder._save` logs
"The run record of ... was not saved to the FileSystemLineageStore lineage store"
and carries on. No fallback manifest is written, so `ubunye sync lineage` finds
nothing to replay. The sync docs say a record that could not be saved is kept in a
fallback manifest for `sync` to replay. The Delta store does write one
(`ubunye/lineage/delta_store.py`, `_fallback`); the filesystem store does not.

## Repro
```bash
touch pipelines/.ubunye     # the lineage dir now sits under a file
ubunye run -d pipelines -u uc -p pl -t task --lineage
ubunye sync lineage -d pipelines   # nothing to sync
```

## Expected
The record goes to a fallback manifest somewhere writable (or the docs say the
filesystem store has none), and `sync lineage` replays it.

## Fix
Every store's failed save now takes one path: `LineageRecorder._save` catches the
failure and `_keep` appends the record to `~/.ubunye/fallback/<run_id>/lineage.jsonl`
(the place `ubunye sync lineage` reads), with the store's name and location, and
warns with the exact `ubunye sync lineage --lineage-backend ... --lineage-dir ...`
command. The Delta store's own copy of that logic (`_fallback`) is gone: its `save`
raises like the filesystem store's. A failure to mark a dead run interrupted (ADR
008) is still only a warning, as it always was for the filesystem store.

## Evidence
`tests/unit/lineage/test_lineage_stores.py`:
`test_a_filesystem_record_that_cannot_be_written_is_kept_and_replayed` (the repro:
the lineage folder under a file; both saves kept; `sync lineage` replays the finished
record into a writable store) and `test_a_delta_record_that_cannot_be_written_is_kept_and_said`
fail before the fix and pass after.
