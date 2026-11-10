# F-097: a failed filesystem run record write leaves no fallback manifest

**Status:** open (target 0.9.1)
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

## Why not fixed here
Out of scope for the 0.9.0 release blockers: where a filesystem store's fallback
should live when its own folder is unwritable needs a decision. Planned for 0.9.1.

## Evidence
The 0.9.0 CLI test run; `ubunye/lineage/recorder.py` `_save`.
