# F-032: the pre-run check for partitioned writes never fired

**Status:** fixed on hardening/real-world (2026-09-29)
**Severity:** minor
**Source:** building F-012 (2026-09-29)
**Promise:** a task a backend cannot run is refused before anything starts

## What happens
`ubunye/core/capabilities.py` looked for `partition_by` in an output's config. Every
writer reads `partitionBy` (s3, delta, hive, unity), so a backend that cannot write
partition folders was never refused up front; the task failed half way instead, at
the write.

## Fix
The check reads `partitionBy`, and still accepts `partition_by`. The test runs both
spellings; on the old code both fail.
