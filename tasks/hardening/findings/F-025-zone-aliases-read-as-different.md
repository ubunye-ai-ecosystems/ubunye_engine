# F-025: the proving report called two names for UTC two different zones

**Status:** fixed (see commit fixing F-025)
**Severity:** major (a false FAIL in the proving ground itself)
**Source:** proving ground, prove-c01 run 36496707929 (2026-09-28), Databricks column
**Promise:** the proving ground reports what happened

## What happens
Databricks records its session zone as `Etc/UTC`; local Spark and pandas record `UTC`.
The data was identical (digest bb08a7d7a9fd, data, schema, rows and inputs PASS), yet
identity was FAIL with "session time zone Etc/UTC, reference UTC": the names were
compared as text.

## Fix
`ubunye.proving.matrix.same_zone`: the UTC spellings are folded together; other names
are the same zone when both give the same UTC offset every 15 days from 1990 to 2040;
a name that cannot be resolved stays different (strict when in doubt).

## Evidence
tests/unit/proving/test_matrix.py: `test_databricks_etc_utc_is_the_same_run_as_utc`
fails without the fix; aliases and genuinely different zones (Europe/London against UTC)
are both pinned.
