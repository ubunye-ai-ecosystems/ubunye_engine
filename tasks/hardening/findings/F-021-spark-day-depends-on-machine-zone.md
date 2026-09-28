# F-021: on local Spark, a "day" depended on the machine's time zone

**Status:** fixed (see commit fixing F-021; ADR 007)
**Severity:** major
**Source:** proving ground, workload C01, pandas-local vs spark-local (2026-09-28)
**Environment:** Windows dev box in Africa/Johannesburg (UTC+2), Spark 4.2, pandas 3
**Promise:** 1 (the same task gives the same result anywhere)

## What happens
C01's portable transform truncates each order's timestamp to a day. Spark truncated
`2024-01-02 10:15 UTC` to `2024-01-01 22:00 UTC` (midnight in Johannesburg); pandas to
`2024-01-02 00:00 UTC`. `ubunye prove report`: inputs, schema and rows PASS, data FAIL
for `order_lines`.

## Root cause
The pandas backend reads `spark.sql.session.timeZone` and defaults to UTC; a Spark
session the engine created took the JVM's zone, which is the machine's. Every Spark test
pinned the zone explicitly, so the default divergence was never exercised.

## Repro
Run examples/proving/c01_portable_etl on pandas and on local Spark on a machine outside
UTC, then `ubunye prove report`.

## Fix
Engine-created Spark sessions default to UTC unless the task sets the key; sessions the
engine did not start are untouched (ADR 007).

## Evidence
Before: spark-local digest 09f629ed5b5f, pandas-local bb08a7d7a9fd, data FAIL.
After: both bb08a7d7a9fd, every dimension PASS.
Test: tests/unit/backends/test_spark_default_time_zone.py (fails without the fix).

## Follow-ups
Record the effective session zone in the run record, so a divergence explains itself;
warn when an ambient session's zone differs from the task's.
