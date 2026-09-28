# ADR 007: One time zone for every backend, UTC unless the task says

**Status:** accepted (hardening programme, 2026-09-28)

## Context

A timestamp is an instant; a *day*, an *hour of day* or a *date* of that instant
depends on a time zone. Spark takes it from `spark.sql.session.timeZone`, which
defaults to the JVM's zone, which is the machine's. The pandas backend already read the
same key and defaulted to UTC (ADR 004 era), so the two agreed only when a task set the
key or the machine was in UTC.

The proving ground's first workload (C01, finding F-021) showed the cost: one Narwhals
transform, `dt.truncate("1d")` on `2024-01-02 10:15 UTC`, gave `2024-01-01 22:00 UTC` on
local Spark in Johannesburg and `2024-01-02 00:00 UTC` on pandas. The same task on a UTC
cloud would have agreed with pandas, so a laptop disagreed with its own cloud run. The
run record's data hash (ADR 006) caught it; every existing Spark test pinned the zone
itself, which is why none had.

## Decision

- A Spark session **the engine creates** is given `spark.sql.session.timeZone=UTC`
  unless the task's config sets the key. The pandas backend keeps UTC as its default.
  One key, one default, on every backend.
- A session **someone else started** (a Databricks notebook, the user's own) is never
  changed: the engine does not own it (the same rule as stopping sessions). On
  Databricks the platform default is UTC.
- A task that means local days says so: `ENGINE.spark_conf: {spark.sql.session.timeZone:
  Africa/Johannesburg}`; both backends then use that zone.

## Consequences

- Laptop and cloud, Spark and pandas, cut time the same way by default.
- **Behaviour change** for Spark runs on machines outside UTC whose tasks relied on the
  machine's zone: date truncation, `date()` of a timestamp and hour-of-day now follow
  UTC. Such tasks set the key explicitly. Instants themselves (and the `rows-v1` hash,
  which writes timestamps in UTC) are unaffected.
- Not yet done: the effective zone is not written in the run record; an ambient session
  in another zone is not reported. Both are follow-ups (see F-021).
