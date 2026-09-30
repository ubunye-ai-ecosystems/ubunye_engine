# F-063: a timestamp in a daylight saving gap or fold stops the pandas run

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** major (a run Spark finishes stops on pandas)
**Source:** experiment E-09 (awkward data), shape 5 (time zones)
**Promise:** 1 (same result anywhere)

## What happens
With `spark.sql.session.timeZone: America/New_York`, a CSV holding the wall clock
time `2024-11-03 01:30:00` (it happens twice that night) or `2024-03-10 02:30:00`
(it never happens; clocks jump from 02:00 to 03:00):

```
SourceReadError: ... Timestamp is ambiguous in timezone 'America/New_York':
2024-11-03 01:30:00 is ambiguous.
ArrowInvalid: Timestamp doesn't exist in timezone 'America/New_York':
2024-03-10 02:30:00.000000 is in a gap
```

Three places: CSV `inferSchema`, an explicit `TIMESTAMP` schema (CSV and JSON), and
writing a naive timestamp a transform made (it is read as session time). Every zone
with daylight saving meets this twice a year.

## Expected
Spark turns a local time into an instant with `ZonedDateTime.of(localDateTime,
zone)` (`DateTimeUtils.stringToTimestamp`, and the parsers built on it). Java's rule:
a time in a gap moves later by the length of the gap (so 02:30 reads as 03:30 EDT,
07:30Z); a time in a fold takes the earlier offset (01:30 EDT, 05:30Z). Spark never
stops on either. Partition values already followed this rule (`zoneinfo`, fold 0).

## Fix
`pandas_io.assume_zone`: pyarrow's `assume_timezone` first (no cost when there is no
gap or fold); otherwise the fold takes the earliest instant and a gap time is taken
with the offset in force a minute before the change. Used by all three paths.

## Evidence
Unit: `tests/unit/backends/test_pandas_awkward_data.py::TestDaylightSavingGapsAndFolds`
(4 failed before, 4 pass after), in seconds, ms, us and ns. Integration (CI):
`test_csv_daylight_saving_gaps_and_folds` (New York, London, Lord Howe's 30 minute
change; inferred and explicit schema) and `test_json_daylight_saving_with_a_schema`.
A pyarrow trap found on the way: `local_timestamp` of `06:59:59.999999999Z` in
nanoseconds gives the offset after the change; the fix reads the offset a whole
minute before it.
