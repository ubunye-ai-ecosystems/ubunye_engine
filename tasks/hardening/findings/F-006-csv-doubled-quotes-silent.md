# F-006: csv doubled quotes silent

**Status:** fixed in PR #98 (a warning; the read is unchanged)
**Severity:** blocker
**Source:** stranger (both), round 1 (2026-09-28)
**Promise:** 5

## What happens
Files written by pandas or Excel double their quotes; Spark's default escape is a backslash, so rows split wrongly and a number column silently became text. Both strangers lost the most time here.

## Repro
Read the Olist reviews or Amazon Reviews.csv with header, inferSchema, multiLine and no escape option.

## Expected
This is Spark's own behaviour: Reviews.csv reads as 568,357 rows on Spark and on pandas, and 568,454 with the escape set to a double quote. Parity is kept; the engine must say so.

## Evidence
escape_hint: ubunye plan and the pandas reader warn and name the fix. It flags both problem files and none of three clean ones.
