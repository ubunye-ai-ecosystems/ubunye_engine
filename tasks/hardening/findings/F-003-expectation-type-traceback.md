# F-003: expectation type traceback

**Status:** fixed in PR #98
**Severity:** major
**Source:** stranger (Olist), round 1 (2026-09-28)
**Promise:** none

## What happens
A between rule on a column that had been read as text crashed with a pyarrow/narwhals traceback of about 20,000 characters that never named the column.

## Repro
between on a string column (tests/unit/core/test_expectations.py::TestAColumnARuleCannotCheck).

## Expected
One error naming output, rule, column and type.

## Evidence
Now one ExpectationError under 2,000 characters, with a CSV hint. Missing columns are named too.
