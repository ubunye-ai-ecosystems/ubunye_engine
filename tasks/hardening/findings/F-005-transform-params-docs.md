# F-005: transform params docs

**Status:** fixed in PR #98
**Severity:** major
**Source:** stranger (Olist), round 1 (2026-09-28)
**Promise:** none

## What happens
docs/config/transform.md had no word on transform.params or how a Task reads it. It was removed by mistake in 0.7.1.

## Repro
Read docs/config/transform.md looking for how to pass a setting to a Task.

## Expected
A worked example of params and self.config.

## Evidence
Section restored with an example.
