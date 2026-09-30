# F-004: run id prefix

**Status:** fixed in PR #98
**Severity:** minor
**Source:** stranger (both), round 1 (2026-09-28)
**Promise:** none

## What happens
lineage list prints 8 characters of a run id; lineage compare and show refused them. gate accepted a prefix but silently took the first match.

## Repro
ubunye lineage show --run-id <8 characters>

## Expected
Every command takes the prefix; an ambiguous prefix is refused.

## Evidence
tests/unit/lineage/test_storage.py::TestLoadByRunIdPrefix
