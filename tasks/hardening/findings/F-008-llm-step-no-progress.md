# F-008: llm step no progress

**Status:** open
**Severity:** minor
**Source:** stranger (reviews), round 1 (2026-09-28)
**Promise:** none

## What happens
A 300-call LLM step ran for about 6 minutes with no output between 'Starting task' and 'Run complete'.

## Repro
Run any task with a few hundred complete_many calls.

## Expected
Some sign of progress (calls done of total, spend so far), without noise in short runs.

## Evidence
To be gathered when worked.
