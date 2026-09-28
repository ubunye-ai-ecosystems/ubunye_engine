# F-009: llm structured output

**Status:** open
**Severity:** minor
**Source:** stranger (reviews), round 1 (2026-09-28)
**Promise:** none

## What happens
complete_many returns raw text; the user hand-wrote a parser and 20 of 300 answers missed the format.

## Repro
Ask a small model for 'SENTIMENT | ASPECT'.

## Expected
Decide: a documented pattern, a plugin, or a small helper. Must not grow the core without evidence.

## Evidence
To be gathered when worked.
