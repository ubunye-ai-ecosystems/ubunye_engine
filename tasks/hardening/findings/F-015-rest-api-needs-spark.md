# F-015: the REST connector needs Spark; a laptop cannot pull from an API

**Status:** open
**Severity:** major
**Source:** experiment E-04 (2026-09-28)
**Promise:** 1 (same task anywhere)

## What happens
`ubunye plan` and `run` on the pandas backend refuse `format: rest_api`: "the
'rest_api' connector, which needs spark; the pandas backend does not provide it."
Pulling JSON from an HTTP API is one of the most common first tasks, and the backend
for small machines cannot do it.

## Repro
tests/experiments/e04_secrets.py <ubunye> pandas

## Expected
The REST reader (and writer) on pandas: the HTTP, auth, pagination and retry logic is
not Spark-specific; only the final frame is. Same rows as Spark, checked by the
parity-checker.

## Evidence
The refusal above (clear, which is good).
