# F-030: an input's `schema` (skip inference) is not in the I/O docs

**Status:** open
**Severity:** minor
**Source:** real-world example R1 (examples/real-world/food_prices_africa), WFP data 2025-2026, pandas-local vs spark-local (2026-09-29)
**Promise:** none

## What happens
The s3 reader accepts `schema:` (a DDL string) and skips type inference, which matters
on a 465 MB CSV. `docs/config/io.md` does not mention it; it is found only in the
reader's source.

## Fix
Document `schema` in docs/config/io.md with an example.
