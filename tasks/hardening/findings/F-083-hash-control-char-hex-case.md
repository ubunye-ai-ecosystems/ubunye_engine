# F-083: text with a control character hashes differently on pandas and Spark

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** minor (a digest mismatch; values the same)
**Source:** experiment E-09 (awkward data), CI run on live Spark 3.5 and 4
**Promise:** 1 (same result anywhere)

## What happens
E-09 task `long-text`: the same two rows on both engines, different rows-v1 digest
on Spark 3.5 and Spark 4. Spark `sha256:96f5aba4...a3ea4875`, pandas
`sha256:0393637e...addce36c8`. The text holds U+000B and U+001F.

## Cause
The canonical line is Spark's `to_json`, which is Jackson: a control character with
no short escape is written `\u000B`, upper case hex. The pandas side used Python's
`json.dumps`: `\u000b`. Characters whose hex has only digits (U+0000, U+0001) agreed,
which is why the older parity tests passed. Tried against the CI digest: upper case
hex alone gives Spark's digest; escaping U+007F or U+2028 as well does not.

## Fix
`content_hash.jackson_string`, used for every string in the canonical line (values,
struct and map keys, column names), and by the pandas json writer. A string with no
such character takes the fast C encoder as before.

## Evidence
Unit: `TestJacksonControlEscapes` (the pandas digest of the CI table equals Spark's,
on the Arrow path and the row path; failed before) and
`test_json_control_characters_are_upper_case_hex`. Integration: e2e `long-text` (CI).
