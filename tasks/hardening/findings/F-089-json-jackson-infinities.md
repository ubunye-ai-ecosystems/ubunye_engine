# F-089: JSON infinities Jackson reads stop the pandas read

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** minor (a read that works on Spark stops on pandas)
**Source:** skeptic review of E-09 (item 8)
**Promise:** 1 (same result anywhere)

## What happens
`{"a": +INF}` (and `-INF`, `+Infinity`): the pandas read stops with a JSON syntax
error.

## Expected
Checked with Spark 4.2's JSON reader (JVM oracle): `NaN`, `Infinity`, `-Infinity`,
`+INF`, `-INF` and `+Infinity` are doubles; `INF`, `+NaN`, `-NaN`, `inf`, `nan` are
malformed. In a text column the source text is kept (`+INF`, F-084).

## Fix
`spark_json.loads`: when a document fails to parse and holds one of the three, they
are spelled as Python reads them (only where they stand as values) and it is parsed
again. `raw_tree` knows them for the source text.

## Evidence
Unit: `test_jacksons_other_infinities_are_doubles` (failed before; values and text
match the oracle). Integration (CI): `test_json_inference[jackson-infinities]`.
