# F-088: a JSON record that repeats a key keeps its last value on pandas

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** major (silent: a value dropped where Spark refuses)
**Source:** skeptic review of E-09 (item 7)
**Promise:** 5 (nothing is lost silently)

## What happens
`{"a":1,"a":"x"}`: Python's `json` keeps the last value, so the pandas backend read a
string column `a` = `x` and the `1` was gone.

## Expected
Spark's Jackson parser does not reject repeated keys; `JsonInferSchema` makes one
field per occurrence (`struct<a:bigint,a:string>`, checked with Spark 4.2's classes),
and the read is then refused because the data schema has a duplicate column.

## Fix
`spark_json.loads` parses with an `object_pairs_hook` that refuses a repeated key at
any depth, naming it; the pandas JSON reader uses it for lines and multiLine. The dead
`_dedupe` (which could never see a repeated key) is gone. 200,000 records read in
3.37 s (unchanged within noise).

## Evidence
Unit: `test_a_repeated_key_is_refused_as_spark_refuses_it` (lines and nested
multiLine; failed before). Integration (CI): `test_a_json_record_with_a_repeated_key_is_refused`.
