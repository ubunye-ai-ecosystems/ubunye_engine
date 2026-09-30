# F-087: with nullValue set, an empty CSV field is text on pandas

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** major (silent: a number column becomes text)
**Source:** skeptic review of E-09 (item 4)
**Promise:** 1 (same result anywhere)

## What happens
`a` = `1`, empty, `NA`, `2` with `nullValue: "NA"` and `inferSchema`: pandas read it
as text `["1", "", null, "2"]`; Spark as `int` `[1, null, null, 2]`.

## Cause
Spark gives univocity `setNullValue(nullValue)`: an empty unquoted field comes back as
the `nullValue` text, and Spark reads that text as null. So with any `nullValue`, an
empty field is null. The pandas reader passed only `nullValue` to pyarrow's
`null_values`.

## Fix
`null_values` holds both `nullValue` and the empty string. Known difference left: a
quoted empty field (`""`) is null on pandas; Spark returns univocity's `emptyValue`
(`""`) for it. pyarrow cannot tell a quoted field from an unquoted one after parsing.

## Evidence
Unit: `test_an_empty_field_is_null_with_a_null_value_set` (failed before). Integration
(CI): `test_csv_empty_field_with_a_null_value`.
