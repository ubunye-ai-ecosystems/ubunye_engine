# F-105: the pandas JSON reader failed on a file holding one JSON array

**Status:** fixed (0.9.1)
**Severity:** minor (a common file shape failed with a parse error)
**Source:** docs audit (0.9.1)
**Promise:** the pandas backend reads files as Spark does (ADR 004)

## What happened
Without `multiLine`, the pandas reader parsed JSON Lines only, so a file holding one
array spread over lines (`json.dumps(rows, indent=2)`, what most tools write) failed
on its first line, `[`. Spark needs `multiLine: "true"` for that file too.

## Fix
`pandas_io._read_json` reads such a file as one document, as Spark does with
`multiLine`. It is recognised by its first line: it opens with `[` and is not a whole
JSON value by itself, so a JSON Lines file with an array on each line still reads as
JSON Lines. A file read this way keeps text values as Jackson writes them back, the
`multiLine` rule.

## Evidence
`tests/unit/backends/test_pandas_reads_like_spark.py::TestJsonDefaults::test_a_json_array_file_reads_without_multiline`
fails before and passes after; `test_lines_that_each_hold_an_array_stay_json_lines`
passes both ways. Parity with Spark:
`tests/integration/test_awkward_data_parity.py::test_json_array_file_reads_as_spark_reads_it_with_multiline`
(pandas with no option against Spark with `multiLine`) fails before and passes
after; the other 26 JSON parity tests pass.
