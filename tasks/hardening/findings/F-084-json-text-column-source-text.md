# F-084: Spark 4 keeps a JSON value's source text in a text column

**Status:** fixed on hardening/awkward-data (2026-09-30), following Spark 4
**Severity:** minor (text differs: `1.50` against `1.5`)
**Source:** experiment E-09 (awkward data), CI run on live Spark 4.2 and 3.5
**Promise:** 1 (same result anywhere)

## What happens
A JSON field that is a number in one record and text in another is a text column.
What text does the number become?

| Input (JSON lines) | Spark 4.2 | Spark 3.5 | pandas before |
|---|---|---|---|
| `1.50` | `1.50` | `1.5` | `1.5` |
| `1e2` | `1e2` | `100.0` | `100.0` |
| `{ "k" : 1, "s":"q\u000b" }` | as written | `{"k":1,"s":"q\u000B"}` | as 3.5 |

CI: `test_json_inference[number-and-text]` and `[object-and-text]` failed on Spark 4
only, and the E-09 task `conflicting-json` had another input digest on Spark 4.

## Cause
Checked with Spark 4.2's own `JacksonParser` in a small JVM (no session,
`scratchpad/awkward/oracle/JsonRows.java`): when the parser reads bytes (the JSON lines
path, `CreateJacksonParser.text`), a value read into a text column is its exact source
text. When it reads a stream (`multiLine`, and a line read with an `encoding` option),
the value is copied through Jackson's generator (`Double.toString`, compact, upper case
hex), which is what Spark 3.5 does in every case.

## Fix
`spark_json.raw_tree` finds each value's source text in its line; the pandas JSON
reader uses it for values that are not strings in a text column, for JSON lines read
with no `encoding` option (also with an explicit schema). Other cases keep Jackson's
text. Only lines that need it are parsed a second time.

## Spark 3.5
Spark 3.5 differs from Spark 4 here and the pandas backend cannot know which Spark a
task will meet, so it follows Spark 4. The two cases skip on Spark 3.5 in CI with that
reason.

## Evidence
Unit: `TestJsonInference` (source text for lines, Jackson text for multiLine; 2 failed
before). Oracle: Spark 4.2 `JacksonParser` gave the same rows as the pandas reader for
the unit and CI inputs, and for arrays of objects and nested text fields.
