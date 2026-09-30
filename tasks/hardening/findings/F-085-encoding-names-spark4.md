# F-085: pandas reads encoding names that Spark 4 refuses

**Status:** fixed on hardening/awkward-data (2026-09-30), following Spark 4
**Severity:** major (a task passes on pandas and fails on Spark 4)
**Source:** experiment E-09 (awkward data), CI run on live Spark 4.2
**Promise:** 1 (same result anywhere)

## What happens
`test_csv_bytes_not_in_the_encoding[cp1252]` and `[latin1]` on Spark 4.2:

```
IllegalArgumentException: [INVALID_PARAMETER_VALUE.CHARSET] The value of parameter(s)
`charset` in `CSVOptions` is invalid: expects one of the iso-8859-1, us-ascii, utf-16,
utf-16be, utf-16le, utf-32, utf-8, but got cp1252.
```

The pandas backend read both. Spark 3.5 read both too.

## Expected
Spark 4 (`CharsetProvider.forName`, unless `spark.sql.legacy.javaCharsets` is true)
takes seven names, compared in lower case. Checked with Spark 4.2's `CSVOptions` and
`JSONOptions` (`scratchpad/awkward/oracle/Charsets.java`): `UTF-8`, `utf-8`,
`ISO-8859-1`, `US-ASCII`, `UTF-16`, `utf-16le`, `UTF-16BE`, `UTF-32` pass; `utf8`,
`UTF8`, `latin1`, `iso8859_1`, `cp1252`, `windows-1252`, `ascii`, `utf-32le` are
refused, for csv and json alike.

## Fix
The pandas backend refuses any other csv or json `encoding` before reading
(`read_problems`, so `ubunye validate --backend pandas` says it too), naming the seven
and the way out. Spark 3.5 takes any Java name; the backend cannot know which Spark a
task will meet, so it follows Spark 4. The CI case skips on Spark 3.5 with that reason.

## Evidence
Unit: `TestEncodingNamesSpark4Accepts` (10 refusal cases failed before; the seven
names read). Integration (CI): `test_encoding_names_spark_4_refuses` (both engines
refuse, csv and json), and the encoding cases now use `ISO-8859-1`.
