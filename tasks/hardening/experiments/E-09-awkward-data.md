# E-09: Does awkward data go through a real task the same way on pandas and on Spark?

**Status:** answered on pandas (2026-09-30); the Spark side is in CI
**Why:** real inputs are rarely clean. Wide exports, long text, API JSON, spreadsheet
CSV, daylight saving, folders of small files, empty drops, odd numbers and odd column
names are where engines quietly disagree. (Numbering: the lead asked for E-09; the
file `E-09-stranger-rerun.md` also uses the number. Renumber one of them.)

## Method
Ten generated shapes, each run as a real Ubunye task on the pandas backend:
read (`s3` path connector), a trivial Narwhals transform (`with_columns` of a
literal), write parquet (and json and csv where the shape is about text), with
`--lineage`, through the CLI (`ubunye run --backend pandas`). Harness and data
generators: `scratchpad/awkward/harness.py` (not committed; the committed record is
the tests). Dev box, Windows 11, Python 3.13, pandas 2.3.3, pyarrow 25.0.1,
narwhals 2.19. Time and peak memory are for the whole CLI run, after the fixes.

Spark was not run locally (memory). Spark's behaviour comes from its source (Spark
3.5 and 4: `CSVInferSchema`, `CSVUtils.makeSafeHeader`, `JsonInferSchema`,
`JacksonParser`, `ParquetSchemaConverter`, `DateTimeUtils`, `DataSource`), and every
case is pinned as a parity test that CI runs on Spark 4 and 3.5:
`tests/integration/test_awkward_data_parity.py` (read cases compare columns, types
and values; digest cases compare the rows-v1 fingerprint; `test_the_same_task_gives_the_same_run_record`
runs one task per shape on both engines and compares the run records: rows, schema
hash, data hash, input and output). Cases where the answer is a design question are
`xfail(strict=False)`.

## Pass means
Every shape: the same rows, types and rows-v1 digest as Spark, or a clear refusal
where Spark refuses too. No crash where Spark reads; no silent difference.

## Result

Before the fixes 13 of 41 cases stopped or gave a silent difference. After: every
case runs or is refused as Spark refuses it, except the open questions below.

| # | Shape | Cases | pandas before | pandas after (time, peak) | Spark (source, CI test) | Rows, types, digest |
|---|---|---|---|---|---|---|
| 1 | Wide: 1,500 columns x 2,000 rows | csv inferSchema, parquet | ran; csv and parquet same digest | 5.4 s / 4.3 s, 385 MB | same | match expected; e2e `wide` (1,200 columns) |
| 2 | Long text: 1 MB values, LF, CRLF, NUL, tab | parquet (-> parquet, json, csv), csv multiLine, jsonl | csv **stopped**: "straddling object" (F-062) | 2.8 s, 3.6 s, 1.8 s; 290 MB | reads any length | match expected; `test_csv_values_longer_than_a_megabyte`, e2e `long-text` |
| 3 | Nested JSON: depth 30, arrays of structs, per-row keys, null vs missing | jsonl, multiLine, conflicts, `{}` records | nested fields **unsorted** (silent schema diff); conflicts **stopped**; `{}` rows **lost** (F-064, F-065) | 1.6 s each | sorted, widened to string / decimal, rows kept | match expected; `test_json_inference` (11), e2e `nested-json`, `conflicting-json` |
| 4 | Messy CSV | doubled quotes (both escapes), backslash, BOM + CRLF, latin-1, cp1252, cp1252 read as UTF-8, mixed dates, thousands separators, duplicate and blank header, ragged rows, space after comma | cp1252-as-UTF-8 **stopped** (F-061); duplicate/blank header **stopped** (F-060); ` 2` typed int (F-067) | 1.3 to 1.7 s each, 110 MB | U+FFFD; `_c2`, `a0`; double | match expected; `test_csv_header_names`, `test_csv_bytes_not_in_the_encoding`, fuzz, `test_csv_infer_schema_numbers` |
| 5 | Time zones (New York session): offsets, DST gap and fold, naive vs aware, 0001 to 9999 | csv inferSchema, csv schema, parquet ntz and ltz | all three **stopped** on the gap or fold (F-063) | 1.4 to 1.6 s | Java's rule: gap moves later, fold takes earlier | values match expected; **no digest** past year 9999 (F-081); ntz round trip differs (F-080); looser forms (F-077) |
| 6 | Many small files: 5,000 files | parquet folder, csv folder, `p=/q=` partitions | 9.2 s warm, 4.8 s of it hashing one chunk per file (F-076) | 3.8 / 4.4 / 4.8 s warm; 30 to 40 s on files just written (antivirus, cold cache); 136 MB | same rows | match expected; e2e `many-files` (300), `partitioned`; header order (F-078) and schema drift across files (F-079) open |
| 7 | One big file: 300 MB, 5,000,000 rows | csv inferSchema, parquet | 14.0 s, 1.27 GB (csv) | 12.5 s, 1.31 GB (csv); 9.7 s, 594 MB (parquet) | same | csv inference now Spark's (F-067): read 2.6 s, was 2.0 s |
| 8 | Empty inputs | zero rows with schema, header only, 0 byte csv/json (with and without schema), empty folder with schema, no-column records | with schema **refused** (F-068); `{}` records lost (F-065) | 1.3 to 1.5 s | zero rows of the schema | match expected; no schema: pandas refuses, Spark by source gives an empty frame (F-068, open, xfail) |
| 9 | Special numbers | parquet NaN, inf, -0.0, 5e-324, decimal(38,18), int64 limits, float32; uint8..uint64; csv and json tokens | uints kept **unsigned** (F-066); csv past 64 bits a **lossy double** (F-067) | 1.3 to 1.5 s | smallint/int/bigint/decimal(20,0); exact decimal | digest compared in `test_parquet_special_numbers`, `test_parquet_unsigned_integers`, e2e `special-numbers` |
| 10 | Column names: unicode, spaces, dots, reserved words, tab, case-only duplicates | csv header, parquet, parquet `Col`/`col` | case duplicates **accepted** (Spark refuses) (F-069) | 1.4 to 1.5 s; case duplicates refused | refused by default | `test_awkward_column_names_read_the_same` (digest), `test_names_that_differ_only_by_case_are_refused` |

Findings filed: F-060 to F-081. Fixed, one commit each with a test that failed
before: F-060, F-061, F-062, F-063, F-064, F-065, F-066, F-067, F-068 (with a
schema), F-069, F-076. Open (design questions or needing a live Spark answer):
F-068 (no schema), F-077, F-078, F-079, F-080, F-081.

Stops where Spark reads: 6 findings before (F-060, F-061, F-062, F-063, F-064,
F-068), none after. Silent differences: F-064 (nested order), F-065 (rows lost),
F-066, F-067 fixed; F-078, F-079, F-080 open. F-069 was the reverse (runs on pandas,
fails on Spark), fixed.

What is not measured here: live Spark timings for the same shapes (CI runs them as
tests, not as a benchmark) and whether each source-derived Spark behaviour holds on
3.5 and 4; the first CI run of the parity file answers the second.
