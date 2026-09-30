# F-078: a folder of CSV files with headers in different orders reads differently

**Status:** open (design question: follow Spark and be wrong, or keep the right answer and differ)
**Severity:** major (silent wrong values on one of the two engines)
**Source:** experiment E-09 (awkward data), shape 6 (many small files)
**Promise:** 1 (same result anywhere), 5 (nothing lost silently)

## What happens
Two files in one folder, `a.csv` with `x,y` / `1,2` and `b.csv` with `y,x` / `3,4`,
read with `header: "true"`:

| Engine | Rows |
|---|---|
| pandas | `x=1, y=2` and `x=4, y=3` (each file's header names its columns) |
| Spark (by the source) | `x=1, y=2` and `x=3, y=4` (every file read by position) |

Spark takes the column names from one file (the first line of its first partition,
and file splits are ordered by size, largest first) and reads every file by
position; with `enforceSchema` true by default it only logs a warning that a header
does not match. So Spark swaps the values of `b.csv` silently. The pandas backend
gives the right answer, and so a different one.

## The question
"Spark decides" says follow Spark, which would port a silent data error, and which
file names the columns depends on file sizes. The alternatives: keep reading by
name and warn that Spark would not; or refuse a folder whose headers differ (Spark
itself refuses with `enforceSchema: false`). Recommended: refuse, naming the files
and the headers, and suggest `enforceSchema: false` on Spark too; that keeps parity
of outcome (no silent difference) without porting the wrong answer. Not changed here.

## Evidence
`tests/integration/test_awkward_data_parity.py::test_csv_folder_with_headers_in_different_orders`,
`xfail(strict=False)`.

## Live Spark (CI, 2026-09-30)
The `xfail` parity case failed on Spark 4.2 (Python 3.13, Java 21) and Spark 3.5
(Java 11) as this finding says: the difference is real on both. Still open.
