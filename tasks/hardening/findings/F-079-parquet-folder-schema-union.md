# F-079: a parquet folder whose files differ in columns reads differently

**Status:** open (design question)
**Severity:** major (silent: columns present on one engine, missing on the other)
**Source:** experiment E-09 (awkward data), shape 6 (many small files)
**Promise:** 1 (same result anywhere), 5 (nothing lost silently)

## What happens
`part-0.parquet` has columns `a, b`; `part-1.parquet` has `a, c` (a writer that added
a column, a late file from last month's schema):

| Engine | Columns |
|---|---|
| pandas | `a, b, c` (every file's columns, missing values null) |
| Spark (by the source) | the columns of one file only (`mergeSchema` is false by default; the schema is read from one footer), so `b` or `c` is silently dropped |

Which file's schema Spark takes is not a documented order.

## The question
Following Spark means dropping a column silently, depending on file listing order.
Keeping the union means a different result. Options: refuse a folder whose files
disagree (and say `mergeSchema: true` makes Spark take the union); or read the union
and warn. The pandas backend refuses the `mergeSchema` option today, so the task
cannot even ask for the union on both engines. Recommended: accept `mergeSchema`
on pandas (union), and without it refuse files that disagree. Not changed here; E-05
(schema drift) is the related experiment.

## Evidence
`tests/integration/test_awkward_data_parity.py::test_parquet_folder_with_different_schemas`,
`xfail(strict=False)`. Local pandas: `a, b, c` with nulls, as above.
