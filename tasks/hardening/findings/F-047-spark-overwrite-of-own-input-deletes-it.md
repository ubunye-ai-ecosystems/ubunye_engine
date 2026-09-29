# F-047: On Spark, an output that overwrites its own input deletes the input, then fails

**Status:** open (made safe on the paths ADR 009 holds; a plain run still loses the data)
**Severity:** major (data loss, with an error, but after the delete)
**Source:** engine-fixer, while choosing the mechanism for F-040 (2026-09-29)
**Promise:** 5 (nothing is lost silently); here it is lost loudly

## What happens
A task reads a parquet folder and writes back to the same folder with
`mode: overwrite`. Spark 4.2 (dev box, local) does not refuse the plan: it deletes the
folder's files, then runs the write, which reads the files it just deleted:

```
[FAILED_READ_FILE.FILE_NOT_EXIST] Encountered error while reading file
file:///T:/f040/mat1m/own_plain/part-00001-....snappy.parquet. File does not exist.
```

The folder is left holding only `_temporary`. The source is gone.

When ADR 009 holds the output (the run is recorded, the output has expectations, or
it is written under two names), the rows are computed before the delete, and the
overwrite succeeds with the right rows (the written files hash the same as the held
rows, `e06_materialise.py`, `overwrite_input`). If an executor holding a block dies
during that write, the write fails with `CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND`, after the
delete: the same loss as the plain run, in a narrower window.

## Repro
`python tasks/hardening/experiments/e06/e06_materialise.py WORK_DIR 1000000`, the line
with `"mechanism": "none", "question": "overwrite_input"` (plain Spark), next to the
`localCheckpoint` line (held).

## Expected
A plain run is refused before anything is deleted (`ubunye validate` can see that an
output overwrites a path or table an input reads), or the output is held first so the
source is read in full before it is deleted. Not fixed here: F-040 is about the
record, and this changes what a plain run does.
