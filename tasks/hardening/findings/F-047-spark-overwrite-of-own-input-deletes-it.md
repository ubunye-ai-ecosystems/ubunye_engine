# F-047: On Spark, an output that overwrites its own input deletes the input, then fails

**Status:** fixed (refused before anything is read, on every lazy backend)
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

## Fix
Refused before the run starts. `check_task` (the pre-run check behind `ubunye run`,
`ubunye validate --backend` and `ubunye plan`) reports any output whose mode deletes
(`overwrite`, `overwrite_partitions`) and whose path is a folder an input of the same
task reads: the same folder, a parent, or a folder inside it, with trailing slashes,
`file:` and globs normalised. Only a lazy backend is checked (pandas reads into memory
first, so its overwrite is safe), and Delta outputs pass, since a Delta overwrite
reads a fixed snapshot. A path that is not rendered yet (`{{ ... }}`) is not judged.

Holding the output first (what ADR 009 does for recorded runs) was rejected as the
fix: the rows are computed before the delete, but an executor lost during the write
fails with `CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND` after the delete, the same loss in a
narrower window. A refusal loses nothing and says what to do instead.

Pinned by `tests/integration/test_self_overwrite_spark.py` (live Spark, both backends:
refused, and the 1,000 input rows still read back; with the rule off the same test
fails with `FAILED_READ_FILE.FILE_NOT_EXIST`) and `TestSelfOverwrite` in
`tests/unit/core/test_capabilities.py`. No config in the repo is flagged (21 checked:
12 rendered, 9 raw templates).

