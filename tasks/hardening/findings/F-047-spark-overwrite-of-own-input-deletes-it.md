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


## Skeptic review (2026-09-29) and what changed
Probes of the pure functions (no Spark, the box was short on memory) found:

1. **The notebook path skipped the check.** `ubunye.notebook(...)` writes through
   `Engine.write_outputs`, which never ran the pre-checks. Fixed: `write_outputs` runs
   `_check_backend_can_run` first.
2. **Cloud spellings compared as raw text:** `s3`, `s3n` and `s3a`; `//` and `..`;
   host case; %-escapes. Fixed: s3 and s3n are s3a, the host is lower case, the path is
   unescaped and normalised (repeated slashes collapsed first, since `normpath` keeps a
   leading `//`).
3. **`dbfs:` was read as a local relative path.** Fixed: `dbfs:/x`, `dbfs:///x` and
   `/dbfs/x` are one place.
4. **A drive-root glob became the current folder** (`abspath("C:")`). Fixed, and a glob
   in a bucket or host name is not judged.
5. **The mode was not read as the writer reads it** (an enum, spaces). Fixed.
6. **False positive: `/data/events_*` next to `/data/summary` was refused.** A glob now
   keeps its literal start as a prefix, so `in*` reaches `in2` but not `out`.
7. **False positive: a Unity output with a fallback `path`** (the writer ignores it).
   Fixed: only writers that write to `path` (`REQUIRES` has `path_io`, or undeclared)
   are checked.
8. **The hint told a Spark user to use `--backend spark`.** A self-overwrite now gets
   its own hint.

Every case is a unit test in `TestSelfOverwrite`. Kept on purpose: a glob from a
folder (`/data/*/x.parquet`, `/*`) refuses an overwrite of anything under that folder,
since the glob can read it.

Still not seen (documented in `self_overwrites`): a relative path a cluster resolves
against another folder (HDFS `/user/<name>`), two `secret://` names for one path, and a
table input whose storage is the output's folder. Symlinks are followed for local paths.
