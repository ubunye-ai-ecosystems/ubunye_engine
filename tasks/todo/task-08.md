# task-08 — Drive `ubunye lineage` on Databricks

**Priority:** medium — feature exists, never been validated on a
workspace runtime.

## Why

Lineage is recorded under `.ubunye/lineage` by default. On Databricks
that path is ephemeral (driver local FS) — records disappear when the
cluster shrinks to zero. The feature is likely unusable today on
serverless without an explicit `--lineage-dir` pointing at a UC volume.

## Steps

1. Add `--lineage` + `--lineage-dir=/Volumes/.../lineage` to the titanic_ml
   notebooks (or pass via `ubunye.run_task(lineage=True, lineage_dir=...)`).
2. Re-run training and predict twice each.
3. From a notebook, run `ubunye lineage list`, `show`, `compare`, `trace`
   against the UC volume store.
4. If the CLI can't read from `/Volumes/...` (it probably can't unless
   the store abstraction honours it), file that as a fix.

## Likely finding

The `FileSystemLineageStore` may assume local paths and break on
`/Volumes/...`. If so, either teach it about the Databricks FS mount or
document the limitation and ship a `DatabricksLineageStore` variant.

## Offline audit (2026-04-16, overnight branch)

Read `ubunye/lineage/storage.py` top-to-bottom. Findings:

- All FS access is `pathlib.Path`-based. No OS-specific path munging,
  no calls to `os.*` beyond what `pathlib` abstracts.
- `mkdir(parents=True, exist_ok=True)`, `write_text`, `glob`, `rglob`,
  `stat().st_mtime` — every operation is supported by UC volumes'
  POSIX FUSE mount on Databricks.
- Existing unit tests in `tests/unit/lineage/test_storage.py` already
  exercise absolute `base_dir` via pytest's `tmp_path` — the code path
  a `/Volumes/...` user would hit.
- `search(..., since=...)` uses lexicographic string comparison. Works
  for ISO-8601 when both sides use a consistent format; silently
  mis-compares if some records are `Z` and some `+00:00`. Not UC-specific,
  but worth tightening later.

**Conclusion:** no offline bug. The remaining validation is a live
Databricks run with `--lineage-dir=/Volumes/...` — covered by this
task's original scope.
