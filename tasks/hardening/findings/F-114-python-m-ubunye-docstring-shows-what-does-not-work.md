# F-114: `python -m ubunye`'s docstring shows commands that do not work

**Status:** fixed (0.9.1), docs only
**Severity:** minor
**Source:** review (0.9.1 CLI pass)
**Promise:** `python -m ubunye` runs a task under spark-submit

## What happens
`ubunye/__main__.py` showed `spark-submit --py-files deps.zip -m ubunye --task-dir
s3a://bucket/...`. spark-submit takes a file, not `-m`, and `--task-dir` must be a
local folder: `run_task` reads `config.yaml` from disk. `docs/deployment/anywhere.md`
said `--task-dir /path/or/bucket/...`.

## Fix
Docs only: the docstring and the page show `spark-submit .../ubunye/__main__.py
--task-dir /local/path/...` and `python -m ubunye`, say a bucket URI does not work,
and point to `ubunye deploy glue|dataproc|emr-serverless` (their entry script fetches
the task). Accepting a remote task dir is left out on purpose for now.

## Evidence
`tests/unit/test_module_entrypoint.py::test_its_docs_show_only_commands_that_work`
failed before (`spark-submit ... deps.zip -m ubunye`) and passes after.
