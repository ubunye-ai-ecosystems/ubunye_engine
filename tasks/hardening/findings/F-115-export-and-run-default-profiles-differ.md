# F-115: export and run default to different profiles, and not every help says so

**Status:** fixed (0.9.1), help and docs only
**Severity:** minor (a surprise, not a failure)
**Source:** review (0.9.1 CLI pass)
**Promise:** `ubunye <cmd> --help` is authoritative

## What happens
`ubunye run` defaults to `-m DEV`. `ubunye export airflow` and `export databricks`
default to `--profile prod`, `export spark-pipeline` to `-m PROD`. The airflow and
databricks help said so; `export spark-pipeline --help` showed only `[default: PROD]`
and `run --help` only `[default: DEV]`.

## Fix
No default changes (nothing that works may break). `export spark-pipeline -m` and
`run -m` help now say their default and the other's; `docs/cli.md` says once, under
`ubunye export`, that export defaults to production while run defaults to `DEV`.

## Evidence
`tests/unit/cli/test_export.py::test_each_help_says_export_defaults_to_production_and_run_to_dev`
failed before (`export spark-pipeline` help) and passes after.
