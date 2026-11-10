# F-095: `export databricks` names a 0.1.0 wheel and an old runtime by default

**Status:** fixed on fix/0.9-cli-findings (2026-11-10)
**Severity:** major (the generated job installs a wheel that does not exist)
**Source:** review (full CLI test run before 0.9.0)
**Promise:** 6 (the config owns the task, never the platform)

## What happens
With no `wheel_dbfs_path` and no `spark_version` under `ORCHESTRATION.databricks`,
`ubunye export databricks` writes `"whl": "dbfs:/libs/ubunye_engine-0.1.0-py3-none-any.whl"`
and `"spark_version": "13.3.x-scala2.12"`. 13.3 LTS is Spark 3.4, past end of support
and older than the `pyspark>=3.5` the engine declares. The `--profile` default of
`prod` (while `run` defaults to `DEV`) was not explained in the help.

## Repro
```bash
ubunye export databricks -c pipelines/uc/pl/task/config.yaml -o job.json
```

## Expected
The wheel of the installed engine, a current LTS runtime, and help that states the
`prod` default. Both stay overridable.

## Fix
`ubunye/orchestration/databricks_exporter.py`: `default_wheel_path()` uses
`ubunye.__version__`; `DEFAULT_SPARK_VERSION = "17.3.x-scala2.13"` (Databricks
Runtime 17.3 LTS). `--profile` help on `export airflow` and `export databricks` says
the default is `prod`, unlike `run`'s `DEV`. Behaviour of `--profile` is unchanged.

Not changed: `deploy databricks` targets still default `spark_version` to
`13.3.x-scala2.12` for job clusters (`ubunye/deploy/databricks/targets.py`; serverless
is the default there, so the field is unused unless `serverless: false`). Changing a
deploy default is left for a decision.

## Evidence
`tests/unit/cli/test_export.py::test_export_databricks_defaults_follow_the_installed_engine`
and `::test_export_help_says_its_profile_default_differs_from_run` failed before and pass
after; `::test_export_databricks_wheel_can_still_be_given` passes.
