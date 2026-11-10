# F-110: the generated GitHub Actions workflow cannot deploy, and its run step never runs the job

**Status:** fixed (0.9.1)
**Severity:** major (the workflow `ubunye init github-actions` writes fails as generated)
**Source:** review (0.9.1 CLI pass)
**Promise:** `ubunye init github-actions` writes a workflow that deploys to Databricks

## What happens
- The workflow sets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` and runs
  `ubunye deploy databricks ... --target nonprod`. The deploy ignored
  `DATABRICKS_HOST`: with no committed `targets.yaml` that has a `nonprod` target it
  failed with "No targets.yaml found and no --host flag provided."
- The "Run job (manual trigger)" step ran the same `deploy databricks` command
  again. It redeployed and never started the job.

## Repro
```bash
ubunye init github-actions -d pipelines -u fraud -p ingestion -t claim_etl -o wf.yml
DATABRICKS_HOST=https://adb-1.azuredatabricks.net DATABRICKS_TOKEN=x \
  ubunye deploy databricks -d pipelines -u fraud -p ingestion -t claim_etl --target nonprod
```

## Fix
- `deploy databricks --host` reads `DATABRICKS_HOST` (used only when there is no
  `targets.yaml`, as before).
- New `deploy databricks --run`: start a run of the job after deploying it
  (`jobs.run_now`) and print the run's link. The workflow's run step uses it.
- The workflow's "Notebook output link" step is removed; the run step prints the link.

## Evidence
`tests/unit/cli/test_init.py::test_workflow_deploy_and_run_steps_work_with_the_env_it_sets`
renders the workflow, runs each `ubunye deploy` step through the real CLI parser with
the workflow's env (Databricks client faked), and checks the host and that only the
run step starts the job. It failed before ("No targets.yaml found and no --host flag
provided") and passes after.
