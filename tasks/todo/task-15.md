# task-15: Notebook cell output not surfaced in CI — blind to runtime assertion failures

## Symptom
The `databricks bundle run (manual trigger only)` step in CI logs only two lines of
job-level status:
```
2026-04-16 13:38:52 "[dev thabangline] titanic_survival_by_class" RUNNING
2026-04-16 13:40:24 "[dev thabangline] titanic_survival_by_class" TERMINATED SUCCESS
```
No notebook cell output (row counts, `df.show()` results, or assert statements from
the notebook) is captured in the GitHub Actions log.

## Repro
```
gh workflow run databricks_deploy.yml --ref main -f run_after_deploy=true
gh run view 24513408184 --log | grep "bundle run"
```

## Context
- Example: `titanic_databricks`
- Workflow: `.github/workflows/databricks_deploy.yml`
- Step: `databricks bundle run (manual trigger only)`
- Run 24513408184, 2026-04-16

The notebook (`examples/production/titanic_databricks/notebooks/run_titanic.py`) ends
with:
```python
for name, df in outputs.items():
    df.show(truncate=False)
    print(f"Row count: {df.count()}")
```
These prints and any failed asserts inside `ubunye.run_task()` are only visible in the
Databricks workspace job-run UI, not in the Actions log. A `TERMINATED SUCCESS` at the
job level does not guarantee the notebook's data assertions passed.

## Suspected root cause
`databricks bundle run` does not stream notebook cell output back to stdout by default;
the CI workflow has no post-run step to fetch the run output (e.g. via
`databricks jobs runs get-output --run-id ...`) and fail the workflow if output
contains assertion errors or unexpected row counts.
