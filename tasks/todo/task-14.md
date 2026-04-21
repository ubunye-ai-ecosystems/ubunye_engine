# task-14: `typer[all]` extra not provided by typer 0.24.1

## Symptom
```
WARNING: typer 0.24.1 does not provide the extra 'all'
```
Emitted during `pip install -e ".[spark,dev]" pyarrow` in the `Install test dependencies`
step of the `titanic_databricks` CI workflow.

## Repro
```
gh workflow run databricks_deploy.yml --ref main -f run_after_deploy=true
gh run view 24513408184 --log | grep "WARNING"
```

## Context
- Example: `titanic_databricks`
- Workflow: `.github/workflows/databricks_deploy.yml`
- Step: `Install test dependencies`
- Run 24513408184, 2026-04-16
- The warning surfaced because `pyproject.toml` (or a dependency) declares
  `typer[all]` as a dependency, but typer dropped the `[all]` extras marker in
  version 0.24.x.

## Suspected root cause
The engine's `pyproject.toml` (or a transitive dependency spec) references
`typer[all]`, but typer >= 0.x removed the `[all]` optional extras group; the
dependency should be updated to plain `typer` (or the specific sub-extras that
were previously bundled under `[all]`, such as `typer[rich]`).
