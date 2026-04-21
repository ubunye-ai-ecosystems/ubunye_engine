# task-13: `actions/setup-java@v4` uses Node.js 20 — will break June 2026

## Symptom
```
##[warning]Node.js 20 actions are deprecated. The following actions are running on Node.js 20
and may not work as expected: actions/setup-java@v4. Actions will be forced to run with
Node.js 24 by default starting June 2nd, 2026.
```

## Repro
```
gh workflow run databricks_deploy.yml --ref main -f run_after_deploy=true
gh run view 24513408184 --log | grep "Node.js 20"
```

## Context
- Example: `titanic_databricks`
- Workflow: `.github/workflows/databricks_deploy.yml`
- Step: `Set up Java 17 (required by PySpark)` — uses `actions/setup-java@v4`
  (SHA: c1e323688fd81a25caa38c78aa6df2d33d3e20d9)
- Observed on run 24513408184 (2026-04-16), job 71650505594

## Suspected root cause
`actions/setup-java@v4` at its current pinned SHA bundles a Node.js 20 runtime; a newer
release of the action (or pinning to a SHA that ships Node.js 24) is required before the
GitHub Actions runner removes Node.js 20 on 2026-09-16.
