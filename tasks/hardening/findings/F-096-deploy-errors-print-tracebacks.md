# F-096: deploy errors print a full Rich traceback instead of a clean message

**Status:** fixed (0.9.1)
**Severity:** minor (the cause is in the traceback, but buried)
**Source:** review (full CLI test run before 0.9.0)
**Promise:** none

## What happens
Expected failures of `ubunye deploy` end in a full Rich traceback:
- `deploy databricks` with no `targets.yaml`, or no token in the token env var;
- `deploy glue` with no AWS credentials or CLI;
- `deploy k8s` and `deploy container-apps`, which also dump the whole launcher
  script into the traceback.

## Repro
```bash
ubunye deploy databricks -d pipelines -u uc -p pl -t task      # no targets.yaml
ubunye deploy glue -d pipelines -u uc -p pl -t task --bucket b  # no AWS login
```

## Expected
`[ERROR]` with what failed, where, and what to try (the `DeployError` message and
hint), exit 1, no traceback. A traceback only with a debug flag.

## Why not fixed here
Out of scope for the 0.9.0 release blockers; it touches every deploy target's error
path. Planned for 0.9.1.

## Evidence
The 0.9.0 CLI test run.
