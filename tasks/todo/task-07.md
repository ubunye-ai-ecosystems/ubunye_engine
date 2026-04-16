# task-07 — Exercise failure, retry, and rollback paths

**Priority:** medium — happy paths are green; failure paths are unknown.

## Scenarios to run against titanic_ml

### 7a — Promotion gate failure

Temporarily raise `TITANIC_MIN_AUC` above what the model actually
achieves (e.g. 0.999). Expectation:

- Training job succeeds — model is registered at `development` stage.
- `registry.promote` raises, the `promotion_error` column in the
  `training_metrics` audit row captures the first line.
- Predict job falls back to the prior staging version (first-run case:
  predict should refuse with a clear error).

### 7b — Rollback

Manually train twice so two staging/production versions exist. Run
`ubunye models rollback -u titanic -m TitanicSurvivalModel -v 1.0.0`.
Verify predict picks up the rolled-back version on the next run.

### 7c — Transient failure mid-run

Kill the Databricks job partway through training. Confirm:

- No stale entry left in the registry (or one marked clearly incomplete).
- Re-running from scratch does not collide on version numbers.

## What to watch for

- Silent partial writes to the UC Delta audit table.
- Registry metadata drifting out of sync with the filesystem.
- Monitor hooks swallowing exceptions.
