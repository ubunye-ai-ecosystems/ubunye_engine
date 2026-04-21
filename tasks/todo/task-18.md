# task-18 — AUC promotion gate failure is silently swallowed; CI is green even when model quality gate fails

**Example:** `titanic_ml_databricks`
**Step:** "databricks bundle run (manual trigger only)" → `train_titanic.py` notebook → `TrainTitanicClassifier.transform()`

## Symptom

The promotion gate failure is caught with `except ValueError` and stored as a plain string in `promotion_error`. The task returns normally, the training_metrics row is written to UC, and `bundle run` exits 0. There is no way to distinguish a successful promotion from a failed one from the CI log alone — both show `TERMINATED SUCCESS`.

Relevant code in `examples/production/titanic_ml_databricks/pipelines/titanic/ml/train_classifier/transformations.py`:

```python
promoted_to: str = "none"
promotion_error: str = ""
try:
    registry.promote(
        use_case=USE_CASE,
        model_name=MODEL_NAME,
        version=version.version,
        to_stage=ModelStage.STAGING,
        gates={"min_auc": MIN_AUC},
    )
    promoted_to = "staging"
except ValueError as exc:
    promotion_error = str(exc).splitlines()[0]

return {"training_metrics": _metrics_row(..., promoted_to, promotion_error)}
```

## Repro

```
gh workflow run titanic_ml_databricks.yml --ref main -f job_to_run=titanic_train
# Run ID 24513421238, job 71650541307
# Result: TERMINATED SUCCESS regardless of whether AUC gate passed
```

The CI log only shows:
```
2026-04-16 13:40:17 "[dev thabangline] titanic_ml_train" RUNNING
2026-04-16 13:44:56 "[dev thabangline] titanic_ml_train" TERMINATED SUCCESS
```

## Context

- File: `examples/production/titanic_ml_databricks/pipelines/titanic/ml/train_classifier/transformations.py`, lines 66-78
- The `promoted_to` and `promotion_error` values are only visible by querying the `workspace.titanic_ml.training_metrics` UC table — not surfaced in the CI log or the bundle run exit code.
- This means a regression that drops model AUC below `min_auc` (0.80) would go undetected by CI.

## Suspected root cause

The design intent is to record the promotion outcome as an audit log row and not fail the job (allowing training to always write metrics). However this means the AUC gate provides no enforcement signal at the CI level — it is documentation-only in practice.
