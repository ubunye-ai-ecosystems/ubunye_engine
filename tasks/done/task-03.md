# task-03 — Validate titanic_ml lifecycle end-to-end on Databricks

**Status:** done (2026-04-16)

## What

Fired both jobs against Databricks Free Edition via `workflow_dispatch`.

- `titanic_train` → run **24484787472**, 6m59s, ✅ all steps green.
  Notebook provisioned UC schema + `data` and `model_store` volumes,
  downloaded the Titanic CSV, fit the model, registered v1, promoted
  to staging (AUC ≥ 0.80 gate passed), appended one `training_metrics`
  row.
- `titanic_predict` → run **24485020522**, 6m49s, ✅ all steps green.
  Resolved staging version (no production yet), scored the DataFrame,
  wrote UC Delta predictions tagged with `model_version` + `model_stage`.

## Signal

The engine + Model Registry + MLflow + UC volume stack works end-to-end
on serverless. Known gaps live in `todo/` — streaming, JDBC, multi-task
DAGs, failure paths, rollback, lineage on Databricks.
