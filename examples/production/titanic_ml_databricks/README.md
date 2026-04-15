# Titanic — ML End-to-End on Databricks (train + predict, serverless + UC)

A production-shaped ML reference pipeline that runs on **Databricks
serverless compute** and exercises every ML primitive that ships with
Ubunye Engine:

- **`UbunyeModel`** — the library-independent model contract (train,
  predict, save, load, metadata).
- **MLflow** — params, metrics, and artifacts logged to the workspace
  tracking server (auto-detected on Databricks).
- **Model Registry** — filesystem-backed versioning on a Unity Catalog
  volume, with semver auto-bump on each training run.
- **Promotion Gate** — blocks promotion to `staging` when validation AUC
  falls below a configurable threshold.
- **Unity Catalog outputs** — a training audit log and a predictions
  table, both managed Delta.

Two jobs are deployed from a single Asset Bundle:

| Job               | Purpose                                              |
|-------------------|------------------------------------------------------|
| `titanic_train`   | Fit model → MLflow log → register → auto-promote.    |
| `titanic_predict` | Load current production/staging model → score → write. |

Validated end-to-end on Databricks Free Edition (serverless + `workspace`
catalog). On paid workspaces, override `titanic_catalog=main`.

---

## What gets deployed

```
titanic_ml_train (Job)
└── train (Notebook task, serverless)
    ├── notebook: notebooks/train_titanic.py
    └── base_parameters:
        ├── task_dir:        <workspace>/pipelines/titanic/ml/train_classifier
        ├── dt:              2026-04-15
        ├── mode:            PROD
        ├── titanic_catalog: workspace
        ├── titanic_schema:  titanic_ml
        └── min_auc:         0.80

titanic_ml_predict (Job)
└── predict (Notebook task, serverless)
    ├── notebook: notebooks/predict_titanic.py
    └── base_parameters:
        ├── task_dir:        <workspace>/pipelines/titanic/ml/predict_classifier
        ├── dt:              2026-04-15
        ├── mode:            PROD
        ├── titanic_catalog: workspace
        └── titanic_schema:  titanic_ml
```

The **train** notebook:

1. Provisions `workspace.titanic_ml` schema and two UC volumes (`data` +
   `model_store`).
2. Downloads the Titanic CSV into `/Volumes/workspace/titanic_ml/data/titanic.csv`.
3. Sets `TITANIC_MODEL_STORE=/Volumes/.../model_store`,
   `MLFLOW_EXPERIMENT_NAME=/Shared/titanic_ml`, `TITANIC_MIN_AUC`.
4. Calls `ubunye.run_task()` → `train_classifier/transformations.py`
   which fits the model, logs to MLflow, registers the version, and
   attempts promotion to `staging`.
5. Writes a one-row audit DataFrame to
   `workspace.titanic_ml.training_metrics` (append).

The **predict** notebook:

1. Asserts the input CSV is present (train must run first).
2. Calls `ubunye.run_task()` → `predict_classifier/transformations.py`
   which loads the current production version — or falls back to
   staging — from the registry, scores the raw DataFrame, and tags each
   row with the model version + stage.
3. Overwrites `workspace.titanic_ml.predictions`.

---

## Directory layout

```
titanic_ml_databricks/
├── pipelines/titanic/ml/
│   ├── train_classifier/
│   │   ├── config.yaml            # CSV reader -> unity training_metrics writer
│   │   ├── transformations.py     # train + MLflow + register + promote
│   │   └── model.py               # TitanicSurvivalModel (sklearn)
│   └── predict_classifier/
│       ├── config.yaml            # CSV reader -> unity predictions writer
│       ├── transformations.py     # load from registry + predict
│       └── model.py               # byte-identical copy; CI enforces diff
├── notebooks/
│   ├── train_titanic.py           # serverless notebook entry — train
│   └── predict_titanic.py         # serverless notebook entry — predict
├── tests/
│   ├── conftest.py
│   └── test_model.py              # pandas-only model contract tests
├── databricks.yml                 # Asset Bundle with both jobs
└── README.md
```

---

## Prerequisites

1. **Databricks workspace with Unity Catalog.** Free Edition ships with
   the `workspace` catalog out of the box.
2. **Personal access token** (Free Edition has no service principals).
3. **Databricks CLI `>= v0.205`** (the Go rewrite):
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   ```
4. **GitHub repository secrets** for CI:
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`

---

## Deploy and run manually

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<pat>

cd examples/production/titanic_ml_databricks

databricks bundle validate --target nonprod
databricks bundle deploy   --target nonprod

# First run: train. Registers v1.0.0 and promotes to staging if AUC >= 0.80.
databricks bundle run titanic_train   --target nonprod

# Then: predict using the staging model.
databricks bundle run titanic_predict --target nonprod
```

To target `main` on a paid workspace:

```bash
databricks bundle deploy --target nonprod --var="titanic_catalog=main"
```

---

## Verify outputs

### Training audit log (append-only, one row per run)

```sql
SELECT model_version, trained_at, accuracy, auc, f1, promoted_to
FROM   workspace.titanic_ml.training_metrics
ORDER  BY trained_at DESC;
```

### Predictions (per-passenger scores, overwritten each run)

```sql
SELECT Pclass, Sex, Age, prediction, proba, model_version, model_stage
FROM   workspace.titanic_ml.predictions
ORDER  BY proba DESC
LIMIT  20;
```

### Model Registry

Use the CLI from a workstation that has the UC volume synced locally,
or open a Databricks notebook and:

```python
from ubunye.models.registry import ModelRegistry
reg = ModelRegistry("/Volumes/workspace/titanic_ml/model_store")
for v in reg.list_versions("titanic", "TitanicSurvivalModel"):
    print(v.version, v.stage.value, v.metrics.get("auc"))
```

### MLflow

Each training run appears under the `/Shared/titanic_ml` experiment in
the workspace's MLflow UI, with params, metrics, and the registered
artifact.

---

## CI — GitHub Actions

See `.github/workflows/titanic_ml_databricks.yml`. The workflow:

1. Runs the pandas/sklearn unit tests (no Spark).
2. Diffs `model.py` between `train_classifier/` and `predict_classifier/`
   — **fails loudly on drift**.
3. If `DATABRICKS_HOST` / `DATABRICKS_TOKEN` secrets are configured:
   - `databricks bundle validate --target nonprod`
   - `databricks bundle deploy --target nonprod`
4. On manual `workflow_dispatch` you can pick either `titanic_train` or
   `titanic_predict` from the dropdown to kick off a run after deploy.

If the secrets are absent (forks, unconfigured repos), steps 1-2 still
run; steps 3-4 soft-skip with a warning.

---

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| Predict job raises `No production or staging version` | Train has never run successfully. Trigger `titanic_train` first. |
| Train job raises `TITANIC_MODEL_STORE must be set` | The notebook failed to set the env var — usually because schema/volume creation failed earlier in the cell chain. Re-run. |
| Promotion logs `Promotion blocked — 1 gate(s) failed` | Validation AUC < `min_auc`. Lower the gate, improve the model, or run with more training data. |
| `LocalFilesystemAccessDeniedException: Cannot access non /Workspace local filesystem path` | You set `TITANIC_MODEL_STORE` to a `/tmp` path. Must be a UC volume on serverless. |
| `[DATA_SOURCE_NOT_FOUND] Failed to find the data source: unity` | Engine version < 0.1.6. Upgrade. |
| MLflow experiment empty | Your workspace has disabled the `/Shared` folder. Set `MLFLOW_EXPERIMENT_NAME` to a path under a folder you have write access to. |
