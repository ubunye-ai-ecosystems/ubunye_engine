# task-01 — Titanic ML end-to-end example on Databricks

**Status:** done (2026-04-16)

## What

Built `examples/production/titanic_ml_databricks/` — the ML lifecycle
reference. Two serverless Databricks jobs share one Asset Bundle:

- `titanic_train` — fits `TitanicSurvivalModel` (sklearn RandomForest),
  logs params/metrics/artifact to MLflow, registers a new version in the
  Ubunye Model Registry (filesystem-backed on a UC volume), auto-promotes
  to `staging` iff AUC ≥ `TITANIC_MIN_AUC` (default 0.80), and appends a
  one-row `training_metrics` audit row to a UC Delta table.
- `titanic_predict` — loads the current production (or staging fallback)
  model, scores the raw Titanic DataFrame, writes predictions to a UC
  Delta table tagged with `model_version` + `model_stage`.

## Design choices worth remembering

- Two task dirs with byte-identical `model.py` copies — CI diffs them
  (same pattern as the existing `transformations.py` portability check).
  Single-task-with-conditional-outputs was rejected because Jinja doesn't
  render over YAML structure, only string values.
- `UbunyeModel` (not the plugin `BaseModel`) is the user-facing contract
  because `transformations.py` dispatches to the model class explicitly.
- UC volumes used for both CSV data and registry store — serverless
  Spark blocks `file:///tmp/...`.

## Verified working

- Training run 24484787472 — model registered + promoted to staging.
- Predict run 24485020522 — staging model loaded + predictions written.
