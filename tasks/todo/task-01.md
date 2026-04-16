# task-01 — Ship v0.1.7 and drop the titanic_ml sys.path shim

**Blocks:** `todo/task-00.md` wrap-up.
**Blocked by:** all bugs from the fire-test being fixed.

## Steps

1. Bump version in `pyproject.toml` → `0.1.7`.
2. Move `docs/changelog.md` `[Unreleased]` entries under a new `[0.1.7]`
   section, dated.
3. Tag `v0.1.7`, let `publish_pypip.yml` publish via OIDC.
4. Once the release is on PyPI, remove the localised sys.path shim from
   both titanic_ml `transformations.py` files:
   - `examples/production/titanic_ml_databricks/pipelines/titanic/ml/train_classifier/transformations.py`
   - `examples/production/titanic_ml_databricks/pipelines/titanic/ml/predict_classifier/transformations.py`
   Delete lines 17–20 (the `_TASK_DIR` block and the comment above it).
5. Repin the notebooks in `notebooks/train_titanic.py` and
   `notebooks/predict_titanic.py` from `ubunye-engine[...]==0.1.6` to
   `==0.1.7`.
6. Re-run both jobs to confirm the shim wasn't load-bearing.
