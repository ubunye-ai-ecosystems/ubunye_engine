# task-17 — CI installs torch (GPU wheels, ~1 GB) for sklearn-only tests in titanic_ml_databricks

**Example:** `titanic_ml_databricks`
**Step:** "Install test dependencies" (`pip install -e ".[ml,dev]"`)

## Symptom

The CI install step takes 93 seconds (13:38:30 → 13:40:03) because `[ml]` declares `torch` as a dependency, which pulls in the full PyTorch GPU wheel stack:

```
torch-2.11.0
triton-3.6.0
nvidia-cublas-13.1.0.3
nvidia-cudnn-cu13-9.19.0.56
nvidia-cufft-12.0.0.61
nvidia-cufile-1.15.1.6
nvidia-curand-10.4.0.35
nvidia-cusolver-12.0.4.66
nvidia-cusparse-12.6.3.3
nvidia-cusparselt-cu13-0.8.0
nvidia-nccl-cu13-2.28.9
nvidia-nvjitlink-13.0.88
nvidia-nvshmem-cu13-3.4.5
... (12+ CUDA packages)
```

None of the five unit tests in `examples/production/titanic_ml_databricks/tests/test_model.py` import `torch`. The `TitanicSurvivalModel` uses only `scikit-learn` and `joblib`.

## Repro

```
gh workflow run titanic_ml_databricks.yml --ref main -f job_to_run=titanic_train
# Run ID 24513421238, job 71650541307
```

Observed at:
```
deploy  Install test dependencies  2026-04-16T13:38:30 … Successfully installed torch-2.11.0 …
```

## Context

- `pyproject.toml` `[ml]` extra: `["scikit-learn", "torch", "mlflow"]`
- Workflow step: `pip install -e ".[ml,dev]"`
- Test file: `examples/production/titanic_ml_databricks/tests/test_model.py` (5 tests, all pandas/sklearn)

## Suspected root cause

`torch` was added to the `[ml]` extra for SparkML/PyTorch users but is unconditionally pulled in by this example's CI, which only exercises the sklearn-backed `TitanicSurvivalModel`.
