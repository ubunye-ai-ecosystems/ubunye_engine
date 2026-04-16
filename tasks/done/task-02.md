# task-02 — Fix task_runner sys.path timing bug

**Status:** done (2026-04-16) — not yet released

## What

`ubunye/core/task_runner.py` was calling `_load_task_class(task_dir)`
*before* entering the `_with_task_dir_on_path` context. Consequence: any
top-level `from model import X` (or similar adjacent-module import) in
`transformations.py` raised `ModuleNotFoundError` at import time.

Surfaced first on the Titanic ML example's training job on Databricks.

## Fix

Moved the whole load → register → run sequence inside
`_with_task_dir_on_path(task_dir)`. Commit `6362942`.

## Compat band-aid

Both titanic_ml `transformations.py` files (`train_classifier/` and
`predict_classifier/`) carry a localised sys.path shim so the example
works against 0.1.6 on PyPI *today*. The shim is the three-line block
guarded by a comment pointing at this fix — remove it after 0.1.7 ships
(see `todo/task-01.md`).
