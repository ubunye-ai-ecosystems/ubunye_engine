# Ubunye Engine — Backlog

## Plugins (no engine changes — register as entry points)

- [ ] Feature store connector — register as `ubunye.readers.feature_store` and `ubunye.writers.feature_store`. Same plugin system as Hive, Delta, etc. Reads/writes to Databricks Feature Store or any feature store backend.
- ~~Drift detection~~ — not needed as a plugin. Users compute drift in their `transformations.py` (they know their data), and MLflowMonitor already logs metrics. Drift is just a task.
- [ ] Model sync transform — register as `ubunye.transforms.model_sync`. Copies models between environments (nonprod → prod registry). Transform plugin.

## New CLI commands

- [ ] `ubunye deploy` command — new CLI command in `cli/`. Reads pipeline YAML, generates Databricks Asset Bundle definition, and deploys via Databricks CLI. Eliminates manual job creation in Databricks UI.

## Repo hygiene

- [x] Pre-commit hooks — `.pre-commit-config.yaml` with black, ruff, trailing-whitespace, yaml checks.
- [ ] Add mypy and yaml-lint to pre-commit hooks.
- [x] Migrate `setup.py` → `pyproject.toml` — done.
- [x] Fix CI unit tests — 294 tests passing.

## Documentation

- [x] Add deployment guide to docs (`docs/deployment.md`).
- [x] Add end-to-end example using Python API (`examples/python_api/`).
- [x] Support `USE CATALOG` / `USE SCHEMA` in config.yaml ENGINE section natively.
